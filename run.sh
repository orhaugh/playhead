#!/usr/bin/env bash
# Run playhead end to end, from empty, a Worker kill included, and hold every
# output to what the tape says it must contain.
#
#   ./run.sh                 the whole run
#   SPEED=30 ./run.sh        the half hour at thirty times real time (default 15)
#   KILL=0 ./run.sh          without the Worker kill
#   CLEANUP=1 ./run.sh       and remove the cluster and the cloud at the end
#   CLINK_IMAGE=clink-runtime:dev-<sha> ./run.sh
#                            against a local clink build instead of the pinned release
#
# Steps: create the kind cluster if it is not there; recreate the cloud from
# empty and name its services inside the cluster; reinstall clink from the
# chart with an empty HA directory; provision the topics, subjects, bucket and
# namespace; generate the tape; submit the jobs; play the tape into Kafka,
# killing a Worker with SIGKILL at minute 14, inside the failing-title
# incident; wait for the outputs to settle; then tools/check.py checks the
# Iceberg table, the alerts topic and the CDN vendor's bucket.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
CTX=kind-playhead
SPEED="${SPEED:-15}"
KILL="${KILL:-1}"
IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:0.15.0}"
COMPOSE=(docker compose -f cloud/compose.yml --env-file state/cloud.env)
JOBS=(qoe alerts edges)
k() { kubectl --context "$CTX" -n clink "$@"; }
say() { printf '\n==> %s\n' "$*"; }

mkdir -p state
if [ ! -f state/cloud.env ]; then
  ( umask 077; printf 'PLAYHEAD_AZURE_KEY=%s\n' "$(openssl rand -base64 64 | tr -d '\n')" > state/cloud.env )
fi

if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv && .venv/bin/python -m pip install --quiet -r requirements.txt
fi

say "The platform's cluster"
kind get clusters 2>/dev/null | grep -qx playhead || kind create cluster --config k8s/kind.yaml
kubectl --context "$CTX" wait --for=condition=Ready node --all --timeout=180s >/dev/null

say "The cloud, from empty"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --wait >/dev/null
scripts/cloud-services.sh

say "clink, from the chart, with an empty HA directory"
helm --kube-context "$CTX" uninstall clink -n clink --wait >/dev/null 2>&1 || true
docker exec playhead-control-plane rm -rf /var/lib/clink-ha
rm -rf state/chart && mkdir -p state/chart
git -C "${CLINK_SRC:-../clink}" archive v0.15.0 deploy/helm/clink | tar -x -C state/chart
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
kind load docker-image "$IMAGE" --name playhead >/dev/null
helm --kube-context "$CTX" upgrade --install clink state/chart/deploy/helm/clink -n clink --create-namespace \
  -f k8s/values.yaml --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" \
  --wait --timeout 5m >/dev/null
for _ in $(seq 1 60); do
  n="$(k exec deploy/clink-coordinator -- sh -c 'curl -s http://$(cat /var/lib/clink/ha/active-leader.json | sed -E "s/.*\"host\":\"([^\"]+)\".*/\1/"):8081/api/v1/workers' 2>/dev/null \
       | python3 -I -c 'import json,sys; print(sum(1 for w in json.load(sys.stdin)["workers"] if not w["lost"]))' 2>/dev/null || echo 0)"
  [ "$n" = 2 ] && break
  sleep 2
done
echo "two Workers registered with the leading Coordinator"

say "Provisioning the cloud"
scripts/provision.sh

say "The tape"
python3 -I tools/gen.py

say "Submitting the jobs"
for j in "${JOBS[@]}"; do scripts/submit.sh "$j"; done

say "Playing the half hour at ${SPEED}x$( [ "$KILL" = 1 ] && echo ', killing a Worker at minute 14')"
hooks=()
[ "$KILL" = 1 ] && hooks+=(--at "14:kubectl --context $CTX -n clink delete pod clink-worker-1 --grace-period=0 --force --wait=false >/dev/null 2>&1")
python3 -I tools/feed.py --speed "$SPEED" "${hooks[@]}" | grep -vE '^feed: minute [0-9]+$'

say "Checking every output against the tape"
.venv/bin/python -I tools/check.py --wait 180

if [ "${CLEANUP:-0}" = 1 ]; then
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  kind delete cluster --name playhead >/dev/null 2>&1 || true
fi
