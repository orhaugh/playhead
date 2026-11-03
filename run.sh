#!/usr/bin/env bash
# Run playhead end to end, from empty, with its faults, and hold every output
# to what the tape says it must contain.
#
#   ./run.sh                 the whole run, clink run by the clink operator
#   DEPLOY=chart ./run.sh    clink from the Helm chart instead, its jobs
#                            submitted with the clink CLI
#   SPEED=30 ./run.sh        the half hour at thirty times real time (default 15)
#   KILL=0 ./run.sh          without the Worker kill
#   FAILOVER=0 ./run.sh      without the Coordinator failover
#   UPGRADE=0 ./run.sh       without the edges job's upgrade (operator only)
#   CLEANUP=1 ./run.sh       and remove the cluster and the cloud at the end
#
# Steps: create the kind cluster if it is not there; recreate the cloud from
# empty and name its services inside the cluster; reinstall clink with an
# empty HA directory, through the operator (a ClinkCluster) or the chart;
# provision the topics, subjects, bucket and namespace; generate the tape;
# start the jobs (one ClinkJob each, or one `clink run` each); play the tape
# into Kafka; wait for the outputs to settle; then tools/check.py checks every
# output, the security of the cluster, and its traces, metrics and lineage, and
# tools/footprint.py reports the CPU and memory clink's pods and the cloud's
# containers used from the start of the tape to the end of the checks.
#
# Three faults land inside the tape: at minute 8 the operator upgrades the
# edges job to a new version of its script, through a savepoint; at minute 14
# a Worker is killed with SIGKILL, inside the failing-title incident; at
# minute 20 the leading Coordinator's pod is killed, and the standby takes
# over.
#
# The sample needs clink after v0.15.0: typed SQL tables on object-store
# Parquet, the chart's and the operator's auth, TLS and Secrets, and SQL
# ClinkJobs. Until the release that carries them, run it against a build of
# clink and its deploy/ tree at that commit:
#
#   CLINK_IMAGE=clink-runtime:dev-<sha> CLINK_REF=<sha> ./run.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
CTX=kind-playhead
SPEED="${SPEED:-15}"
DEPLOY="${DEPLOY:-operator}"
KILL="${KILL:-1}"
FAILOVER="${FAILOVER:-1}"
UPGRADE="${UPGRADE:-1}"
IMAGE="${CLINK_IMAGE:?set CLINK_IMAGE and CLINK_REF; see the header}"
REF="${CLINK_REF:?set CLINK_IMAGE and CLINK_REF; see the header}"
SRC="${CLINK_SRC:-../clink}"
case "$DEPLOY" in operator | chart) ;; *) echo "run: DEPLOY is operator or chart" >&2; exit 2 ;; esac
COMPOSE=(docker compose --progress quiet -f cloud/compose.yml --env-file state/cloud.env)
JOBS=(qoe alerts edges ledger)
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

say "clink, through the ${DEPLOY}, with an empty HA directory"
# Whichever ran clink last goes first: the chart's release, or the operator's
# jobs and cluster (the jobs' finalizers cancel them).
helm --kube-context "$CTX" uninstall clink -n clink --wait >/dev/null 2>&1 || true
if kubectl --context "$CTX" get crd clinkclusters.clink.dev >/dev/null 2>&1; then
  k delete clinkjob --all --timeout=90s >/dev/null 2>&1 || true
  k delete clinkcluster --all --timeout=120s >/dev/null 2>&1 || true
fi
k wait --for=delete pod -l app.kubernetes.io/instance=clink --timeout=120s >/dev/null 2>&1 || true
docker exec playhead-control-plane rm -rf /var/lib/clink-ha /var/lib/clink-checkpoints
scripts/secrets.sh
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
kind load docker-image "$IMAGE" --name playhead >/dev/null
rm -rf state/deploy && mkdir -p state/deploy
if [ "$DEPLOY" = operator ]; then
  # The operator at the same commit as the image, built here as the chart is
  # taken from there: its CRDs, its RBAC and its manager.
  git -C "$SRC" archive "$REF" deploy/operator | tar -x -C state/deploy
  op=state/deploy/deploy/operator
  docker build -q -t clink-operator:playhead "$op" >/dev/null
  kind load docker-image clink-operator:playhead --name playhead >/dev/null
  kubectl --context "$CTX" apply --server-side --force-conflicts -f "$op/config/crd" >/dev/null
  for f in manager/manager.yaml rbac; do kubectl --context "$CTX" apply -f "$op/config/$f" >/dev/null; done
  kubectl --context "$CTX" -n clink-operator-system set image deploy/clink-operator-controller \
    manager=clink-operator:playhead >/dev/null
  kubectl --context "$CTX" -n clink-operator-system rollout restart deploy/clink-operator-controller >/dev/null
  kubectl --context "$CTX" -n clink-operator-system rollout status deploy/clink-operator-controller --timeout=120s >/dev/null
  sed -e "s|repository: ghcr.io/orhaugh/clink-runtime|repository: ${IMAGE%:*}|" \
      -e "s|tag: \"0.15.0\"|tag: \"${IMAGE##*:}\"|" k8s/cluster.yaml | kubectl --context "$CTX" apply -f - >/dev/null
  for _ in $(seq 1 90); do
    [ "$(k get clinkcluster clink -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ] && break
    sleep 2
  done
  [ "$(k get clinkcluster clink -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ] \
    || { echo "run: the ClinkCluster never reached Running" >&2; exit 1; }
else
  # A chart deployment has no operator: remove one an earlier run installed,
  # now that its jobs' finalizers have run.
  kubectl --context "$CTX" delete namespace clink-operator-system --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
  git -C "$SRC" archive "$REF" deploy/helm/clink | tar -x -C state/deploy
  helm --kube-context "$CTX" upgrade --install clink state/deploy/deploy/helm/clink -n clink --create-namespace \
    -f k8s/values.yaml --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" \
    --wait --timeout 5m >/dev/null
fi
for _ in $(seq 1 60); do
  # Any Coordinator answers with the leader's view: a standby forwards the API.
  n="$(k exec clink-coordinator-0 -- sh -c 'curl -s -H "Authorization: Bearer $CLINK_AUTH_TOKEN" http://127.0.0.1:8081/api/v1/workers' 2>/dev/null \
       | python3 -I -c 'import json,sys; print(sum(1 for w in json.load(sys.stdin)["workers"] if not w["lost"]))' 2>/dev/null || echo 0)"
  [ "$n" = 2 ] && break
  sleep 2
done
echo "two Workers registered with the leading Coordinator"

say "Provisioning the cloud"
scripts/provision.sh

say "The tape"
python3 -I tools/gen.py

say "Starting the jobs"
if [ "$DEPLOY" = operator ]; then
  for j in "${JOBS[@]}"; do scripts/job.sh "$j"; done
  for j in "${JOBS[@]}"; do
    for _ in $(seq 1 60); do
      [ "$(k get clinkjob "$j" -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ] && break
      sleep 2
    done
    echo "$j: $(k get clinkjob "$j" -o jsonpath='{.status.phase}: {.status.message}')"
  done
else
  for j in "${JOBS[@]}"; do scripts/submit.sh "$j"; done
fi

faults=()
hooks=()
if [ "$DEPLOY" = operator ] && [ "$UPGRADE" = 1 ]; then
  # Version 2 of the edges job waits twice as long before it sets a quiet
  # partition aside. Applied as a ClinkJob change, so the operator rolls it out
  # through a savepoint; in the background, so the tape plays on meanwhile.
  faults+=("upgrading the edges job at minute 8")
  rm -f state/upgrade-edges.json
  hooks+=(--at "8:scripts/upgrade.sh edges \"s/idle_timeout_ms     = '10000'/idle_timeout_ms     = '20000'/\" &")
fi
if [ "$KILL" = 1 ]; then
  faults+=("killing a Worker at minute 14")
  hooks+=(--at "14:kubectl --context $CTX -n clink delete pod clink-worker-1 --grace-period=0 --force --wait=false >/dev/null 2>&1")
fi
if [ "$FAILOVER" = 1 ]; then
  faults+=("killing the leading Coordinator at minute 20")
  hooks+=(--at "20:scripts/kill-leader.sh")
fi
desc=""
# ${a[@]+"${a[@]}"}: an empty array is an unbound variable to bash 3.2 under set -u.
for f in ${faults[@]+"${faults[@]}"}; do desc+=", $f"; done
say "Playing the half hour at ${SPEED}x${desc}"
# What the run costs, sampled from here until the checks are done.
python3 -I tools/footprint.py --out state/footprint.json > state/footprint.txt &
footprint=$!
trap 'kill -TERM "$footprint" 2>/dev/null || true' EXIT
python3 -I tools/feed.py --speed "$SPEED" ${hooks[@]+"${hooks[@]}"} | grep -vE '^feed: minute [0-9]+$'

say "Checking every output against the tape"
checks=(--deploy "$DEPLOY")
[ "$DEPLOY" = operator ] && [ "$UPGRADE" = 1 ] && checks+=(--upgraded edges)
[ "$FAILOVER" = 1 ] && checks+=(--failover)
status=0
.venv/bin/python -I tools/check.py --wait 180 "${checks[@]}" || status=$?

say "What it cost"
kill -TERM "$footprint" && wait "$footprint" || true
trap - EXIT
cat state/footprint.txt

if [ "${CLEANUP:-0}" = 1 ]; then
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  kind delete cluster --name playhead >/dev/null 2>&1 || true
fi
exit "$status"
