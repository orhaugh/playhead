#!/usr/bin/env bash
# Submit one pipeline/*.sql job to the cluster's leading Coordinator.
#
#   scripts/submit.sh qoe            # pipeline/qoe.sql, as job "qoe"
#
# The file is compiled by the clink CLI in the leading Coordinator's pod, in a
# catalog of its own, and the job it describes is submitted there, so each
# pipeline/*.sql stands alone. The job checkpoints every two seconds into the
# shared volume every Worker mounts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTX="${KUBE_CONTEXT:-kind-playhead}"
name="$1"
sql="${ROOT}/pipeline/${name}.sql"
[ -f "$sql" ] || { echo "submit: no ${sql}" >&2; exit 2; }
k() { kubectl --context "$CTX" -n clink "$@"; }

# The leader is whichever Coordinator holds the lock; its pod IP is in the
# shared HA directory.
leader_ip="$(k exec deploy/clink-coordinator -- cat /var/lib/clink/ha/active-leader.json \
  | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["host"])')"
leader="$(k get pods -l app.kubernetes.io/component=coordinator \
  -o jsonpath="{range .items[?(@.status.podIP==\"${leader_ip}\")]}{.metadata.name}{end}")"
[ -n "$leader" ] || { echo "submit: no Coordinator pod at the leader's address ${leader_ip}" >&2; exit 1; }

k cp "$sql" "${leader}:/tmp/${name}.sql" >/dev/null
k exec "$leader" -- clink run "/tmp/${name}.sql" --coordinator-host=127.0.0.1 \
  --coordinator-port=8081 --name="${name}" \
  --checkpoint-dir=/var/lib/clink/ha/checkpoints --checkpoint-interval-ms=2000
