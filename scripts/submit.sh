#!/usr/bin/env bash
# Submit one pipeline/*.sql job to the cluster.
#
#   scripts/submit.sh qoe            # pipeline/qoe.sql, as job "qoe"
#
# The file is compiled by the clink CLI in a Coordinator's pod, in a catalog of
# its own, so each pipeline/*.sql stands alone, and submitted over that
# Coordinator's HTTP API with the pod's token. Any Coordinator will do: a
# standby forwards the submission to the leader. The job checkpoints every two
# seconds into the shared volume every Worker mounts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTX="${KUBE_CONTEXT:-kind-playhead}"
name="$1"
sql="${ROOT}/pipeline/${name}.sql"
[ -f "$sql" ] || { echo "submit: no ${sql}" >&2; exit 2; }
k() { kubectl --context "$CTX" -n clink "$@"; }

pod=clink-coordinator-0
k cp "$sql" "${pod}:/tmp/${name}.sql" >/dev/null
k exec "$pod" -- clink run "/tmp/${name}.sql" --coordinator-host=127.0.0.1 \
  --coordinator-port=8081 --name="${name}" \
  --checkpoint-dir=/var/lib/clink/ha/checkpoints --checkpoint-interval-ms=2000
