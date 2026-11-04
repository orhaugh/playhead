#!/usr/bin/env bash
# Run one pipeline/*.sql job on the cluster as a ClinkJob, which the clink
# operator submits, checkpoints and upgrades.
#
#   scripts/job.sh qoe                    # pipeline/qoe.sql, as ClinkJob "qoe"
#   scripts/job.sh edges 's/a/b/'         # the same script through a sed edit:
#                                         # a new version, rolled out as an upgrade
#
# The operator submits the script over the Coordinator's HTTP API with the
# cluster's token, compiled in a catalog of its own, checkpointing every two
# seconds under the shared checkpoint volume. Applying a changed script is an
# upgrade: the operator ends the job with a savepoint that cancels it, and
# submits the new script restored from the savepoint.
#
# pipeline/<name>.policy.json, where there is one, joins the ClinkJob's spec:
# the edges job's says when it parks and what wakes it (PARK=0 leaves it out).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTX="${KUBE_CONTEXT:-kind-playhead}"
name="$1"
edit="${2:-}"
sql="${ROOT}/pipeline/${name}.sql"
[ -f "$sql" ] || { echo "job: no ${sql}" >&2; exit 2; }
script="$(cat "$sql")"
[ -n "$edit" ] && script="$(sed -e "$edit" <<<"$script")"
policy="${ROOT}/pipeline/${name}.policy.json"
if [ "${PARK:-1}" = 0 ] || [ ! -f "$policy" ]; then policy=""; fi
python3 -I -c '
import json, sys
name, policy, script = sys.argv[1], sys.argv[2], sys.stdin.read()
spec = {"clusterName": "clink", "checkpointIntervalMs": 2000, "sql": script}
if policy:
    spec.update(json.load(open(policy)))
print(json.dumps({
    "apiVersion": "clink.dev/v1alpha1",
    "kind": "ClinkJob",
    "metadata": {"name": name, "namespace": "clink"},
    "spec": spec,
}))' "$name" "$policy" <<<"$script" | kubectl --context "$CTX" apply -f - >/dev/null
