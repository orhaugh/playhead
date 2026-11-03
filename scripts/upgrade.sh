#!/usr/bin/env bash
# Roll out a new version of one job through its ClinkJob, and record what the
# operator did for the checks: the job it replaced, the savepoint it ended that
# job at, and the restore point the new job started from. Recorded at once,
# because a later failover restores the job again, from its own checkpoints.
#
#   scripts/upgrade.sh edges 's/a/b/'     # pipeline/edges.sql through a sed edit
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTX="${KUBE_CONTEXT:-kind-playhead}"
name="$1"
edit="$2"
k() { kubectl --context "$CTX" -n clink "$@"; }
field() { k get clinkjob "$name" -o jsonpath="{$1}" 2>/dev/null || true; }

before="$(field .status.jobID)"
"${ROOT}/scripts/job.sh" "$name" "$edit"
after=""
for _ in $(seq 1 90); do
  now="$(field .status.jobID)"
  if [ "$(field .status.phase)" = Running ] && [ -n "$now" ] && [ "$now" != "$before" ]; then
    after="$now"
    break
  fi
  sleep 1
done
[ -n "$after" ] || { echo "upgrade: ${name} did not come back as a new job: $(field .status.message)" >&2; exit 1; }
savepoint="$(field .status.lastSavepoint.dir)@$(field .status.lastSavepoint.checkpointId)"
restored="$(k exec clink-worker-0 -c worker -- sh -c \
  "curl -s -H \"Authorization: Bearer \$CLINK_AUTH_TOKEN\" http://clink-coordinator:8081/api/v1/jobs/${after}" |
  python3 -I -c 'import json,sys; d=json.load(sys.stdin); print("%s@%s" % (d["restore_from_dir"], d["restore_from_checkpoint_id"]))')"
python3 -I -c 'import json,sys; print(json.dumps(dict(zip(["job","from","to","savepoint","restored_from"], sys.argv[1:]))))' \
  "$name" "$before" "$after" "$savepoint" "$restored" > "${ROOT}/state/upgrade-${name}.json"
echo "upgraded ${name}: job ${before} -> ${after}, restored from ${restored}"
