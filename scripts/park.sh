#!/usr/bin/env bash
# The scale-to-zero scene, once the half hour is over and checked. The edges
# job declares that it parks after a minute without a record, and that a
# hundred records waiting for it on cdn.edge wake it (pipeline/edges.policy.json).
# This waits for the park, delivers the CDN's later batch of edge logs, waits
# for the backlog to wake the job and for the job to park again once the batch
# is done, and records what the operator and the Coordinator said at each step
# in state/park-edges.json for the checks.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTX="${KUBE_CONTEXT:-kind-playhead}"
k() { kubectl --context "$CTX" -n clink "$@"; }
field() { k get clinkjob edges -o jsonpath="{$1}" 2>/dev/null || true; }
api() {
  k exec clink-worker-0 -c worker -- sh -c \
    "curl -s -H \"Authorization: Bearer \$CLINK_AUTH_TOKEN\" http://clink-coordinator:8081$1"
}
parked() { [ "$(field .status.phase)" = Suspended ] && [ "$(field .status.suspendReason)" = Idle ]; }
running() { local id; id="$(field .status.jobID)"; [ "$(field .status.phase)" = Running ] && [ -n "$id" ] && [ "$id" != 0 ]; }
wait_for() {  # wait_for SECONDS PREDICATE
  local i
  for i in $(seq 1 "$1"); do "$2" && return 0; sleep 1; done
  return 1
}
fail() { echo "park: $*" >&2; exit 1; }
# Slots in use across the live Workers, then their capacity.
slots() {
  api /api/v1/workers | python3 -I -c 'import json,sys
ws = [w for w in json.load(sys.stdin)["workers"] if not w["lost"]]
print(sum(w["slots_in_use"] for w in ws), sum(w["slot_capacity"] for w in ws))'
}
running_jobs() {
  api /api/v1/jobs | python3 -I -c 'import json,sys
print(",".join(sorted(j["name"] for j in json.load(sys.stdin)["jobs"] if j["status"] == "RUNNING")))'
}
savepoint() { echo "$(field .status.suspendSavepoint.dir)@$(field .status.suspendSavepoint.checkpointId)"; }

echo "waiting for the edges job to park: it parks after a minute without a record"
wait_for 300 parked || fail "edges did not park: $(field .status.phase): $(field .status.message)"
parked_sp="$(savepoint)"
parked_at="$(field .status.suspendedAt)"
baseline="$(field .status.wakeBaseline)"
read -r used_parked capacity < <(slots)
jobs_parked="$(running_jobs)"
echo "edges parked at ${parked_at}: savepoint ${parked_sp}; ${used_parked} of ${capacity} slots in use, by ${jobs_parked}"

# The half hour's edge requests are still on cdn.edge, and clink's Kafka source
# commits no offsets to its group: the job must stay parked through a few polls.
sleep 15
parked || fail "edges woke with nothing new on cdn.edge: $(field .status.phase): $(field .status.message)"
held="$(field .status.message)"
echo "still parked: ${held}"

delivered="$(date -u +%s)"
python3 -I "${ROOT}/tools/batch.py"
wait_for 120 running || fail "the batch did not wake edges: $(field .status.phase): $(field .status.message)"
woke="$(date -u +%s)"
woke_as="$(field .status.jobID)"
restored="$(api "/api/v1/jobs/${woke_as}" |
  python3 -I -c 'import json,sys; d=json.load(sys.stdin); print("%s@%s" % (d["restore_from_dir"], d["restore_from_checkpoint_id"]))')"
used_running="$used_parked"
for _ in $(seq 1 30); do
  read -r used_running _ < <(slots)
  [ "$used_running" -gt "$used_parked" ] && break
  sleep 1
done
echo "woke as job ${woke_as} $((woke - delivered)) s after the batch, restored from ${restored}; ${used_running} of ${capacity} slots in use"

echo "waiting for edges to park again once the batch is done"
wait_for 300 parked || fail "edges did not park again: $(field .status.phase): $(field .status.message)"
again="$(date -u +%s)"
again_sp="$(savepoint)"
echo "parked again $((again - delivered)) s after the batch: savepoint ${again_sp}"

python3 -I -c 'import json,sys
keys = ["parked_savepoint", "parked_at", "baseline", "slots_parked", "slot_capacity", "running_while_parked",
        "held", "delivered", "woke", "woke_as", "restored_from", "slots_running", "parked_again", "again_savepoint"]
ev = dict(zip(keys, sys.argv[1:]))
ev["baseline"] = json.loads(ev["baseline"] or "[]")
for key in ("slots_parked", "slot_capacity", "slots_running", "delivered", "woke", "parked_again"):
    ev[key] = int(ev[key])
print(json.dumps(ev, indent=1))' \
  "$parked_sp" "$parked_at" "$baseline" "$used_parked" "$capacity" "$jobs_parked" \
  "$held" "$delivered" "$woke" "$woke_as" "$restored" "$used_running" "$again" "$again_sp" \
  > "${ROOT}/state/park-edges.json"
