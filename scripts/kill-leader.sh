#!/usr/bin/env bash
# Kill the leading Coordinator's pod, as a node failure would: no grace period.
# The standby takes the leadership over the shared HA directory, recovers the
# jobs, and the StatefulSet starts the killed pod again as the new standby.
set -euo pipefail
CTX="${KUBE_CONTEXT:-kind-playhead}"
k() { kubectl --context "$CTX" -n clink "$@"; }
for pod in clink-coordinator-0 clink-coordinator-1; do
  host="$(k exec "$pod" -- cat /var/lib/clink/ha/active-leader.json 2>/dev/null |
    python3 -I -c 'import json,sys; print(json.load(sys.stdin)["host"])' 2>/dev/null || true)"
  [ -n "$host" ] && break
done
[ -n "${host:-}" ] || { echo "kill-leader: no leader recorded" >&2; exit 1; }
# The leader advertises <pod>.clink-coordinator-headless.<namespace>.svc.cluster.local.
leader="${host%%.*}"
k delete pod "$leader" --grace-period=0 --force --wait=false >/dev/null 2>&1
echo "killed the leading Coordinator, ${leader}"
