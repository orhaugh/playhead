#!/usr/bin/env bash
# The cluster's secrets, made once per checkout into state/ (gitignored) and
# given to Kubernetes as Secrets in namespace clink, before the chart starts a
# pod that mounts them:
#
#   clink-auth             the HTTP API token every clink pod requires and
#                          presents (CLINK_AUTH_TOKEN)
#   clink-tls-coordinator  the Coordinators' certificate, naming the Service and
#                          each HA Coordinator's stable name, and its CA
#   clink-tls-worker       the Workers' client certificate: mutual TLS
#   playhead-azure         the finance account's key, which the egress job
#                          reads through env://AZURE_ACCOUNT_KEY
#
# Nothing here is printed; the files stay under state/ with mode 0600.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CTX="${KUBE_CONTEXT:-kind-playhead}"
NS=clink
PKI=state/pki
umask 077
mkdir -p "$PKI"

if [ ! -f "$PKI/ca.key" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=playhead CA" \
    -keyout "$PKI/ca.key" -out "$PKI/ca.crt" 2>/dev/null
fi
issue() {  # issue <name> <subjectAltName> <extendedKeyUsage>
  [ -f "$PKI/$1.crt" ] && return
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$1" \
    -keyout "$PKI/$1.key" -out "$PKI/$1.csr" 2>/dev/null
  openssl x509 -req -in "$PKI/$1.csr" -CA "$PKI/ca.crt" -CAkey "$PKI/ca.key" \
    -CAcreateserial -days 365 -out "$PKI/$1.crt" \
    -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=%s\n' "$2" "$3") 2>/dev/null
}
svc=clink-coordinator
issue coordinator "DNS:$svc,DNS:$svc.$NS,DNS:$svc.$NS.svc,DNS:$svc.$NS.svc.cluster.local,DNS:*.$svc-headless.$NS.svc.cluster.local" serverAuth
issue worker "DNS:clink-worker" clientAuth
# Without a newline: a header cannot carry one, and curl sends this token too.
[ -f state/auth-token ] || printf '%s' "$(openssl rand -hex 32)" > state/auth-token
# shellcheck disable=SC1091
. state/cloud.env
printf 'AZURE_ACCOUNT_KEY=%s\n' "$PLAYHEAD_AZURE_KEY" > state/azure.env

kubectl --context "$CTX" get namespace "$NS" >/dev/null 2>&1 \
  || kubectl --context "$CTX" create namespace "$NS" >/dev/null
secret() { kubectl --context "$CTX" -n "$NS" create secret "$@" --dry-run=client -o yaml \
  | kubectl --context "$CTX" -n "$NS" apply -f - >/dev/null; }
secret generic clink-auth --from-file=token=state/auth-token
for who in coordinator worker; do
  secret generic "clink-tls-$who" --type=kubernetes.io/tls --from-file=tls.crt="$PKI/$who.crt" \
    --from-file=tls.key="$PKI/$who.key" --from-file=ca.crt="$PKI/ca.crt"
done
secret generic playhead-azure --from-env-file=state/azure.env
echo "secrets: clink-auth, clink-tls-coordinator, clink-tls-worker, playhead-azure"
