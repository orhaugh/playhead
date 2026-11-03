#!/usr/bin/env bash
# Give every cloud container a Service in the cluster's `cloud` namespace, so a
# pod reaches redpanda.cloud.svc.cluster.local as it would a managed service.
#
# The containers sit on kind's Docker network, which a pod can route to but not
# name: the network is dual-stack and Docker's resolver answers a container's
# name with its IPv6 address, which the cluster's IPv4-only pods cannot reach.
# A Service without a selector, backed by an EndpointSlice at the container's
# IPv4 address, is the cluster's own way of naming something outside it.
set -euo pipefail
CTX="${KUBE_CONTEXT:-kind-playhead}"
PROJECT="${COMPOSE_PROJECT:-playhead-cloud}"

# service name -> "port[:name] ..." it serves to pods
declare -a SERVICES=(
  "redpanda 9093:kafka 8081:registry"
  "localstack 4566:s3"
  "iceberg-rest 8181:http"
  "fake-gcs 4443:http"
  "azurite 10000:blob"
  "marquez 5000:http"
  "otel-collector 4318:otlp-http"
)

kubectl --context "$CTX" create namespace cloud --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null
manifest=""
for entry in "${SERVICES[@]}"; do
  read -r name ports <<<"$entry"
  cid="$(docker ps -q --filter "label=com.docker.compose.project=${PROJECT}" \
                      --filter "label=com.docker.compose.service=${name}")"
  [ -n "$cid" ] || { echo "cloud-services: ${name} is not running" >&2; exit 1; }
  ip="$(docker inspect -f '{{with index .NetworkSettings.Networks "kind"}}{{.IPAddress}}{{end}}' "$cid")"
  [ -n "$ip" ] || { echo "cloud-services: ${name} has no IPv4 address on the kind network" >&2; exit 1; }
  svc_ports="" eps_ports=""
  for p in $ports; do
    port="${p%%:*}" pname="${p##*:}"
    svc_ports+=$'\n'"    - name: ${pname}"$'\n'"      port: ${port}"$'\n'"      targetPort: ${port}"
    eps_ports+=$'\n'"  - name: ${pname}"$'\n'"    port: ${port}"$'\n'"    protocol: TCP"
  done
  manifest+="---
apiVersion: v1
kind: Service
metadata:
  name: ${name}
  namespace: cloud
spec:
  ports:${svc_ports}
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: ${name}
  namespace: cloud
  labels:
    kubernetes.io/service-name: ${name}
addressType: IPv4
endpoints:
  - addresses: [\"${ip}\"]
ports:${eps_ports}
"
  echo "cloud-services: ${name}.cloud.svc.cluster.local -> ${ip} (${ports})"
done
printf '%s' "$manifest" | kubectl --context "$CTX" apply -f - >/dev/null
