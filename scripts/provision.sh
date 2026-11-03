#!/usr/bin/env bash
# Create what the jobs expect to find in the cloud: the topics, the registry
# subjects the apps publish under, the S3 bucket under the Iceberg warehouse
# and the warehouse's namespace, and the CDN vendor's GCS bucket. Idempotent:
# whatever exists already is left as it is.
#
# Only the v1 playback schema is registered here. App 5.0's v2 is registered
# when it rolls out, mid-run (tools/feed.py), as a team shipping it would.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/cloud/compose.yml" --env-file "${ROOT}/state/cloud.env")
rpk() { "${COMPOSE[@]}" exec -T redpanda rpk -X brokers=redpanda:9092 -X registry.hosts=localhost:8081 "$@"; }

for t in playback.events:4 cdn.edge:4 qoe.alerts:1; do
  rpk topic create "${t%%:*}" -p "${t##*:}" >/dev/null 2>&1 || true
done
"${COMPOSE[@]}" cp "${ROOT}/schemas/playback-v1.avsc" redpanda:/tmp/playback-v1.avsc >/dev/null
"${COMPOSE[@]}" cp "${ROOT}/schemas/edge_request.proto" redpanda:/tmp/edge_request.proto >/dev/null
rpk registry schema create playback.events-value --schema /tmp/playback-v1.avsc --type avro >/dev/null
rpk registry schema create cdn.edge-value --schema /tmp/edge_request.proto --type protobuf >/dev/null
echo "provision: topics playback.events, cdn.edge, qoe.alerts; subjects playback.events-value (v1), cdn.edge-value"

"${COMPOSE[@]}" exec -T localstack awslocal s3 mb s3://lake >/dev/null 2>&1 || true
curl -fsS -o /dev/null -X POST -H 'Content-Type: application/json' \
  -d '{"namespace":["qoe"]}' http://127.0.0.1:8181/v1/namespaces 2>/dev/null || true
curl -fsS -o /dev/null http://127.0.0.1:8181/v1/namespaces/qoe \
  || { echo "provision: the Iceberg namespace qoe is missing" >&2; exit 1; }
echo "provision: s3://lake, Iceberg namespace qoe"

curl -fsS -o /dev/null -X POST -H 'Content-Type: application/json' -d '{"name":"cdn-vendor"}' \
  "http://127.0.0.1:4443/storage/v1/b?project=playhead" 2>/dev/null || true
curl -fsS -o /dev/null http://127.0.0.1:4443/storage/v1/b/cdn-vendor \
  || { echo "provision: the GCS bucket cdn-vendor is missing" >&2; exit 1; }
echo "provision: gs://cdn-vendor"
