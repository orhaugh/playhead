#!/usr/bin/env bash
# Create what the jobs expect to find in the cloud: the topics, the registry
# subjects the apps publish under, the S3 bucket under the Iceberg warehouse
# and the warehouse's namespace, the CDN vendor's GCS bucket and the finance
# team's Azure container. Idempotent: whatever exists already is left as it is.
#
# Only the v1 playback schema is registered here. App 5.0's v2 is registered
# when it rolls out, mid-run (tools/feed.py), as a team shipping it would.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose --progress quiet -f "${ROOT}/cloud/compose.yml" --env-file "${ROOT}/state/cloud.env")
rpk() { "${COMPOSE[@]}" exec -T redpanda rpk -X brokers=redpanda:9092 -X registry.hosts=localhost:8081 "$@"; }

for t in playback.events:4 cdn.edge:4 qoe.alerts:1; do
  rpk topic create "${t%%:*}" -p "${t##*:}" >/dev/null 2>&1 || true
done
"${COMPOSE[@]}" cp "${ROOT}/schemas/playback-v1.avsc" redpanda:/tmp/playback-v1.avsc >/dev/null
"${COMPOSE[@]}" cp "${ROOT}/schemas/edge_request.proto" redpanda:/tmp/edge_request.proto >/dev/null
# The registry takes writes once its own client has reached the broker and
# made its _schemas topic, which on a slow start is after the container reports
# healthy; until then a write times out. Registering a schema twice returns the
# same id, so retry within a bound.
register() {
  for _ in $(seq 1 30); do
    rpk registry schema create "$@" >/dev/null 2>&1 && return 0
    sleep 2
  done
  rpk registry schema create "$@" >/dev/null
}
register playback.events-value --schema /tmp/playback-v1.avsc --type avro
register cdn.edge-value --schema /tmp/edge_request.proto --type protobuf
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

# Azurite's account and key are this run's (state/cloud.env); writes do not
# create a container, so the finance team's is made here.
"${ROOT}/.venv/bin/python" -I - "${ROOT}/state/cloud.env" <<'PY'
import sys
import pyarrow.fs as pafs
env = dict(line.strip().split("=", 1) for line in open(sys.argv[1]) if "=" in line)
azure = pafs.AzureFileSystem(account_name="playhead", account_key=env["PLAYHEAD_AZURE_KEY"],
                             blob_storage_authority="127.0.0.1:10000", blob_storage_scheme="http",
                             dfs_storage_authority="127.0.0.1:10000", dfs_storage_scheme="http")
azure.create_dir("finance")
PY
echo "provision: azure://playhead/finance"

