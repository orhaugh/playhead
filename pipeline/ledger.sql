-- Finance's billing ledger: one row for every playback session that started,
-- written to the finance team's Azure container exactly once, so a session is
-- billed once whatever the cluster went through. The storage account's key
-- reaches the job from a Kubernetes Secret as an environment variable on each
-- Worker (env://), never as text in this file: the SQL is compiled into a job
-- graph that anyone who can read the job can see.
CREATE TABLE playback (
    event_id     VARCHAR,
    session_id   VARCHAR,
    title_id     VARCHAR,
    device       VARCHAR,
    app_version  VARCHAR,
    event_type   VARCHAR,
    ts           BIGINT,
    startup_ms   BIGINT,
    stall_ms     BIGINT,
    bitrate_kbps BIGINT,
    error_code   VARCHAR,
    network      VARCHAR
) WITH (
    connector           = 'kafka',
    brokers             = 'redpanda.cloud.svc.cluster.local:9093',
    topic               = 'playback.events',
    group_id            = 'playhead-ledger',
    format              = 'avro',
    schema_registry_url = 'http://redpanda.cloud.svc.cluster.local:8081',
    decode_error        = 'skip'
);

CREATE TABLE billable_session (
    session_id  VARCHAR,
    title_id    VARCHAR,
    device      VARCHAR,
    app_version VARCHAR,
    started_at  BIGINT
) WITH (
    connector              = 'azure_parquet',
    account_name           = 'playhead',
    account_key            = 'env://AZURE_ACCOUNT_KEY',
    container              = 'finance',
    prefix                 = 'billable-sessions',
    blob_storage_authority = 'azurite.cloud.svc.cluster.local:10000',
    blob_storage_scheme    = 'http',
    delivery_guarantee     = 'exactly_once'
);

INSERT INTO billable_session
SELECT session_id, title_id, device, app_version, ts
FROM playback
WHERE event_type = 'START';
