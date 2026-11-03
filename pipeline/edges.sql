-- The CDN vendor's view: requests, bytes, server errors and time to first byte
-- per edge location per minute, from the edge logs it reports in Protobuf.
-- Published to the vendor's GCS bucket exactly once: each checkpoint's file is
-- staged, then made visible atomically when the checkpoint commits.
CREATE TABLE edge (
    request_id VARCHAR,
    session_id VARCHAR,
    pop        VARCHAR,
    bytes      BIGINT,
    ttfb_ms    BIGINT,
    status     BIGINT,
    ts         BIGINT
) WITH (
    connector           = 'kafka',
    brokers             = 'redpanda.cloud.svc.cluster.local:9093',
    topic               = 'cdn.edge',
    group_id            = 'playhead-edges',
    format              = 'protobuf',
    schema_registry_url = 'http://redpanda.cloud.svc.cluster.local:8081',
    decode_error        = 'skip',
    event_time_column   = 'ts',
    watermark_lag_ms    = '15000'
);

CREATE TABLE pop_minute (
    pop           VARCHAR,
    window_start  BIGINT,
    window_end    BIGINT,
    requests      BIGINT,
    bytes         BIGINT,
    server_errors BIGINT,
    ttfb_ms       BIGINT
) WITH (
    connector          = 'gcs_parquet',
    bucket             = 'cdn-vendor',
    prefix             = 'pop-minute',
    endpoint_override  = 'fake-gcs.cloud.svc.cluster.local:4443',
    scheme             = 'http',
    anonymous          = 'true',
    delivery_guarantee = 'exactly_once'
);

INSERT INTO pop_minute
SELECT pop, window_start, window_end,
       COUNT(*)            AS requests,
       SUM(bytes)          AS bytes,
       SUM(is_server_error) AS server_errors,
       SUM(ttfb_ms)        AS ttfb_ms
FROM (
    SELECT pop, ts, bytes, ttfb_ms,
           CASE WHEN status >= 500 THEN 1 ELSE 0 END AS is_server_error
    FROM edge
) AS scored
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), pop;
