-- Quality of experience per title, minute by minute, kept in an Iceberg table
-- on S3 for the analysts. A window is final when it closes, so the table only
-- grows: each checkpoint's rows are committed as one Iceberg snapshot when the
-- checkpoint completes, and a replay after a restart commits nothing twice.
-- The players' events, as the apps publish them: Avro, framed for the Schema
-- Registry. Apps before 5.0 write the v1 schema; 5.0 writes v2, which adds the
-- network. Each message carries its writer's schema id, so one table reads
-- both: a v1 event has no network, and the column is NULL.
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
    group_id            = 'playhead-qoe',
    format              = 'avro',
    schema_registry_url = 'http://redpanda.cloud.svc.cluster.local:8081',
    -- A message no schema decodes (a wrong magic byte, an unknown id) is
    -- dropped and counted rather than stopping the job.
    decode_error        = 'skip',
    event_time_column   = 'ts',
    watermark_lag_ms    = '15000'
);

CREATE TABLE title_minute (
    title_id       VARCHAR,
    window_start   BIGINT,
    window_end     BIGINT,
    starts         BIGINT,
    plays          BIGINT,
    failures       BIGINT,
    rebuffers      BIGINT,
    stall_ms       BIGINT,
    startup_ms     BIGINT,
    cellular_plays BIGINT
) WITH (
    connector     = 'iceberg',
    warehouse     = 's3://lake/warehouse/',
    namespace     = 'qoe',
    "table"       = 'title_minute',
    catalog_uri   = 'http://iceberg-rest.cloud.svc.cluster.local:8181',
    s3_endpoint   = 'http://localstack.cloud.svc.cluster.local:4566',
    s3_region     = 'us-east-1',
    s3_path_style = 'true',
    -- LocalStack's documented placeholder credentials; it checks none.
    s3_access_key = 'test',
    s3_secret_key = 'test'
);

INSERT INTO title_minute
SELECT title_id, window_start, window_end,
       SUM(is_start)      AS starts,
       SUM(is_play)       AS plays,
       SUM(is_failure)    AS failures,
       SUM(is_rebuffer)   AS rebuffers,
       SUM(stalled_ms)    AS stall_ms,
       SUM(started_ms)    AS startup_ms,
       SUM(cellular_play) AS cellular_plays
FROM (
    -- An aggregate takes a column, so each event is first scored as columns.
    SELECT title_id, ts,
           CASE WHEN event_type = 'START' THEN 1 ELSE 0 END       AS is_start,
           CASE WHEN event_type = 'FIRST_FRAME' THEN 1 ELSE 0 END AS is_play,
           CASE WHEN event_type = 'ERROR' THEN 1 ELSE 0 END       AS is_failure,
           CASE WHEN event_type = 'REBUFFER' THEN 1 ELSE 0 END    AS is_rebuffer,
           COALESCE(stall_ms, 0)                                  AS stalled_ms,
           COALESCE(startup_ms, 0)                                AS started_ms,
           CASE WHEN event_type = 'FIRST_FRAME' AND network = 'CELLULAR'
                THEN 1 ELSE 0 END                                 AS cellular_play
    FROM playback
) AS scored
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), title_id;
