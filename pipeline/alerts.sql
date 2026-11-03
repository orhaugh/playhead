-- A title whose sessions fail to start: at least five failures in a minute,
-- and more failures than successes. Each alert is published to Kafka in Avro,
-- its schema derived from the columns and registered, inside a transaction
-- that commits with the job's checkpoint, so a read_committed consumer sees
-- each alert once.
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
    group_id            = 'playhead-alerts',
    format              = 'avro',
    schema_registry_url = 'http://redpanda.cloud.svc.cluster.local:8081',
    -- A message no schema decodes (a wrong magic byte, an unknown id) is
    -- dropped and counted rather than stopping the job.
    decode_error        = 'skip',
    -- The apps' producers put unkeyed records on whichever partition they
    -- are on, and a partition can go quiet for the rest of the half hour.
    -- clink takes the lowest partition watermark, so a quiet partition would
    -- hold every window open; after ten seconds of silence it is set aside
    -- until it speaks again.
    idle_timeout_ms     = '10000',
    event_time_column   = 'ts',
    watermark_lag_ms    = '15000'
);

CREATE TABLE qoe_alerts (
    title_id     VARCHAR,
    window_start BIGINT,
    window_end   BIGINT,
    starts       BIGINT,
    failures     BIGINT
) WITH (
    connector           = 'kafka',
    brokers             = 'redpanda.cloud.svc.cluster.local:9093',
    topic               = 'qoe.alerts',
    format              = 'avro',
    schema_registry_url = 'http://redpanda.cloud.svc.cluster.local:8081',
    delivery_guarantee  = 'exactly_once',
    transactional_id    = 'playhead-alerts'
);

INSERT INTO qoe_alerts
SELECT title_id, window_start, window_end,
       SUM(is_start)   AS starts,
       SUM(is_failure) AS failures
FROM (
    SELECT title_id, ts,
           CASE WHEN event_type = 'START' THEN 1 ELSE 0 END AS is_start,
           CASE WHEN event_type = 'ERROR' THEN 1 ELSE 0 END AS is_failure
    FROM playback
) AS scored
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), title_id
HAVING SUM(is_failure) >= 5 AND SUM(is_failure) * 2 > SUM(is_start);
