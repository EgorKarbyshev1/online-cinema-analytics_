CREATE TABLE IF NOT EXISTS movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp Int64,
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'cinema-clickhouse',
         kafka_format = 'AvroConfluent',
         kafka_num_consumers = 1,
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS movie_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(6, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate32(timestamp), user_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_ingest TO movie_events AS
SELECT toUUID(event_id) AS event_id, user_id, movie_id, event_type,
       fromUnixTimestamp64Micro(timestamp, 'UTC') AS timestamp, device_type, session_id, progress_seconds
FROM movie_events_queue;

CREATE TABLE IF NOT EXISTS daily_metrics
(
    date Date32,
    event_count UInt64,
    dau UInt64,
    average_watch_time Nullable(Float64),
    view_conversion Float64
)
ENGINE = ReplacingMergeTree
ORDER BY date;

CREATE TABLE IF NOT EXISTS top_movies
(
    date Date32,
    movies Array(Tuple(String, UInt64))
)
ENGINE = ReplacingMergeTree
ORDER BY date;

CREATE TABLE IF NOT EXISTS retention_cohorts
(
    date Date32,
    cohort_size UInt64,
    retention Array(Nullable(Float64))
)
ENGINE = ReplacingMergeTree
ORDER BY date;
