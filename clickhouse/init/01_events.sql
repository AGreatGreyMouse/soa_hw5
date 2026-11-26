-- Миграции ClickHouse применяются автоматически при первом старте контейнера
-- (/docker-entrypoint-initdb.d). Все операторы идемпотентны (IF NOT EXISTS).
CREATE DATABASE IF NOT EXISTS cinema;

-- 1) Kafka Engine: «окно» в топик movie-events. Формат AvroConfluent: ClickHouse сам берёт схему
--    из Schema Registry по schema_id из заголовка сообщения. Имена колонок = имена полей Avro.
CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id          String,
    user_id           String,
    movie_id          String,
    event_type        Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                            'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    `timestamp`       DateTime64(3, 'UTC'),
    device_type       Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id        String,
    progress_seconds  Nullable(Int32)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'clickhouse-movie-events',
         kafka_format = 'AvroConfluent',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 1000,
         kafka_handle_error_mode = 'stream',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

-- 2) Постоянное хранилище raw-событий.
--    ReplacingMergeTree: повторная доставка (at-least-once) с тем же event_id схлопывается при слияниях
--    (в агрегатах читаем с FINAL).
--    PARTITION BY месяц: дешёвое удаление/архивирование старых данных, мало партиций.
--    ORDER BY (event_date, event_type, user_id, ...): все наши запросы фильтруют по дню
--    и типу события и считают уникальных пользователей.
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id          UUID,
    user_id           String,
    movie_id          String,
    event_type        Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                            'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    `timestamp`       DateTime64(3, 'UTC'),
    event_date        Date,
    device_type       Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id        String,
    progress_seconds  Nullable(Int32),
    kafka_partition   UInt16,
    kafka_offset      UInt64,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, event_id);

-- 3) Перекладка Kafka -> MergeTree. MV срабатывает на каждый блок, прочитанный из топика.
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv TO cinema.movie_events AS
SELECT toUUID(event_id)          AS event_id,
       user_id,
       movie_id,
       event_type,
       `timestamp`,
       toDate(`timestamp`)       AS event_date,
       device_type,
       session_id,
       progress_seconds,
       _partition                AS kafka_partition,
       _offset                   AS kafka_offset
FROM cinema.movie_events_queue
WHERE length(_error) = 0;

-- Битые сообщения (не прошли разбор Avro) не теряются молча, а складываются отдельно.
CREATE TABLE IF NOT EXISTS cinema.movie_events_errors
(
    received_at DateTime DEFAULT now(),
    kafka_partition UInt16,
    kafka_offset UInt64,
    raw String,
    error String
)
ENGINE = MergeTree ORDER BY received_at;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_errors_mv TO cinema.movie_events_errors AS
SELECT _partition AS kafka_partition, _offset AS kafka_offset, _raw_message AS raw, _error AS error
FROM cinema.movie_events_queue
WHERE length(_error) > 0;
