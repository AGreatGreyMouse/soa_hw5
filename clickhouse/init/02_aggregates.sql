-- Материализованные агрегаты (п.5). Их заполняет Aggregation Service, их же читает Grafana.
-- Пересчёт за дату: DELETE за дату + INSERT ... SELECT, поэтому повторный запуск не плодит строк.

-- Дневные скалярные метрики в «длинном» формате: (дата, метрика, значение)
CREATE TABLE IF NOT EXISTS cinema.agg_daily_metrics
(
    metric_date  Date,
    metric_name  LowCardinality(String),   -- dau, avg_watch_seconds, view_conversion, retention_d1, retention_d7, events_total
    value        Float64,
    computed_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, metric_name);

-- Топ фильмов за день
CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    metric_date    Date,
    rank           UInt16,
    movie_id       String,
    views          UInt64,
    unique_viewers UInt64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, rank);

-- Когортный retention: когорта = день первого просмотра, day_number = 0..7
CREATE TABLE IF NOT EXISTS cinema.agg_retention_cohort
(
    cohort_date    Date,
    day_number     UInt8,
    cohort_size    UInt64,
    retained_users UInt64,
    retention_pct  Float64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);

-- Распределение по устройствам за день
CREATE TABLE IF NOT EXISTS cinema.agg_device_daily
(
    metric_date  Date,
    device_type  LowCardinality(String),
    users        UInt64,
    events       UInt64,
    computed_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, device_type);
