-- Хранилище «готовых» метрик. Первичные ключи делают запись идемпотентной:
-- повторный пересчёт за дату = UPSERT (ON CONFLICT DO UPDATE), а не новые строки.

CREATE TABLE daily_metrics (
    metric_date  DATE             NOT NULL,
    metric_name  TEXT             NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    computed_at  TIMESTAMPTZ      NOT NULL,
    loaded_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE top_movies (
    metric_date    DATE        NOT NULL,
    rank           SMALLINT    NOT NULL CHECK (rank > 0),
    movie_id       TEXT        NOT NULL,
    views          BIGINT      NOT NULL,
    unique_viewers BIGINT      NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, rank)
);

CREATE TABLE retention_cohort (
    cohort_date    DATE             NOT NULL,
    day_number     SMALLINT         NOT NULL CHECK (day_number BETWEEN 0 AND 7),
    cohort_size    BIGINT           NOT NULL,
    retained_users BIGINT           NOT NULL,
    retention_pct  DOUBLE PRECISION NOT NULL,
    computed_at    TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (cohort_date, day_number)
);

CREATE TABLE device_daily (
    metric_date  DATE        NOT NULL,
    device_type  TEXT        NOT NULL,
    users        BIGINT      NOT NULL,
    events       BIGINT      NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, device_type)
);

-- Журнал запусков агрегации и экспорта (наблюдаемость + повтор упавших экспортов)
CREATE TABLE job_runs (
    id           BIGSERIAL PRIMARY KEY,
    job          TEXT        NOT NULL,          -- aggregate | export
    target_date  DATE        NOT NULL,
    status       TEXT        NOT NULL,          -- success | failed
    rows         BIGINT,
    duration_ms  BIGINT,
    error        TEXT,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX job_runs_job_date_idx ON job_runs (job, target_date, finished_at DESC);
