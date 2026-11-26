import os

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "app")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "app")
CH_DB = os.getenv("CLICKHOUSE_DB", "cinema")

PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@postgres:5432/analytics")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://s3:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")

# Расписание (настраивается через переменные окружения)
AGGREGATION_INTERVAL_SECONDS = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60"))
RECOMPUTE_DAYS = int(os.getenv("RECOMPUTE_DAYS", "8"))       # сегодня + 7 дней назад (retention D7 «дозревает»)
EXPORT_INTERVAL_SECONDS = int(os.getenv("EXPORT_INTERVAL_SECONDS", "300"))
TOP_N = int(os.getenv("TOP_N_MOVIES", "10"))

PG_WRITE_ATTEMPTS = int(os.getenv("PG_WRITE_ATTEMPTS", "3"))
