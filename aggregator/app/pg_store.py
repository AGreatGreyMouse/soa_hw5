"""Выгрузка агрегатов в PostgreSQL: идемпотентный UPSERT в одной транзакции + retry."""
import logging
import time

import psycopg

from app import config

log = logging.getLogger("pg")


def _write(conn, day, data):
    with conn.transaction():
        cur = conn.cursor()
        cur.executemany(
            """INSERT INTO daily_metrics (metric_date, metric_name, value, computed_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (metric_date, metric_name)
               DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at, loaded_at = now()""",
            data["daily"])
        # Топ может сократиться (меньше фильмов) — заменяем список целиком внутри транзакции
        cur.execute("DELETE FROM top_movies WHERE metric_date = %s", (day,))
        cur.executemany(
            """INSERT INTO top_movies (metric_date, rank, movie_id, views, unique_viewers, computed_at)
               VALUES (%s, %s, %s, %s, %s, %s)""", data["top"])
        cur.executemany(
            """INSERT INTO retention_cohort (cohort_date, day_number, cohort_size, retained_users,
                                             retention_pct, computed_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (cohort_date, day_number)
               DO UPDATE SET cohort_size = EXCLUDED.cohort_size, retained_users = EXCLUDED.retained_users,
                             retention_pct = EXCLUDED.retention_pct, computed_at = EXCLUDED.computed_at""",
            data["cohort"])
        cur.executemany(
            """INSERT INTO device_daily (metric_date, device_type, users, events, computed_at)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (metric_date, device_type)
               DO UPDATE SET users = EXCLUDED.users, events = EXCLUDED.events, computed_at = EXCLUDED.computed_at""",
            data["devices"])
    return len(data["daily"]) + len(data["top"]) + len(data["cohort"]) + len(data["devices"])


def save(day, data) -> int:
    last = None
    for attempt in range(1, config.PG_WRITE_ATTEMPTS + 1):
        try:
            with psycopg.connect(config.PG_DSN, connect_timeout=5) as conn:
                return _write(conn, day, data)
        except psycopg.Error as exc:
            last = exc
            delay = 0.5 * 2 ** (attempt - 1)
            log.warning("PostgreSQL write failed for %s (attempt %s/%s): %s; retry in %.1fs",
                        day, attempt, config.PG_WRITE_ATTEMPTS, exc, delay)
            if attempt < config.PG_WRITE_ATTEMPTS:
                time.sleep(delay)
    raise last


def log_run(job, day, status, rows, duration_ms, error, started_at):
    """Журнал запусков; ошибки журнала не должны ронять сам job."""
    try:
        with psycopg.connect(config.PG_DSN, connect_timeout=3) as conn:
            conn.execute(
                """INSERT INTO job_runs (job, target_date, status, rows, duration_ms, error, started_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (job, day, status, rows, duration_ms, error, started_at))
    except psycopg.Error as exc:
        log.warning("cannot write job_runs: %s", exc)


def read_day(day) -> dict:
    with psycopg.connect(config.PG_DSN, connect_timeout=5) as conn:
        cur = conn.cursor()
        q = lambda sql: cur.execute(sql, (day,)).fetchall()  # noqa: E731
        return {
            "daily": q("SELECT metric_name, value, computed_at FROM daily_metrics "
                       "WHERE metric_date = %s ORDER BY metric_name"),
            "top": q("SELECT rank, movie_id, views, unique_viewers, computed_at FROM top_movies "
                     "WHERE metric_date = %s ORDER BY rank"),
            "cohort": q("SELECT day_number, cohort_size, retained_users, retention_pct, computed_at "
                        "FROM retention_cohort WHERE cohort_date = %s ORDER BY day_number"),
            "devices": q("SELECT device_type, users, events, computed_at FROM device_daily "
                         "WHERE metric_date = %s ORDER BY device_type"),
        }
