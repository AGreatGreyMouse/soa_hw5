"""Вычисление бизнес-метрик в ClickHouse и материализация в agg_* таблицы.

Пересчёт за дату идемпотентен: DELETE за дату -> INSERT ... SELECT.
Читаем raw с FINAL, чтобы дубли (at-least-once) не искажали метрики.
"""
import datetime as dt

import clickhouse_connect

from app import config

AGG_TABLES = {
    "agg_daily_metrics": "metric_date",
    "agg_top_movies": "metric_date",
    "agg_retention_cohort": "cohort_date",
    "agg_device_daily": "metric_date",
}

SQL_RAW_COUNT = "SELECT count() FROM cinema.movie_events FINAL WHERE event_date = {d:Date}"

SQL_DAILY = """
INSERT INTO cinema.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT {d:Date}, kv.1, kv.2, toDateTime64({ts:String}, 3, 'UTC')
FROM
(
    SELECT
        uniqExact(user_id)                                        AS dau,
        avgIf(progress_seconds, event_type = 'VIEW_FINISHED')     AS avg_watch,
        countIf(event_type = 'VIEW_STARTED')                      AS started,
        countIf(event_type = 'VIEW_FINISHED')                     AS finished,
        count()                                                   AS events_total
    FROM cinema.movie_events FINAL
    WHERE event_date = {d:Date}
)
ARRAY JOIN [
    ('dau',               toFloat64(dau)),
    ('avg_watch_seconds', round(ifNotFinite(coalesce(toFloat64(avg_watch), 0), 0), 1)),
    ('view_conversion',   if(started = 0, 0, round(finished / started, 4))),
    ('views_started',     toFloat64(started)),
    ('events_total',      toFloat64(events_total))
] AS kv
"""

# Когорта = пользователи, у которых ПЕРВЫЙ VIEW_STARTED за всё время пришёлся на день d.
# Возврат в день N = любое событие пользователя в день d + N.
SQL_COHORT = """
INSERT INTO cinema.agg_retention_cohort
    (cohort_date, day_number, cohort_size, retained_users, retention_pct, computed_at)
WITH cohort AS
(
    SELECT user_id
    FROM cinema.movie_events FINAL
    WHERE event_type = 'VIEW_STARTED'
    GROUP BY user_id
    HAVING min(event_date) = {d:Date}
)
SELECT
    {d:Date}                                                   AS cohort_date,
    toUInt8(days.dn)                                           AS day_number,
    c.cs                                                       AS cohort_size,
    r.retained                                                 AS retained_users,
    if(c.cs = 0, 0, round(100 * r.retained / c.cs, 2))         AS retention_pct,
    toDateTime64({ts:String}, 3, 'UTC')                                         AS computed_at
FROM (SELECT toInt64(arrayJoin(range(8))) AS dn) AS days
LEFT JOIN
(
    SELECT toInt64(dateDiff('day', {d:Date}, event_date)) AS dn,
           uniqExact(user_id)                             AS retained
    FROM cinema.movie_events FINAL
    WHERE event_date BETWEEN {d:Date} AND {d:Date} + 7
      AND user_id IN (SELECT user_id FROM cohort)
    GROUP BY dn
) AS r ON days.dn = r.dn
CROSS JOIN (SELECT count() AS cs FROM cohort) AS c
WHERE c.cs > 0
  AND {d:Date} + days.dn <= today()          -- будущие дни ещё не наступили
"""

SQL_RETENTION_TO_DAILY = """
INSERT INTO cinema.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT cohort_date, if(day_number = 1, 'retention_d1', 'retention_d7'), retention_pct, toDateTime64({ts:String}, 3, 'UTC')
FROM cinema.agg_retention_cohort FINAL
WHERE cohort_date = {d:Date} AND day_number IN (1, 7)
"""

SQL_TOP = """
INSERT INTO cinema.agg_top_movies (metric_date, rank, movie_id, views, unique_viewers, computed_at)
SELECT {d:Date},
       toUInt16(row_number() OVER (ORDER BY views DESC, unique_viewers DESC, movie_id)) AS rank,
       movie_id, views, unique_viewers, toDateTime64({ts:String}, 3, 'UTC')
FROM
(
    SELECT movie_id,
           countIf(event_type = 'VIEW_STARTED')              AS views,
           uniqExactIf(user_id, event_type = 'VIEW_STARTED') AS unique_viewers
    FROM cinema.movie_events FINAL
    WHERE event_date = {d:Date}
    GROUP BY movie_id
    HAVING views > 0
)
ORDER BY rank
LIMIT %(top_n)d
"""

SQL_DEVICES = """
INSERT INTO cinema.agg_device_daily (metric_date, device_type, users, events, computed_at)
SELECT {d:Date}, toString(device_type), uniqExact(user_id), count(), toDateTime64({ts:String}, 3, 'UTC')
FROM cinema.movie_events FINAL
WHERE event_date = {d:Date}
GROUP BY device_type
"""


def client():
    return clickhouse_connect.get_client(host=config.CH_HOST, port=config.CH_PORT, username=config.CH_USER,
                                         password=config.CH_PASSWORD, database=config.CH_DB)


def compute(day: dt.date) -> dict:
    """Пересчитать все агрегаты за день в ClickHouse и вернуть их для выгрузки в PostgreSQL."""
    ch = client()
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]   # одно время на весь цикл
    p = {"d": day, "ts": ts}
    raw_events = ch.query(SQL_RAW_COUNT, parameters={"d": day}).result_rows[0][0]

    for table, col in AGG_TABLES.items():
        ch.command(f"DELETE FROM cinema.{table} WHERE {col} = {{d:Date}}", parameters={"d": day})
    ch.command(SQL_DAILY, parameters=p)
    ch.command(SQL_COHORT, parameters=p)
    ch.command(SQL_RETENTION_TO_DAILY, parameters=p)
    ch.command(SQL_TOP % {"top_n": config.TOP_N}, parameters=p)
    ch.command(SQL_DEVICES, parameters=p)

    q = lambda sql: ch.query(sql, parameters={"d": day}).result_rows  # noqa: E731
    result = {
        "raw_events": raw_events,
        "daily": q("SELECT metric_date, metric_name, value, computed_at FROM cinema.agg_daily_metrics FINAL "
                   "WHERE metric_date = {d:Date} ORDER BY metric_name"),
        "top": q("SELECT metric_date, rank, movie_id, views, unique_viewers, computed_at "
                 "FROM cinema.agg_top_movies FINAL WHERE metric_date = {d:Date} ORDER BY rank"),
        "cohort": q("SELECT cohort_date, day_number, cohort_size, retained_users, retention_pct, computed_at "
                    "FROM cinema.agg_retention_cohort FINAL WHERE cohort_date = {d:Date} ORDER BY day_number"),
        "devices": q("SELECT metric_date, device_type, users, events, computed_at "
                     "FROM cinema.agg_device_daily FINAL WHERE metric_date = {d:Date} ORDER BY device_type"),
    }
    ch.close()
    return result
