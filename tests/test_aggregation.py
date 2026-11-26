"""Тесты сервиса агрегации: ручной пересчёт, идемпотентность в PostgreSQL, экспорт в S3."""
import json
import os
from datetime import date, timedelta

import boto3
import psycopg
import requests

from conftest import AGGREGATOR_URL

PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@localhost:5433/analytics")
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


def pg_count(sql, params):
    with psycopg.connect(PG_DSN) as conn:
        return conn.execute(sql, params).fetchone()[0]


def test_manual_recompute_is_idempotent():
    r1 = requests.post(f"{AGGREGATOR_URL}/aggregate", params={"date": YESTERDAY}, timeout=120)
    assert r1.status_code == 200, r1.text
    body = r1.json()
    for m in ("dau", "avg_watch_seconds", "view_conversion", "retention_d1"):
        assert m in body["metrics"], body
    assert body["metrics"]["dau"] > 0 and body["raw_events"] > 0
    n1 = pg_count("SELECT count(*) FROM daily_metrics WHERE metric_date = %s", (YESTERDAY,))

    r2 = requests.post(f"{AGGREGATOR_URL}/aggregate", params={"date": YESTERDAY}, timeout=120)
    assert r2.status_code == 200
    n2 = pg_count("SELECT count(*) FROM daily_metrics WHERE metric_date = %s", (YESTERDAY,))
    assert n1 == n2                                   # UPSERT, а не дубли
    assert pg_count("SELECT count(*) FROM retention_cohort WHERE cohort_date = %s AND day_number = 0",
                    (YESTERDAY,)) == 1


def test_export_to_s3_overwrites_same_key():
    for _ in range(2):
        r = requests.post(f"{AGGREGATOR_URL}/export", params={"date": YESTERDAY}, timeout=60)
        assert r.status_code == 200, r.text
    assert r.json()["uri"] == f"s3://movie-analytics/daily/{YESTERDAY}/aggregates.json"

    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="movie-analytics", Prefix=f"daily/{YESTERDAY}/")["Contents"]]
    assert keys == [f"daily/{YESTERDAY}/aggregates.json"]          # один файл, без дубликатов
    doc = json.loads(s3.get_object(Bucket="movie-analytics", Key=keys[0])["Body"].read())
    assert doc["date"] == YESTERDAY
    for m in ("dau", "view_conversion", "avg_watch_seconds", "retention_d1"):
        assert "computed_at" in doc["metrics"][m]
    assert doc["top_movies"] and doc["retention_cohort"]


def test_bad_date_rejected():
    assert requests.post(f"{AGGREGATOR_URL}/aggregate", params={"date": "2026-13-45"}, timeout=10).status_code == 422
