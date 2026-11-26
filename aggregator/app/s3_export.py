"""Экспорт агрегатов дня из PostgreSQL в S3 (MinIO): s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json."""
import datetime as dt
import json
import logging

import boto3
from botocore.config import Config

from app import config, pg_store

log = logging.getLogger("export")


def _s3():
    return boto3.client("s3", endpoint_url=config.S3_ENDPOINT, aws_access_key_id=config.S3_ACCESS_KEY,
                        aws_secret_access_key=config.S3_SECRET_KEY, region_name="us-east-1",
                        config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2},
                                      s3={"addressing_style": "path"}))


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=config.S3_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=config.S3_BUCKET)
        log.info("bucket %s created", config.S3_BUCKET)


def build_document(day: dt.date, rows: dict) -> dict:
    iso = lambda t: t.isoformat()  # noqa: E731
    return {
        "date": day.isoformat(),
        "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "metrics": {name: {"value": value, "computed_at": iso(ts)} for name, value, ts in rows["daily"]},
        "top_movies": [{"rank": r, "movie_id": m, "views": v, "unique_viewers": u, "computed_at": iso(ts)}
                       for r, m, v, u, ts in rows["top"]],
        "retention_cohort": [{"day_number": n, "cohort_size": s, "retained_users": k, "retention_pct": p,
                              "computed_at": iso(ts)} for n, s, k, p, ts in rows["cohort"]],
        "devices": [{"device_type": d, "users": u, "events": e, "computed_at": iso(ts)}
                    for d, u, e, ts in rows["devices"]],
    }


def export_day(day: dt.date) -> str:
    rows = pg_store.read_day(day)
    if not rows["daily"]:
        raise ValueError(f"no aggregates in PostgreSQL for {day} (run aggregation first)")
    body = json.dumps(build_document(day, rows), ensure_ascii=False, indent=2).encode("utf-8")
    key = f"daily/{day.isoformat()}/aggregates.json"
    s3 = _s3()
    ensure_bucket(s3)
    s3.put_object(Bucket=config.S3_BUCKET, Key=key, Body=body, ContentType="application/json")  # перезапись
    return f"s3://{config.S3_BUCKET}/{key}"
