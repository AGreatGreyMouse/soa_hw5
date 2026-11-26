"""Aggregation Service: ClickHouse (raw) -> бизнес-метрики -> PostgreSQL -> S3."""
import datetime as dt
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app import pg_store, service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
scheduler = service.Scheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.stop.set()


app = FastAPI(title="Aggregation Service", version="1.0.0", lifespan=lifespan)


def _parse(date: str | None) -> dt.date:
    if date is None:
        return service.today()
    try:
        return dt.date.fromisoformat(date)
    except ValueError:
        raise HTTPException(422, "date must be YYYY-MM-DD")


@app.post("/aggregate")
async def aggregate(date: str | None = Query(None, description="YYYY-MM-DD, по умолчанию сегодня (UTC)")):
    """Ручной пересчёт агрегатов за дату (ClickHouse -> PostgreSQL)."""
    try:
        return await run_in_threadpool(service.aggregate_day, _parse(date))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, f"aggregation failed: {exc}")


@app.post("/export")
async def export(date: str | None = Query(None, description="YYYY-MM-DD, по умолчанию вчера (UTC)")):
    """Ручной экспорт агрегатов за дату из PostgreSQL в S3 (перезаписывает файл)."""
    day = _parse(date) if date else service.today() - dt.timedelta(days=1)
    try:
        return {"date": day.isoformat(), "uri": await run_in_threadpool(service.export_day, day)}
    except Exception as exc:
        raise HTTPException(503, f"export failed: {exc}")


@app.get("/metrics/{date}")
def metrics(date: str):
    """Готовые метрики за дату из PostgreSQL."""
    rows = pg_store.read_day(_parse(date))
    return {"date": date,
            "metrics": {n: v for n, v, _ in rows["daily"]},
            "top_movies": [{"rank": r, "movie_id": m, "views": v} for r, m, v, _, _ in rows["top"]],
            "retention_cohort": {f"day_{n}": p for n, _, _, p, _ in rows["cohort"]}}


@app.get("/health")
def health():
    return {"status": "ok"}
