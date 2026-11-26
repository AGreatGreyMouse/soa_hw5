"""Циклы агрегации и экспорта (используются и планировщиком, и HTTP-эндпоинтами)."""
import datetime as dt
import logging
import threading
import time

from app import clickhouse_metrics, config, pg_store, s3_export

log = logging.getLogger("aggregator")
_lock = threading.Lock()                 # один пересчёт за раз (расписание и ручной вызов не пересекаются)
pending_exports: set[dt.date] = set()    # даты, экспорт которых упал — повторим при следующем запуске


def today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def aggregate_day(day: dt.date) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    t0 = time.monotonic()
    log.info("AGGREGATION START date=%s", day)
    try:
        with _lock:
            data = clickhouse_metrics.compute(day)
            written = pg_store.save(day, data)
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        log.error("AGGREGATION FAILED date=%s after %d ms: %s", day, ms, exc)
        pg_store.log_run("aggregate", day, "failed", None, ms, str(exc), started_at)
        raise
    ms = int((time.monotonic() - t0) * 1000)
    metrics = {name: value for _, name, value, _ in data["daily"]}
    log.info("AGGREGATION DONE date=%s: processed %s raw events, wrote %s aggregate rows to PostgreSQL in %.2f s",
             day, data["raw_events"], written, ms / 1000)
    pg_store.log_run("aggregate", day, "success", written, ms, None, started_at)
    return {"date": day.isoformat(), "raw_events": data["raw_events"], "rows_written": written,
            "duration_ms": ms, "metrics": metrics, "top_movies": len(data["top"]),
            "cohort_days": len(data["cohort"])}


def export_day(day: dt.date) -> str:
    started_at = dt.datetime.now(dt.timezone.utc)
    t0 = time.monotonic()
    try:
        uri = s3_export.export_day(day)
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1000)
        pending_exports.add(day)
        log.error("EXPORT FAILED date=%s: %s — will retry on next run", day, exc)
        pg_store.log_run("export", day, "failed", None, ms, str(exc), started_at)
        raise
    ms = int((time.monotonic() - t0) * 1000)
    pending_exports.discard(day)
    log.info("EXPORT DONE date=%s -> %s in %d ms", day, uri, ms)
    pg_store.log_run("export", day, "success", 1, ms, None, started_at)
    return uri


def aggregation_cycle():
    """Плановый цикл: пересчёт последних RECOMPUTE_DAYS дней (retention D1/D7 «дозревает»)."""
    t0 = time.monotonic()
    days = [today() - dt.timedelta(days=i) for i in range(config.RECOMPUTE_DAYS - 1, -1, -1)]
    total_raw, ok = 0, 0
    for day in days:
        try:
            total_raw += aggregate_day(day)["raw_events"]
            ok += 1
        except Exception:
            pass   # уже залогировано; следующий цикл повторит
    log.info("CYCLE DONE: %s/%s days, processed %s raw events in %.2f s",
             ok, len(days), total_raw, time.monotonic() - t0)


def export_cycle():
    """Плановый экспорт за прошедший день + повтор упавших ранее дат."""
    for day in sorted({today() - dt.timedelta(days=1)} | pending_exports):
        try:
            export_day(day)
        except Exception:
            pass


class Scheduler:
    def __init__(self):
        self.stop = threading.Event()

    def _loop(self, name, interval, fn, initial_delay):
        if self.stop.wait(initial_delay):
            return
        while not self.stop.is_set():
            try:
                fn()
            except Exception:
                log.exception("%s job crashed", name)
            self.stop.wait(interval)

    def start(self):
        log.info("Scheduler: aggregation every %ss (last %s days), export every %ss",
                 config.AGGREGATION_INTERVAL_SECONDS, config.RECOMPUTE_DAYS, config.EXPORT_INTERVAL_SECONDS)
        threading.Thread(target=self._loop, daemon=True, args=(
            "aggregation", config.AGGREGATION_INTERVAL_SECONDS, aggregation_cycle, 15)).start()
        threading.Thread(target=self._loop, daemon=True, args=(
            "export", config.EXPORT_INTERVAL_SECONDS, export_cycle, 60)).start()
