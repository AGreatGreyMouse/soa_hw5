"""Movie Service (Producer): HTTP API приёма событий + генератор синтетического потока."""
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app import config
from app.generator import Generator
from app.models import MovieEventIn
from app.publisher import EventPublisher, PublishError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("producer")
state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = await run_in_threadpool(EventPublisher)
    state["publisher"] = publisher
    if config.GENERATOR_ENABLED:
        gen = Generator(publisher)
        state["generator"] = gen
        threading.Thread(target=gen.run, daemon=True, name="generator").start()
    yield
    if "generator" in state:
        state["generator"].stop.set()
    publisher.close()


app = FastAPI(title="Movie Service (Producer)", version="1.0.0", lifespan=lifespan)


@app.post("/events", status_code=202)
async def publish_event(event: MovieEventIn):
    """Принимает JSON-событие, валидирует (pydantic + Avro-схема) и публикует в Kafka.
    Ответ отдаётся только после подтверждения записи брокерами (acks=all)."""
    try:
        await run_in_threadpool(state["publisher"].publish, event.to_avro_dict())
    except PublishError as exc:
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")
    except (ValueError, TypeError) as exc:      # ошибка сериализации по Avro-схеме
        raise HTTPException(status_code=422, detail=str(exc))
    return {"event_id": str(event.event_id), "status": "published"}


@app.get("/schema")
def schema_info():
    p = state["publisher"]
    return {"subject": config.SUBJECT, "schema_id": p.schema_id, "version": p.schema_version}


@app.get("/health")
def health():
    if "publisher" not in state:
        return JSONResponse({"status": "starting"}, status_code=503)
    return {"status": "ok"}
