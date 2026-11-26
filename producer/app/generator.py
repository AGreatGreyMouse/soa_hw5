"""Генератор синтетических событий.

1) Backfill: история за GENERATOR_BACKFILL_DAYS дней (нужна для retention и когорт).
   Детерминирован (seed) и event_id = uuid5(...) -> повторный запуск даёт те же event_id,
   а ReplacingMergeTree в ClickHouse схлопнет дубли.
2) Live: поток сессий «сейчас» с реальными паузами между событиями.

Сессия всегда реалистична: [SEARCHED] -> VIEW_STARTED(0) -> (VIEW_PAUSED -> VIEW_RESUMED)* ->
[VIEW_FINISHED(=длительность)] -> [LIKED]; progress_seconds не убывает внутри сессии.
"""
import logging
import math
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from app import config
from app.models import MovieEventIn

log = logging.getLogger("generator")
NS = uuid.UUID("6f1c2a52-0000-4000-8000-000000000000")
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
DEVICE_WEIGHTS = [0.45, 0.25, 0.2, 0.1]


class Generator:
    def __init__(self, publisher):
        self.publisher = publisher
        self.rnd = random.Random(config.GENERATOR_SEED)
        self.movies = {f"movie-{i:03d}": self.rnd.randint(20, 150) * 60 for i in range(1, 61)}
        self.movie_ids = list(self.movies)
        # Zipf-подобная популярность фильмов
        self.movie_weights = [1 / (i ** 0.9) for i in range(1, len(self.movie_ids) + 1)]
        self.users: dict[str, dict] = {}     # user_id -> {"first_day": date, "device": str}
        self.counter = 0
        self.stop = threading.Event()

    # ------------------------------------------------------------------ сессия
    def _session(self, user_id: str, start: datetime, deterministic: bool) -> list[dict]:
        rnd = self.rnd
        device = self.users[user_id]["device"] if rnd.random() < 0.8 else rnd.choice(DEVICES)
        movie_id = rnd.choices(self.movie_ids, self.movie_weights)[0]
        duration = self.movies[movie_id]
        self.counter += 1
        session_id = f"s-{uuid.uuid5(NS, f'{user_id}-{self.counter}') if deterministic else uuid.uuid4()}"
        t, progress = start, 0
        events = []

        def add(event_type, progress_seconds=None):
            idx = len(events)
            eid = uuid.uuid5(NS, f"{session_id}-{idx}") if deterministic else uuid.uuid4()
            ev = MovieEventIn(event_id=eid, user_id=user_id, movie_id=movie_id, event_type=event_type,
                              timestamp=t, device_type=device, session_id=session_id,
                              progress_seconds=progress_seconds)
            events.append(ev.to_avro_dict())

        if rnd.random() < 0.4:
            add("SEARCHED")
            t += timedelta(seconds=rnd.randint(5, 60))
        add("VIEW_STARTED", 0)
        finish = rnd.random() < 0.65
        target = duration if finish else int(duration * rnd.uniform(0.05, 0.9))
        for _ in range(rnd.choice([0, 0, 1, 1, 2])):          # паузы
            progress = min(target, progress + int(target * rnd.uniform(0.1, 0.4)))
            t += timedelta(seconds=progress - (events[-1]["progress_seconds"] or 0) + 1)
            add("VIEW_PAUSED", progress)
            t += timedelta(seconds=rnd.randint(10, 600))
            add("VIEW_RESUMED", progress)
        if finish:
            t += timedelta(seconds=duration - progress + 1)
            add("VIEW_FINISHED", duration)
            if rnd.random() < 0.35:
                t += timedelta(seconds=rnd.randint(1, 30))
                add("LIKED")
        return events

    def _new_user(self, day) -> str:
        uid = f"user-{len(self.users) + 1:05d}"
        self.users[uid] = {"first_day": day, "device": self.rnd.choices(DEVICES, DEVICE_WEIGHTS)[0]}
        return uid

    # ------------------------------------------------------------------ backfill
    def backfill(self):
        now = datetime.now(timezone.utc)
        today = now.date()
        total = 0
        started = time.monotonic()
        for back in range(config.GENERATOR_BACKFILL_DAYS, -1, -1):
            day = today - timedelta(days=back)
            day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
            for _ in range(config.GENERATOR_NEW_USERS_PER_DAY):
                self._new_user(day)
            for uid, info in list(self.users.items()):
                age = (day - info["first_day"]).days
                p = 1.0 if age == 0 else 0.12 + 0.45 * math.exp(-age / 3)   # затухающий retention
                if self.rnd.random() > p:
                    continue
                for _ in range(self.rnd.choice([1, 1, 2, 3])):
                    start = day_start + timedelta(seconds=self.rnd.randint(8 * 3600, 22 * 3600))
                    events = [e for e in self._session(uid, start, deterministic=True) if e["timestamp"] < now]
                    for e in events:
                        self.publisher.publish_async(e)
                    total += len(events)
        self.publisher.flush(60)
        log.info("BACKFILL done: %s events, %s users, %s days, %.1fs",
                 total, len(self.users), config.GENERATOR_BACKFILL_DAYS + 1, time.monotonic() - started)

    # ------------------------------------------------------------------ live
    def live(self):
        log.info("LIVE generator started: %.2f sessions/s", config.GENERATOR_SESSIONS_PER_SEC)
        live_rnd = random.Random()
        while not self.stop.is_set():
            today = datetime.now(timezone.utc).date()
            uid = (self._new_user(today) if live_rnd.random() < 0.2 or not self.users
                   else live_rnd.choice(list(self.users)))
            # сессия «сжата» по времени: события идут с реальными паузами в 0.2–1 с
            events = self._session(uid, datetime.now(timezone.utc), deterministic=False)
            base = events[0]["timestamp"]
            for e in events:
                if self.stop.is_set():
                    return
                e["timestamp"] = datetime.now(timezone.utc) if e is not events[0] else base
                self.publisher.publish_async(e)
                time.sleep(live_rnd.uniform(0.2, 1.0))
            self.stop.wait(1 / config.GENERATOR_SESSIONS_PER_SEC)

    def run(self):
        try:
            self.backfill()
            self.live()
        except Exception:
            log.exception("generator crashed")
