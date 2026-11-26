"""Интеграционные тесты: HTTP producer -> Kafka -> ClickHouse (Kafka Engine + MV -> MergeTree)."""
import time
import uuid
from datetime import datetime, timezone

import requests

from conftest import PRODUCER_URL


def wait_rows(ch, sql, params, expected, timeout=60):
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        rows = ch.query(sql, parameters=params).result_rows
        if len(rows) >= expected:
            return rows
        time.sleep(1)
    return rows


def make_event(user_id, **over):
    ev = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "movie_id": "movie-it-001",
        "event_type": "VIEW_STARTED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": "TV",
        "session_id": f"s-{uuid.uuid4().hex[:8]}",
        "progress_seconds": 0,
    }
    ev.update(over)
    return ev


def test_schema_registered():
    r = requests.get(f"{PRODUCER_URL}/schema", timeout=5)
    assert r.status_code == 200
    body = r.json()
    assert body["subject"] == "movie-events-value" and body["version"] >= 1


def test_event_reaches_clickhouse_with_correct_fields(ch, test_user):
    ev = make_event(test_user, progress_seconds=0)
    r = requests.post(f"{PRODUCER_URL}/events", json=ev, timeout=20)
    assert r.status_code == 202, r.text
    assert r.json()["event_id"] == ev["event_id"]

    rows = wait_rows(ch, "SELECT toString(event_id), user_id, movie_id, toString(event_type), "
                         "toString(device_type), session_id, progress_seconds, toUnixTimestamp64Milli(`timestamp`) "
                         "FROM cinema.movie_events WHERE user_id = {u:String}", {"u": test_user}, 1)
    assert len(rows) == 1, "событие не появилось в ClickHouse за 60 с"
    event_id, user_id, movie_id, etype, device, session, progress, ts_ms = rows[0]
    assert event_id == ev["event_id"]
    assert (user_id, movie_id, etype, device, session, progress) == \
           (test_user, "movie-it-001", "VIEW_STARTED", "TV", ev["session_id"], 0)
    sent_ms = int(datetime.fromisoformat(ev["timestamp"]).timestamp() * 1000)
    assert abs(ts_ms - sent_ms) <= 1                # Avro timestamp-millis


def test_session_order_preserved_by_user_key(ch, test_user):
    """Ключ партиционирования = user_id: события одного пользователя в одной партиции и по порядку."""
    session = f"s-{uuid.uuid4().hex[:8]}"
    seq = [("VIEW_STARTED", 0), ("VIEW_PAUSED", 300), ("VIEW_RESUMED", 300), ("VIEW_FINISHED", 1200), ("LIKED", None)]
    for etype, progress in seq:
        r = requests.post(f"{PRODUCER_URL}/events", timeout=20,
                          json=make_event(test_user, event_type=etype, progress_seconds=progress, session_id=session))
        assert r.status_code == 202, r.text
    rows = wait_rows(ch, "SELECT toString(event_type), kafka_partition, kafka_offset FROM cinema.movie_events "
                         "WHERE user_id = {u:String} ORDER BY kafka_offset", {"u": test_user}, len(seq))
    assert [r[0] for r in rows] == [e for e, _ in seq]
    assert len({r[1] for r in rows}) == 1          # одна партиция


def test_duplicate_event_id_is_collapsed(ch, test_user):
    ev = make_event(test_user)
    for _ in range(2):
        assert requests.post(f"{PRODUCER_URL}/events", json=ev, timeout=20).status_code == 202
    wait_rows(ch, "SELECT 1 FROM cinema.movie_events WHERE user_id = {u:String}", {"u": test_user}, 2, timeout=20)
    rows = ch.query("SELECT count() FROM cinema.movie_events FINAL WHERE user_id = {u:String}",
                    parameters={"u": test_user}).result_rows
    assert rows[0][0] == 1


def test_validation_errors():
    bad = [
        make_event("u-x", event_type="WATCHED"),                       # не из enum
        make_event("u-x", device_type="FRIDGE"),
        make_event("u-x", event_type="VIEW_FINISHED", progress_seconds=None),  # нет прогресса у VIEW_*
        make_event("u-x", progress_seconds=-5),
        {k: v for k, v in make_event("u-x").items() if k != "user_id"},  # нет обязательного поля
        make_event("u-x", event_id="not-a-uuid"),
    ]
    for ev in bad:
        r = requests.post(f"{PRODUCER_URL}/events", json=ev, timeout=10)
        assert r.status_code == 422, (ev, r.status_code, r.text)
