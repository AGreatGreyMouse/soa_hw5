import os
import uuid

import clickhouse_connect
import pytest

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8001")


@pytest.fixture(scope="session")
def ch():
    client = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "localhost"), port=8123,
                                           username=os.getenv("CLICKHOUSE_USER", "app"),
                                           password=os.getenv("CLICKHOUSE_PASSWORD", "app"), database="cinema")
    yield client
    client.close()


@pytest.fixture
def test_user(ch):
    """Изоляция: у каждого теста свой user_id с префиксом it-; после теста его события удаляются,
    поэтому тест можно гонять сколько угодно раз и он не портит метрики."""
    user_id = f"it-{uuid.uuid4().hex[:12]}"
    yield user_id
    ch.command("DELETE FROM cinema.movie_events WHERE user_id = {u:String}", parameters={"u": user_id})
