# ДЗ 5. Online Cinema: Event Streaming + Analytics Pipeline (Kafka + ClickHouse + PostgreSQL)

```
Movie Service ──Avro──► Kafka (2 брокера, RF=2) ──Kafka Engine──► ClickHouse (raw, MergeTree)
 (HTTP API +        ▲                                                │
  генератор)        └── Schema Registry (версии схемы)              ▼
                                                     Aggregation Service (по расписанию / HTTP)
                                                       │ агрегаты в ClickHouse (agg_*) ──► Grafana
                                                       ▼
                                                  PostgreSQL (готовые метрики) ──► S3 (SeaweedFS) daily/YYYY-MM-DD/aggregates.json
```

## Запуск

```bash
docker compose up -d --build      # всё поднимается одной командой, миграции применяются сами
make test                         # интеграционные тесты (= docker compose --profile test run --rm --build tests)
make status                       # топик, версии схемы, данные в ClickHouse/PostgreSQL/S3
make failover                     # демо: остановка брокера kafka-2 без потери событий
docker compose down -v            # остановить и удалить данные
```

| Что | Адрес |
|---|---|
| Producer API (Swagger) | http://localhost:8000/docs |
| Aggregation Service API | http://localhost:8001/docs |
| Grafana (дашборд открывается сразу, admin/admin) | http://localhost:3000 |
| S3 API (SeaweedFS) / файловый браузер | http://localhost:8333 / http://localhost:8888/buckets/movie-analytics/ |
| Schema Registry | http://localhost:8081/subjects/movie-events-value/versions |
| ClickHouse HTTP (app/app) | http://localhost:8123 |
| PostgreSQL (app/app, БД analytics) | localhost:5433 |

После старта генератор за ~1 мин заливает историю за 10 дней (≈ 10–15 тыс. событий), дальше идёт живой поток. Через ~1–2 мин сервис агрегации посчитает метрики, а Grafana их покажет.

## Структура

```
schemas/movie_event.avsc          Avro-схема события
producer/                         Movie Service: FastAPI + confluent-kafka (Avro, Schema Registry), генератор
clickhouse/init/01_events.sql     Kafka Engine -> MV -> ReplacingMergeTree (+ таблица битых сообщений)
clickhouse/init/02_aggregates.sql Таблицы материализованных агрегатов agg_*
aggregator/                       Aggregation Service: расписание, HTTP, ClickHouse -> PostgreSQL -> S3
postgres/migrations/V1__*.sql     Схема PostgreSQL (Flyway применяет автоматически)
grafana/                          Datasource + дашборд (provisioning, без ручной настройки)
tests/                            pytest: producer -> Kafka -> ClickHouse, агрегация, экспорт
scripts/                          status.sh, demo_kafka_failover.sh
```

---

## Блок 1–4

### 1. Схема события и топик

- Схема — **Avro** (`schemas/movie_event.avsc`): `event_id` (string/uuid), `user_id`, `movie_id`, `event_type` (enum из 6 значений), `timestamp` (long/timestamp-millis, UTC), `device_type` (enum), `session_id`, `progress_seconds` (`["null","int"]`: только у VIEW_*).
- **Schema Registry:** продюсер при старте регистрирует схему в subject `movie-events-value` (TopicNameStrategy), выставляет совместимость **BACKWARD** и пишет в лог `subject/id/version`. Версия видна в `GET :8000/schema` и в `GET :8081/subjects/movie-events-value/versions`. Каждое сообщение несёт schema id (формат Confluent wire), по нему ClickHouse достаёт схему.
- **Топик** `movie-events`: 3 партиции, `replication.factor=2`, `min.insync.replicas=1` — создаётся контейнером `kafka-init` (`--if-not-exists`).
- **Ключ партиционирования — `user_id`.** Обоснование:
  1. Все события пользователя попадают в одну партицию → **сохраняется порядок сессии** (STARTED → PAUSED → RESUMED → FINISHED). Это важно для потребителей, которые восстанавливают сессии и прогресс.
  2. Наши метрики считаются по пользователям (DAU, retention, когорты).
  3. `movie_id` дал бы **перекос партиций**: популярность фильмов подчиняется степенному закону, хит сериала забил бы одну партицию. Пользователей много, и активность у каждого небольшая, поэтому нагрузка распределяется ровно.

### 2. Продюсер (`producer/`)

- `POST /events` — JSON → валидация **pydantic** (типы, enum, UUID, `progress_seconds ≥ 0`, обязателен для VIEW_*, для VIEW_STARTED = 0, лишние поля запрещены) → сериализация **AvroSerializer** (вторая валидация по схеме) → Kafka. Ответ `202 {"event_id": ...}` отдаётся **только после подтверждения брокером**. Ошибка валидации → `422`, Kafka недоступна после ретраев → `503`.
- **acks=all** + `enable.idempotence=true` (ретраи не создают дублей внутри партиции).
- **Retry с exponential backoff:** внутри librdkafka (`retry.backoff.ms=100 → max 2000`) и на уровне сервиса (5 попыток, 200 → 400 → 800 → 1600 мс).
- **Логирование:** `PUBLISHED event_id=… event_type=… timestamp=… partition=… offset=…`.
- **Генератор** (`GENERATOR_ENABLED=true`): реалистичные сессии `[SEARCHED] → VIEW_STARTED(0) → (VIEW_PAUSED → VIEW_RESUMED)* → [VIEW_FINISHED(=длительность)] → [LIKED]`, `progress_seconds` растёт. Популярность фильмов — Zipf, устройства — со своими весами. Сначала **backfill за 10 дней** с затухающим retention (иначе не из чего строить когорты), затем живой поток. Backfill детерминирован (seed, `event_id = uuid5`), поэтому перезапуск не дублирует данные.

### 3. ClickHouse (`clickhouse/init/01_events.sql`)

- `movie_events_queue` — **Kafka Engine**, формат `AvroConfluent` (схема берётся из Schema Registry), `kafka_handle_error_mode='stream'`.
- `movie_events` — **ReplacingMergeTree(ingested_at)**:
  - `PARTITION BY toYYYYMM(event_date)` — немного крупных партиций, легко удалять или архивировать старые месяцы;
  - `ORDER BY (event_date, event_type, user_id, event_id)` — все запросы агрегации фильтруют по дню и типу события и считают уникальных пользователей. `event_id` в конце ключа: дубли от at-least-once доставки схлопываются, при чтении используется `FINAL`;
  - дополнительно хранятся `kafka_partition`, `kafka_offset` (трассировка) и `ingested_at`.
- `movie_events_mv` — **Materialized View** Kafka → MergeTree (`toUUID`, `toDate`). Битые сообщения не теряются молча: они идут в `movie_events_errors`.
- Колонки и типы 1:1 соответствуют Avro-схеме (enum → Enum8, timestamp-millis → DateTime64(3,'UTC'), union null → Nullable).

### 4. Интеграционные тесты (`tests/`)

`make test` — тесты собираются и запускаются в контейнере внутри docker-сети, без ручных шагов. Проверяется:

- схема зарегистрирована и у неё есть версия;
- событие через `POST /events` появляется в ClickHouse со **всеми корректными полями**;
- порядок событий одного пользователя сохраняется (одна партиция, offset по возрастанию);
- повтор того же `event_id` схлопывается;
- 6 видов невалидных событий получают `422`;
- агрегация: ручной пересчёт, идемпотентность в PostgreSQL;
- экспорт: перезапись одного и того же ключа в S3.

**Изоляция:** у каждого теста уникальный `user_id = it-…`, после теста его события удаляются (`DELETE FROM … WHERE user_id = …`). Поэтому тесты можно запускать сколько угодно раз, и метрики они не портят.

## Блок 5–7

### 5. Aggregation Service (`aggregator/`)

- Отдельный контейнер. Читает **только ClickHouse**, Kafka не трогает.
- **Расписание:** каждые `AGGREGATION_INTERVAL_SECONDS` (env, по умолчанию 60 с) пересчитываются последние `RECOMPUTE_DAYS=8` дней. Retention D1/D7 «дозревает», когда наступают следующие дни, поэтому старые даты тоже пересчитываются.
- **HTTP:** `POST /aggregate?date=YYYY-MM-DD` — ручной пересчёт, `POST /export?date=…` — ручной экспорт, `GET /metrics/{date}` — готовые метрики из PostgreSQL.
- **Логи цикла:** `AGGREGATION START/DONE date=… processed N raw events, wrote M aggregate rows … in X s` и `CYCLE DONE …`. История запусков пишется в таблицу `job_runs`.
- **Метрики** (SQL в `aggregator/app/clickhouse_metrics.py`):

| Метрика | Как считаем |
|---|---|
| DAU | `uniqExact(user_id)` за день |
| Среднее время просмотра | `avgIf(progress_seconds, event_type='VIEW_FINISHED')` |
| Топ фильмов | `countIf(VIEW_STARTED)`, `uniqExactIf`, ранг — оконная `row_number() OVER (ORDER BY views DESC)` |
| Конверсия | `countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)` |
| Retention D1/D7 и когорты D0…D7 | когорта = пользователи, у которых `min(event_date)` по VIEW_STARTED = d. Для N = 0..7 (`arrayJoin(range(8))`) считаем `uniqExact` пользователей когорты с любым событием в день d+N |
| Доп.: устройства | `uniqExact(user_id)`, `count()` по `device_type` |

- **Материализация в ClickHouse:** `agg_daily_metrics` (длинный формат: дата, метрика, значение), `agg_top_movies`, `agg_retention_cohort`, `agg_device_daily` — ReplacingMergeTree. Пересчёт за дату = `DELETE` за дату + `INSERT … SELECT`, поэтому повторный запуск не плодит строк.
- **PostgreSQL** (`postgres/migrations/V1__aggregates.sql`, Flyway):
  - таблицы `daily_metrics(metric_date, metric_name, value, computed_at)` с PK (date, name), `top_movies`, `retention_cohort`, `device_daily`;
  - запись — **UPSERT `ON CONFLICT DO UPDATE` в одной транзакции**, поэтому пересчёт обновляет строки, а не дублирует их;
  - ошибки записи → 3 попытки с экспоненциальной паузой 0,5 / 1 / 2 с, логирование, запись в `job_runs`.

## Блок 8–10

### 6. Grafana

Datasource ClickHouse (официальный плагин) и дашборд **«Online Cinema — Analytics»** подключаются через provisioning и открываются как домашняя страница. Все панели читают **только `agg_*`**, raw-события не используются:

- **Retention Cohort Heatmap** — таблица: строки — когорты по дню первого просмотра, столбцы — Day 0…Day 7, в ячейке % вернувшихся пользователей, цветовая шкала от красного к зелёному. Будущие дни пустые;
- DAU, конверсия просмотра, топ-10 фильмов, распределение по устройствам, среднее время просмотра, retention D1/D7 по когортам.

### 7. Экспорт в S3 (SeaweedFS)

S3-совместимое хранилище — **SeaweedFS** (аналог MinIO: официальные образы `minio/minio` удалены с Docker Hub). Работа идёт через стандартный S3 API (boto3), поэтому для кода нет разницы, MinIO это или AWS S3.

- **Формат:** `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`. Внутри все метрики дня (DAU, конверсия, среднее время, retention D1/D7, события), топ фильмов, когорта D0–D7 и устройства, у каждой записи — `computed_at`, у файла — `exported_at`.
- **По расписанию:** каждые `EXPORT_INTERVAL_SECONDS` (300 с) выгружается **вчерашний** день. Вручную — `POST :8001/export?date=…`.
- **Перезапись:** `put_object` в тот же ключ, дубликатов нет (проверяется тестом). Бакет создаётся автоматически.
- **Ошибки:** если PostgreSQL или S3 недоступен, ошибка логируется и пишется в `job_runs`, дата попадает в `pending_exports` и повторяется при следующем запуске.

### 8. Отказоустойчивость Kafka

- KRaft: `kafka-controller` + **2 брокера** (`kafka-1`, `kafka-2`). Топик с `replication.factor=2`, `min.insync.replicas=1`, служебные топики (`__consumer_offsets`, `_schemas`) тоже с RF=2.
- **Schema Registry** поднят. **Health checks есть у всех сервисов**, запуск идёт по `depends_on: service_healthy`.
- `make failover` останавливает `kafka-2`: лидеры партиций переезжают на `kafka-1`, продюсер (acks=all при ISR=1) продолжает отвечать 202, события доходят до ClickHouse. После `start` брокер догоняет реплики и возвращается в ISR.
- **Компромисс:** `min.insync.replicas=1` при двух брокерах сохраняет запись при отказе одного брокера, но в этот момент данные живут в одной копии. В продакшене нужно 3 брокера, RF=3, min.insync=2 и 3 контроллера: здесь контроллер один, это упрощение стенда.
