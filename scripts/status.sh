#!/bin/sh
# Сводка состояния pipeline
cd "$(dirname "$0")/.."
echo "=== Состояние контейнеров (health) ==="
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
echo; echo "=== Топик movie-events (партиции, RF, ISR, min.insync.replicas) ==="
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic movie-events
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --describe --entity-type topics --entity-name movie-events
echo; echo "=== Schema Registry: версии схемы movie-events-value ==="
curl -s http://localhost:8081/subjects/movie-events-value/versions; echo
curl -s http://localhost:8081/subjects/movie-events-value/versions/latest | python3 -c 'import json,sys; d=json.load(sys.stdin); print("version", d["version"], "id", d["id"])'
echo; echo "=== ClickHouse: raw-события по дням ==="
docker compose exec -T clickhouse clickhouse-client --user app --password app --query \
 "SELECT event_date, count() AS events, uniqExact(user_id) AS users FROM cinema.movie_events GROUP BY event_date ORDER BY event_date FORMAT PrettyCompact"
echo; echo "=== PostgreSQL: метрики за вчера ==="
docker compose exec -T postgres psql -U app -d analytics -c \
 "SELECT metric_date, metric_name, round(value::numeric, 4) AS value, computed_at FROM daily_metrics WHERE metric_date = current_date - 1 ORDER BY metric_name"
echo; echo "=== S3 (SeaweedFS): экспортированные файлы ==="
docker compose exec -T aggregation-service python -c "
from app.s3_export import _s3
for o in _s3().list_objects_v2(Bucket='movie-analytics').get('Contents', []):
    print(o['LastModified'], o['Size'], 's3://movie-analytics/' + o['Key'])
"
