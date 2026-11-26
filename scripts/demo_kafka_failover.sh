#!/bin/sh
# Демо: один из двух брокеров падает — события продолжают приниматься и доходить до ClickHouse
cd "$(dirname "$0")/.."
CH="docker compose exec -T clickhouse clickhouse-client --user app --password app --query"
describe() { docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic movie-events | grep Partition: ; }

echo "До отказа:"; describe
echo; echo "Останавливаем kafka-2 ..."; docker compose stop kafka-2 >/dev/null; sleep 5
echo "После отказа (лидеры переехали на kafka-1 (id=2), ISR сократился до 1):"; describe

U="failover-$(date +%s)"
echo; echo "Отправляем 5 событий пользователя $U через API продюсера:"
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "  POST /events -> %{http_code}\n" -X POST http://localhost:8000/events \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":\"$U\",\"movie_id\":\"movie-001\",\"event_type\":\"LIKED\",\"device_type\":\"MOBILE\",\"session_id\":\"s-$i\"}"
done
sleep 5
echo "В ClickHouse событий пользователя $U: $($CH "SELECT count() FROM cinema.movie_events WHERE user_id = '$U'")"

echo; echo "Возвращаем kafka-2 ..."; docker compose start kafka-2 >/dev/null; sleep 15
echo "После восстановления (kafka-2 догнал реплики и вернулся в ISR):"; describe
$CH "DELETE FROM cinema.movie_events WHERE user_id = '$U'"
