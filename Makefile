.PHONY: up down test logs status failover clean

up:            ## поднять всё
	docker compose up -d --build

test:          ## интеграционные тесты (одна команда, без ручных шагов)
	docker compose --profile test run --rm --build tests

status:        ## топик, схема, данные в ClickHouse / PostgreSQL / S3
	./scripts/status.sh

failover:      ## демо отказоустойчивости Kafka: останавливаем брокер kafka-2
	./scripts/demo_kafka_failover.sh

logs:
	docker compose logs -f movie-service aggregation-service

down:
	docker compose down -v
