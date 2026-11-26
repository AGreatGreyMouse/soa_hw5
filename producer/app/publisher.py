"""Kafka-продюсер: Avro + Schema Registry, acks=all, idempotence, retry с exponential backoff."""
import logging
import pathlib
import threading
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from app import config

log = logging.getLogger("publisher")

SCHEMA_STR = (pathlib.Path(__file__).parent / "movie_event.avsc").read_text(encoding="utf-8")


class PublishError(Exception):
    pass


class EventPublisher:
    def __init__(self):
        self.sr = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
        self.schema_id, self.schema_version = self._register_schema()
        self.value_serializer = AvroSerializer(
            self.sr, SCHEMA_STR, conf={"auto.register.schemas": False, "use.latest.version": True})
        self.key_serializer = StringSerializer("utf_8")
        self.producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "acks": "all",                      # ждём подтверждения от всех in-sync реплик
            "enable.idempotence": True,         # ретраи librdkafka не создают дублей в партиции
            "retries": 10,
            "retry.backoff.ms": 100,            # внутренний backoff librdkafka (экспоненциальный до max)
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": int(config.DELIVERY_TIMEOUT_S * 1000),
            "linger.ms": 5,
            "compression.type": "lz4",
        })
        self._poller_stop = threading.Event()
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _register_schema(self):
        """Регистрируем схему (идемпотентно) и ставим совместимость BACKWARD для эволюции."""
        for attempt in range(1, 31):
            try:
                schema_id = self.sr.register_schema(config.SUBJECT, Schema(SCHEMA_STR, "AVRO"))
                try:
                    self.sr.set_compatibility(subject_name=config.SUBJECT, level="BACKWARD")
                except Exception as exc:  # не критично
                    log.warning("set_compatibility failed: %s", exc)
                version = self.sr.get_latest_version(config.SUBJECT).version
                log.info("Schema registered: subject=%s id=%s version=%s compatibility=BACKWARD",
                         config.SUBJECT, schema_id, version)
                return schema_id, version
            except Exception as exc:
                log.warning("Schema Registry not ready (attempt %s): %s", attempt, exc)
                time.sleep(2)
        raise RuntimeError("Schema Registry unavailable")

    def _poll_loop(self):
        while not self._poller_stop.is_set():
            self.producer.poll(0.1)

    def publish(self, event: dict) -> None:
        """Синхронная публикация с подтверждением. Ключ = user_id. Бросает PublishError."""
        topic = config.TOPIC
        value = self.value_serializer(event, SerializationContext(topic, MessageField.VALUE))  # валидация по схеме
        key = self.key_serializer(event["user_id"], SerializationContext(topic, MessageField.KEY))

        last_error = None
        for attempt in range(1, config.PUBLISH_MAX_ATTEMPTS + 1):
            done = threading.Event()
            result = {}

            def on_delivery(err, msg, result=result, done=done):
                result["err"], result["msg"] = err, msg
                done.set()

            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                if not done.wait(config.DELIVERY_TIMEOUT_S + 2):
                    raise KafkaException("delivery report timeout")
                if result["err"] is not None:
                    raise KafkaException(result["err"])
                msg = result["msg"]
                log.info("PUBLISHED event_id=%s event_type=%s timestamp=%s partition=%s offset=%s",
                         event["event_id"], event["event_type"], event["timestamp"].isoformat(),
                         msg.partition(), msg.offset())
                return
            except (KafkaException, BufferError) as exc:
                last_error = exc
                if attempt == config.PUBLISH_MAX_ATTEMPTS:
                    break
                delay = config.PUBLISH_BASE_DELAY_MS / 1000 * 2 ** (attempt - 1)
                log.warning("publish failed event_id=%s (attempt %s/%s): %s; retry in %.0f ms",
                            event["event_id"], attempt, config.PUBLISH_MAX_ATTEMPTS, exc, delay * 1000)
                time.sleep(delay)
        log.error("publish FAILED event_id=%s after %s attempts: %s",
                  event["event_id"], config.PUBLISH_MAX_ATTEMPTS, last_error)
        raise PublishError(str(last_error))

    def publish_async(self, event: dict) -> None:
        """Для генератора: без ожидания каждого ack (throughput). Ретраи делает librdkafka
        (idempotence + exponential backoff), результат логируется в delivery callback."""
        topic = config.TOPIC
        value = self.value_serializer(event, SerializationContext(topic, MessageField.VALUE))
        key = self.key_serializer(event["user_id"], SerializationContext(topic, MessageField.KEY))

        def on_delivery(err, msg):
            if err is not None:
                log.error("publish FAILED event_id=%s: %s", event["event_id"], err)
            else:
                log.info("PUBLISHED event_id=%s event_type=%s timestamp=%s partition=%s offset=%s",
                         event["event_id"], event["event_type"], event["timestamp"].isoformat(),
                         msg.partition(), msg.offset())

        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:          # локальная очередь переполнена — ждём и повторяем
                self.producer.poll(0.5)

    def flush(self, timeout: float = 30):
        return self.producer.flush(timeout)

    def close(self):
        self.producer.flush(10)
        self._poller_stop.set()
