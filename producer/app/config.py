import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
SUBJECT = f"{TOPIC}-value"            # TopicNameStrategy

PUBLISH_MAX_ATTEMPTS = int(os.getenv("PUBLISH_MAX_ATTEMPTS", "5"))
PUBLISH_BASE_DELAY_MS = int(os.getenv("PUBLISH_BASE_DELAY_MS", "200"))
DELIVERY_TIMEOUT_S = float(os.getenv("DELIVERY_TIMEOUT_S", "10"))

GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GENERATOR_SEED = int(os.getenv("GENERATOR_SEED", "42"))
GENERATOR_BACKFILL_DAYS = int(os.getenv("GENERATOR_BACKFILL_DAYS", "10"))
GENERATOR_NEW_USERS_PER_DAY = int(os.getenv("GENERATOR_NEW_USERS_PER_DAY", "40"))
GENERATOR_SESSIONS_PER_SEC = float(os.getenv("GENERATOR_SESSIONS_PER_SEC", "1"))
