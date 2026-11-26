"""Pydantic-модель входящего события = зеркало Avro-схемы (schemas/movie_event.avsc)."""
import uuid
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = {EventType.VIEW_STARTED, EventType.VIEW_FINISHED, EventType.VIEW_PAUSED, EventType.VIEW_RESUMED}


class MovieEventIn(BaseModel):
    model_config = {"extra": "forbid"}

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    user_id: str = Field(min_length=1, max_length=64)
    movie_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=64)
    progress_seconds: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def to_utc(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def progress_rules(self):
        if self.event_type in VIEW_EVENTS and self.progress_seconds is None:
            raise ValueError("progress_seconds обязателен для событий просмотра (VIEW_*)")
        if self.event_type == EventType.VIEW_STARTED and self.progress_seconds != 0:
            raise ValueError("для VIEW_STARTED progress_seconds должен быть 0")
        return self

    def to_avro_dict(self) -> dict:
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }
