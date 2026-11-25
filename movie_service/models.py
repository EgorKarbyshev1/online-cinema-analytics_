from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    event_id: UUID
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int = Field(strict=True, ge=0, le=2147483647)

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_datetime(cls, value):
        if not isinstance(value, (datetime, str)) or isinstance(value, str) and "T" not in value.upper():
            raise ValueError("timestamp must be an ISO 8601 datetime")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value):
        if value.tzinfo is None:
            raise ValueError("timestamp must include a UTC offset")
        try:
            value = value.astimezone(timezone.utc)
        except OverflowError as error:
            raise ValueError("timestamp must be within ClickHouse DateTime64 range") from error
        if not 1900 <= value.year < 2300:
            raise ValueError("timestamp must be within ClickHouse DateTime64 range")
        return value
