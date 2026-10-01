from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Annotated, Literal, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator
from sqlalchemy import JSON, DateTime, Float, Index, String, UniqueConstraint, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


# ---------- 1. Validation layer (what the consumer decodes) ----------
class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TemperatureRange(_Base):
    upper_threshold: float
    lower_threshold: float


class ExperimentConfigData(_Base):
    experiment: UUID
    researcher: str
    sensors: list[UUID]
    temperature_range: TemperatureRange


class ExperimentTimestamped(_Base):
    experiment: UUID
    timestamp: datetime  # float epoch seconds -> tz-aware datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def _epoch_to_dt(cls, v):
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        return v


class SensorTemperatureData(ExperimentTimestamped):
    sensor: UUID
    measurement_id: UUID
    temperature: float
    measurement_hash: str


class ExperimentConfigEvent(_Base):
    name: Literal["ExperimentConfig"]
    data: ExperimentConfigData


class ExperimentStartedEvent(_Base):
    name: Literal["experiment_started"]
    data: ExperimentTimestamped


class StabilizationStartedEvent(_Base):
    name: Literal["stabilization_started"]
    data: ExperimentTimestamped


class SensorTemperatureMeasuredEvent(_Base):
    name: Literal["sensor_temperature_measured"]
    data: SensorTemperatureData


Event = Annotated[
    Union[
        ExperimentConfigEvent,
        ExperimentStartedEvent,
        StabilizationStartedEvent,
        SensorTemperatureMeasuredEvent,
    ],
    Field(discriminator="name"),
]
event_adapter = TypeAdapter(Event)


# ---------- 2. Database model ----------
class Base(DeclarativeBase):
    pass


class EventRow(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_name: Mapped[str] = mapped_column(String(64), index=True)
    experiment_id: Mapped[str] = mapped_column(String(36), index=True)
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    sensor_id: Mapped[str | None] = mapped_column(String(36))
    measurement_id: Mapped[str | None] = mapped_column(String(36))
    temperature: Mapped[float | None] = mapped_column(Float)
    measurement_hash: Mapped[str | None] = mapped_column(String(512))
    payload: Mapped[dict] = mapped_column(JSON)
    # Deterministic identity so replayed Kafka messages are ignored.
    event_key: Mapped[str] = mapped_column(String(200))

    __table_args__ = (
        UniqueConstraint("event_key", name="uq_events_event_key"),
        Index("ix_events_exp_name_ts", "experiment_id", "event_name", "timestamp"),
    )


def to_row(ev) -> EventRow:
    d = ev.data
    ts = getattr(d, "timestamp", None)
    sensor = getattr(d, "sensor", None)
    mid = getattr(d, "measurement_id", None)
    key = "|".join([ev.name, str(d.experiment), str(mid or ""), str(sensor or ""),
                    ts.isoformat() if ts else ""])
    return EventRow(
        event_name=ev.name,
        experiment_id=str(d.experiment),
        timestamp=ts,
        sensor_id=str(sensor) if sensor else None,
        measurement_id=str(mid) if mid else None,
        temperature=getattr(d, "temperature", None),
        measurement_hash=getattr(d, "measurement_hash", None),
        payload=json.loads(d.model_dump_json()),
        event_key=key,
    )


# ---------- 3. Insert (idempotent) ----------
def save_event(session: Session, raw: dict) -> bool:
    """Validate and insert one decoded record. Returns False if it was a duplicate."""
    row = to_row(event_adapter.validate_python(raw))
    if session.scalar(select(EventRow.id).where(EventRow.event_key == row.event_key)):
        return False
    session.add(row)
    session.flush()
    return True
