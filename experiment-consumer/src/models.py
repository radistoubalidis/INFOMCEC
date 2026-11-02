from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Annotated, Literal, Union
from uuid import UUID

import redis
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
)
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    JSON,
    String,
    UniqueConstraint,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
)


# ---------- Validation ----------

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
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def epoch_to_datetime(cls, value):
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return value


class SensorTemperatureData(ExperimentTimestamped):
    sensor: UUID
    measurement_id: UUID
    temperature: float
    measurement_hash: str


class ExperimentConfigEvent(_Base):
    name: Literal["ExperimentConfig"]
    data: ExperimentConfigData


class StabilizationStartedEvent(_Base):
    name: Literal["stabilization_started"]
    data: ExperimentTimestamped


class ExperimentStartedEvent(_Base):
    name: Literal["experiment_started"]
    data: ExperimentTimestamped


class ExperimentTerminatedEvent(_Base):
    name: Literal["experiment_terminated"]
    data: ExperimentTimestamped


class SensorTemperatureMeasuredEvent(_Base):
    name: Literal["sensor_temperature_measured"]
    data: SensorTemperatureData


Event = Annotated[
    Union[
        ExperimentConfigEvent,
        StabilizationStartedEvent,
        ExperimentStartedEvent,
        ExperimentTerminatedEvent,
        SensorTemperatureMeasuredEvent,
    ],
    Field(discriminator="name"),
]

event_adapter = TypeAdapter(Event)


# ---------- Database models ----------

class Base(DeclarativeBase):
    pass


EVENT_PHASE = {
    1: "ExperimentConfig",
    2: "stabilization_started",
    3: "experiment_started",
    4: "experiment_terminated",
}


class ExperimentState(Base):
    __tablename__ = "experiment_states"

    experiment_id: Mapped[str] = mapped_column(
        String(36), primary_key=True
    )
    phase: Mapped[str] = mapped_column(String(320))
    researcher: Mapped[str | None] = mapped_column(String(320))
    sensors: Mapped[list | None] = mapped_column(JSON)
    lower_threshold: Mapped[float | None] = mapped_column(Float)
    upper_threshold: Mapped[float | None] = mapped_column(Float)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    terminated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )


class EventRow(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_name: Mapped[str] = mapped_column(String(64), index=True)
    experiment_id: Mapped[str] = mapped_column(String(36), index=True)
    timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    sensor_id: Mapped[str | None] = mapped_column(String(36))
    measurement_id: Mapped[str | None] = mapped_column(String(36))
    temperature: Mapped[float | None] = mapped_column(Float)
    measurement_hash: Mapped[str | None] = mapped_column(String(512))
    payload: Mapped[dict] = mapped_column(JSON)
    event_key: Mapped[str] = mapped_column(String(200))

    __table_args__ = (
        UniqueConstraint("event_key", name="uq_events_event_key"),
        Index(
            "ix_events_exp_name_ts",
            "experiment_id",
            "event_name",
            "timestamp",
        ),
    )


class TemperatureAverage(Base):
    __tablename__ = "temperature_averages"

    id: Mapped[int] = mapped_column(primary_key=True)
    experiment_id: Mapped[str] = mapped_column(String(36))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    measurement_id: Mapped[str] = mapped_column(String(36))
    temperature: Mapped[float] = mapped_column(Float)
    in_range: Mapped[bool] = mapped_column(Boolean)

    __table_args__ = (
        UniqueConstraint(
            "experiment_id",
            "timestamp",
            name="uq_avg_exp_ts",
        ),
        Index("ix_avg_exp_ts", "experiment_id", "timestamp"),
    )


# ---------- Redis sensor buffer ----------

redis_client = redis.Redis.from_url(
    os.environ.get("REDIS_URL", "redis://redis:6379/0"),
    decode_responses=True,
)

BUFFER_TTL_SECONDS = int(
    os.environ.get("BUFFER_TTL_SECONDS", "3600")
)

AVERAGES_STREAM = os.environ.get("AVERAGES_STREAM", "averages")


# Each call adds one sensor reading and checks the actual expected IDs.
# Completed buffers remain available until expiry for replay/recovery.
BUFFER_LUA = """
local expected = cjson.decode(ARGV[1])
local sensor = ARGV[2]
local known = false

for _, id in ipairs(expected) do
    if id == sensor then
        known = true
        break
    end
end

if not known then
    return false
end

redis.call('HSETNX', KEYS[1], sensor, ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))

local readings = {}

for _, id in ipairs(expected) do
    local reading = redis.call('HGET', KEYS[1], id)

    if not reading then
        return false
    end

    table.insert(readings, reading)
end

return readings
"""

buffer_script = redis_client.register_script(BUFFER_LUA)


# ---------- Lifecycle persistence ----------

def to_row(ev) -> EventRow:
    data = ev.data
    timestamp = getattr(data, "timestamp", None)
    sensor = getattr(data, "sensor", None)
    measurement_id = getattr(data, "measurement_id", None)

    event_key = "|".join([
        ev.name,
        str(data.experiment),
        str(measurement_id or ""),
        str(sensor or ""),
        timestamp.isoformat() if timestamp else "",
    ])

    return EventRow(
        event_name=ev.name,
        experiment_id=str(data.experiment),
        timestamp=timestamp,
        sensor_id=str(sensor) if sensor else None,
        measurement_id=str(measurement_id) if measurement_id else None,
        temperature=getattr(data, "temperature", None),
        measurement_hash=getattr(data, "measurement_hash", None),
        payload=json.loads(data.model_dump_json()),
        event_key=event_key,
    )

def advance_state(session: Session, state, ev):
    data = ev.data

    if state is None:
        state = ExperimentState(
            experiment_id=str(data.experiment)
        )
        session.add(state)

    if ev.name == EVENT_PHASE[1]:
        state.phase = EVENT_PHASE[1]
        state.researcher = data.researcher
        state.sensors = [str(sensor) for sensor in data.sensors]
        state.lower_threshold = data.temperature_range.lower_threshold
        state.upper_threshold = data.temperature_range.upper_threshold

    elif ev.name == EVENT_PHASE[2]:
        state.phase = EVENT_PHASE[2]

    elif ev.name == EVENT_PHASE[3]:
        if state.phase == EVENT_PHASE[3]:
            # Producer bug: termination is sent as a second
            # experiment_started event.
            state.phase = EVENT_PHASE[4]
            state.terminated_at = data.timestamp
        else:
            state.phase = EVENT_PHASE[3]
            state.started_at = data.timestamp

    elif ev.name == EVENT_PHASE[4]:
        state.phase = EVENT_PHASE[4]
        state.terminated_at = data.timestamp

def save_event(session: Session, raw: dict) -> bool:
    ev = event_adapter.validate_python(raw)

    state = session.get(
        ExperimentState,
        str(ev.data.experiment),
    )

    if ev.name == "sensor_temperature_measured":
        if state is None:
            raise RuntimeError(
                f"Missing experiment configuration: {ev.data.experiment}"
            )

        return check_temp(ev.data, state, session)

    row = to_row(ev)

    existing = session.scalar(
        select(EventRow.id).where(
            EventRow.event_key == row.event_key
        )
    )

    if existing is not None:
        return False

    session.add(row)
    advance_state(session, state, ev)
    session.flush()
    return True


# ---------- Averaging and Redis publication ----------

def check_temp(
    event: SensorTemperatureData,
    state: ExperimentState,
    session: Session,
) -> bool:
    if state.phase not in (EVENT_PHASE[2], EVENT_PHASE[3]):
        return False

    if (
        not state.sensors
        or state.lower_threshold is None
        or state.upper_threshold is None
    ):
        return False

    expected_sensors = sorted(set(state.sensors))
    sensor_id = str(event.sensor)

    if sensor_id not in expected_sensors:
        return False

    # Use the same normalized timestamp for all sensor readings
    # belonging to one measurement batch.
    timestamp_key = event.timestamp.isoformat()
    buffer_key = (
        f"buffer:{state.experiment_id}:{timestamp_key}"
    )

    readings_json = buffer_script(
        keys=[buffer_key],
        args=[
            json.dumps(expected_sensors),
            sensor_id,
            json.dumps({
                "temperature": event.temperature,
                "measurement_id": str(event.measurement_id),
                "hash": event.measurement_hash,
            }),
            BUFFER_TTL_SECONDS,
        ],
    )

    if not readings_json:
        return False

    readings = [json.loads(value) for value in readings_json]
    average = sum(
        reading["temperature"] for reading in readings
    ) / len(readings)

    inside = (
        state.lower_threshold
        <= average
        <= state.upper_threshold
    )

    # Select metadata deterministically from the first expected sensor.
    representative = readings[0]
    stabilizing = state.phase == EVENT_PHASE[2]
    inserted = False

    # Keep the existing behavior: store only run-phase averages.
    if not stabilizing:
        result = session.execute(
            pg_insert(TemperatureAverage)
            .values(
                experiment_id=state.experiment_id,
                timestamp=event.timestamp,
                measurement_id=representative["measurement_id"],
                temperature=average,
                in_range=inside,
            )
            .on_conflict_do_nothing(
                constraint="uq_avg_exp_ts"
            )
            .returning(TemperatureAverage.id)
        )

        inserted = result.scalar_one_or_none() is not None

    # Both phases go to the notifier.
    # Replays may publish duplicates; consumers must be idempotent.
    redis_client.xadd(
        AVERAGES_STREAM,
        {
            "experiment_id": state.experiment_id,
            "researcher": state.researcher or "",
            "measurement_id": representative["measurement_id"],
            "hash": representative["hash"],
            "timestamp": repr(event.timestamp.timestamp()),
            "temperature": repr(average),
            "in_range": "1" if inside else "0",
            "phase": "stab" if stabilizing else "run",
        },
    )

    return inserted