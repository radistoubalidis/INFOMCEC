from __future__ import annotations

import json
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Annotated, Literal, Union
from uuid import UUID
import os

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator
from requests.adapters import HTTPAdapter
from sqlalchemy import JSON, DateTime, Float, Index, String, UniqueConstraint, create_engine, func, select, Boolean
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

class ExperimentTerminatedEvent(_Base):
    name: Literal["experiment_terminated"]
    data: ExperimentTimestamped

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

# ---------- 2. Database model ----------
class Base(DeclarativeBase):
    pass

EVENT_PHASE = {
    1: "ExperimentConfig",
    2: "stabilization_started",
    3: "experiment_started",
    4: "experiment_terminated",
}

pending = {}
was_in_range = {}
stabilized_notified = {}
state_cache = {}  # experiment_id -> snapshot of ExperimentState (single consumer process)

with open('/usr/src/auth/token') as f:
    token = f.read().strip()
ntf_url = f"{os.environ['NOTIFICATIONS_URL']}?token={token}"

http = requests.Session()
http.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=32))
http.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=32))
executor = ThreadPoolExecutor(max_workers=int(os.environ.get("NOTIFY_WORKERS", "16")))

class ExperimentState(Base):
    __tablename__ = "experiment_states"

    experiment_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    phase: Mapped[int] = mapped_column(String(320))
    researcher: Mapped[str | None] = mapped_column(String(320))
    sensors: Mapped[list | None] = mapped_column(JSON)
    lower_threshold: Mapped[float | None] = mapped_column(Float)
    upper_threshold: Mapped[float | None] = mapped_column(Float)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

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

class TemperatureAverage(Base):
    __tablename__ = "temperature_averages"

    id: Mapped[int] = mapped_column(primary_key=True)
    experiment_id: Mapped[str] = mapped_column(String(36))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    measurement_id: Mapped[str] = mapped_column(String(36))
    temperature: Mapped[float] = mapped_column(Float)
    in_range: Mapped[bool] = mapped_column(Boolean)

    __table_args__ = (
        UniqueConstraint("experiment_id", "timestamp", name="uq_avg_exp_ts"),
        Index("ix_avg_exp_ts", "experiment_id", "timestamp"),
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

def _snapshot(state):
    return SimpleNamespace(
        experiment_id=state.experiment_id,
        phase=state.phase,
        researcher=state.researcher,
        sensors=list(state.sensors) if state.sensors else None,
        lower_threshold=state.lower_threshold,
        upper_threshold=state.upper_threshold,
    )

def _advance_state(session: Session, state, ev):
    d = ev.data
    if state is None:
        state = ExperimentState(experiment_id=str(d.experiment))
        session.add(state)
    if ev.name == EVENT_PHASE[1]:
        state.phase = EVENT_PHASE[1]
        state.researcher = d.researcher
        state.sensors = [str(s) for s in d.sensors]
        state.lower_threshold = d.temperature_range.lower_threshold
        state.upper_threshold = d.temperature_range.upper_threshold
    elif ev.name == EVENT_PHASE[2]:
        state.phase = EVENT_PHASE[2]
    elif state.phase != EVENT_PHASE[3] and ev.name == EVENT_PHASE[3]:
        state.phase = EVENT_PHASE[3]
        state.started_at = d.timestamp
    elif state.phase == EVENT_PHASE[3] and ev.name == EVENT_PHASE[3] and (hasattr(d, 'experiment') and hasattr(d, 'timestamp')):
        # for some reason for the last event of an experient the event schema has name experiment_started and body like [{'experiment': '3ab4a42a-f0a7-4eec-b2fc-41cfbc761255', 'timestamp': 1791223630.5652282}]
        state.phase = EVENT_PHASE[4]
        state.terminated_at = d.timestamp
    return state

def save_event(session: Session, raw: dict) -> bool:
    ev = event_adapter.validate_python(raw)
    exp_id = str(ev.data.experiment)

    if ev.name == "sensor_temperature_measured":
        snap = state_cache.get(exp_id)
        if snap is None:
            state = session.get(ExperimentState, exp_id)
            if state is None:
                return False
            snap = _snapshot(state)
            state_cache[exp_id] = snap
        return check_temp(raw['data'], snap, session)

    state = session.get(ExperimentState, exp_id)
    row = to_row(ev)
    if session.scalar(select(EventRow.id).where(EventRow.event_key == row.event_key)):
        return False
    session.add(row)
    state = _advance_state(session, state, ev)
    session.flush()
    state_cache[exp_id] = _snapshot(state)
    return True

def _post(kind, payload):
    for attempt in (1, 2):
        try:
            r = http.post(url=ntf_url, json=payload, timeout=(3, 10))
            if not 200 <= r.status_code < 300:
                print(f"Notification rejected ({kind}): {r.status_code} {r.text[:120]}")
            return
        except requests.ConnectionError as e:
            # request never reached the service: one retry is safe
            if attempt == 2:
                print(f"Notification failed ({kind}): {e}")
        except requests.RequestException as e:
            # read timeout: the service may have received it, so do not retry
            print(f"Notification outcome unknown ({kind}): {e}")
            return

def _notify(kind, state, entry):
    payload = {
        "notification_type": kind,
        "researcher": state.researcher,
        "experiment_id": state.experiment_id,
        "measurement_id": entry["measurement_id"],
        "cipher_data": entry["hash"],
    }
    executor.submit(_post, kind, payload)

def check_temp(event: dict, state, session: Session) -> bool:
    if state.phase not in (EVENT_PHASE[2], EVENT_PHASE[3]):
        return False
    if not state.sensors or state.lower_threshold is None:
        return False

    exp_id = state.experiment_id
    key = (exp_id, event['timestamp'])
    entry = pending.setdefault(key, {
        "temps": {},
        "measurement_id": str(event['measurement_id']),
        "hash": event['measurement_hash'],
    })
    entry["temps"][str(event['sensor'])] = event['temperature']

    expected = set(state.sensors)
    if not expected.issubset(entry["temps"]):
        return False
    del pending[key]

    avg = sum(entry["temps"][s] for s in expected) / len(expected)
    inside = state.lower_threshold <= avg <= state.upper_threshold

    if state.phase == EVENT_PHASE[2]:  # stabilization: notify, don't store
        if inside and not stabilized_notified.get(exp_id, False):
            stabilized_notified[exp_id] = True
            _notify("Stabilized", state, entry)
        return False

    ts = datetime.fromtimestamp(event['timestamp'], tz=timezone.utc)
    exists = session.scalar(
        select(TemperatureAverage.id).where(
            TemperatureAverage.experiment_id == exp_id,
            TemperatureAverage.timestamp == ts,
        )
    )
    if exists:
        return False
    session.add(TemperatureAverage(
        experiment_id=exp_id, timestamp=ts,
        measurement_id=entry["measurement_id"],
        temperature=avg, in_range=inside,
    ))

    previously_inside = was_in_range.get(exp_id, True)
    was_in_range[exp_id] = inside
    if not inside and previously_inside:
        _notify("OutOfRange", state, entry)
    return True