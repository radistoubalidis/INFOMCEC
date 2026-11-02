import io
import json
import os
import signal

from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from models import Base, save_event


TOPIC = os.environ["TOPIC"]

# Require a stable group ID: replicas of this consumer must use the same value.
GROUP_ID = os.environ["GROUP_ID"]

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg",
    username=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    database=os.environ["POSTGRES_DB"],
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

running = True


def deserialize(msg):
    """Decode a Kafka Avro container into event dictionaries."""
    with DataFileReader(io.BytesIO(msg.value()), DatumReader()) as reader:
        schema = json.loads(reader.meta["avro.schema"].decode("utf-8"))
        schema_name = schema["name"]

        return [
            {"name": schema_name, "data": record}
            for record in reader
        ]


def store(events):
    """Process one Kafka message in a database transaction."""
    inserted = 0

    with Session(engine) as session:
        with session.begin():
            for event in events:
                try:
                    with session.begin_nested():
                        saved = save_event(session, event)

                    inserted += int(bool(saved))

                except ValidationError as exc:
                    print(
                        f"Skipping invalid event {event.get('name')}: {exc}",
                        flush=True,
                    )

    return inserted


def signal_handler(signum, frame):
    global running
    print("Stopping consumer safely...", flush=True)
    running = False


def consume():
    consumer = Consumer({
        "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "security.protocol": "SSL",
        "ssl.ca.location": os.environ["SSL_CA_LOCATION"],
        "ssl.keystore.location": os.environ["SSL_KEYSTORE_LOC"],
        "ssl.keystore.password": os.environ["SSL_KEYSTORE_PASSWORD"],
        "ssl.endpoint.identification.algorithm": "none",
    })

    try:
        consumer.subscribe(
            [TOPIC],
            on_assign=lambda _, partitions: print(
                f"Assigned partitions: {partitions}", flush=True
            ),
        )

        while running:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise RuntimeError(f"Kafka consumer error: {msg.error()}")

            events = deserialize(msg)
            store(events)

            # Commit only after processing and the DB commit succeed.
            # Unexpected DB/Redis errors escape the loop without committing.
            consumer.commit(message=msg, asynchronous=False)

    finally:
        consumer.close()


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        Base.metadata.create_all(engine)
        consume()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()