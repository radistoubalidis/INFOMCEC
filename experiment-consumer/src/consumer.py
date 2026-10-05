import io
import json
import os
import random
import signal
import sys

from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer, KafkaException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import Base, save_event, check_temp

TOPIC = os.environ['TOPIC']
SSL_LOCATION = os.environ['SSL_CA_LOCATION']
BOOTSTRAP_SERVERS = os.environ['BOOTSTRAP_SERVERS']
SSL_KEYSTORE_LOC = os.environ['SSL_KEYSTORE_LOC']
SSL_KEYSTORE_PASSWORD = os.environ['SSL_KEYSTORE_PASSWORD']
DATABASE_URL = f"postgresql+psycopg://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@postgres:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
GROUP_ID = os.environ.get('GROUP_ID') or f"{random.random()}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)


def deserialize(msg):
    """Decode one Kafka message (an Avro container file) into event dicts."""
    reader = DataFileReader(io.BytesIO(msg.value()), DatumReader())
    try:
        schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
        schema_name = schema['name']
        return [{'name': schema_name, 'data': record} for record in reader]
    finally:
        reader.close()
    


def store(events):
    """Insert the events of one message; invalid events are skipped individually."""
    inserted = 0
    with Session(engine) as session:
        try:
            for event in events:
                try:
                    with session.begin_nested():
                        inserted += save_event(session, event)
                except ValidationError as e:
                    print(f"Skipping invalid event {event.get('name')}: {e}")
            session.commit()
        except Exception:
            session.rollback()
            raise
    return inserted


config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
    'security.protocol': 'SSL',
    'ssl.ca.location': SSL_LOCATION,
    'ssl.keystore.location': SSL_KEYSTORE_LOC,
    'ssl.keystore.password': SSL_KEYSTORE_PASSWORD,
    'ssl.endpoint.identification.algorithm': 'none',
}

c = Consumer(config)
running = True


def signal_handler(sig, frame):
    global running
    print('EXITING SAFELY!')
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def consume(topic: str = TOPIC):
    c.subscribe([topic], on_assign=lambda _, p_list: print(p_list))
    try:
        while running:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            try:
                obj = deserialize(msg)
                inserted = store(obj)
            except ValidationError as e:
                print(f"Skipping invalid message at offset {msg.offset()}: {e}")
            c.commit(message=msg, asynchronous=False)
    finally:
        c.close()
        engine.dispose()


if __name__ == '__main__':
    try:
        consume()
    except KafkaException as e:
        print(f"Kafka error: {e}")
        sys.exit(1)
