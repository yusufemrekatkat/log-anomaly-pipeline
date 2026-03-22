import os
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
INPUT_TOPIC = "raw-logs"
DLQ_TOPIC = "raw-logs-dlq"
GROUP_ID = "ingestion-group"

# DB Setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    log_id = Column(String, unique=True, index=True) # Ensure unique constraint
    timestamp = Column(DateTime)
    service_name = Column(String)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)

# CRITICAL: Create tables if they do not exist
Base.metadata.create_all(bind=engine)

def consume_logs():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    print("Ingestion Consumer Started. Synchronizing schema...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            raw_value = msg.value()
            try:
                data = json.loads(raw_value.decode('utf-8'))
                with SessionLocal() as session:
                    log_entry = LogEntry(
                        log_id=data['log_id'],
                        timestamp=datetime.fromisoformat(data['timestamp']),
                        service_name=data['service_name'],
                        log_level=data['log_level'],
                        message=data['message'],
                        response_time_ms=data['response_time_ms'],
                        ip=data['ip']
                    )
                    session.add(log_entry)
                    session.commit()
                consumer.commit(msg)

            except IntegrityError:
                print(f"Duplicate detected: {data.get('log_id')}")
                consumer.commit(msg)
            except Exception as e:
                print(f"Error: {e}. Sending to DLQ.")
                dlq_producer.produce(DLQ_TOPIC, value=raw_value)
                dlq_producer.flush()
                consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
