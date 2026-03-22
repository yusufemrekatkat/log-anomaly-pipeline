import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/log_db")
KAFKA_TOPIC = "raw-logs"
GROUP_ID = "ingestion-group"

# DB Setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    service_name = Column(String)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

def consume_logs():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Consumer started. Listening to {KAFKA_TOPIC}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Process message
            try:
                data = json.loads(msg.value().decode('utf-8'))

                with SessionLocal() as session:
                    log_entry = LogEntry(
                        timestamp=datetime.fromisoformat(data['timestamp']),
                        service_name=data['service_name'],
                        log_level=data['log_level'],
                        message=data['message'],
                        response_time_ms=data['response_time_ms'],
                        ip=data['ip']
                    )
                    session.add(log_entry)
                    session.commit()

            except Exception as e:
                print(f"Error persisting log: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
