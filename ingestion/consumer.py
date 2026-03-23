import os
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
INPUT_TOPIC = "raw-logs"
DLQ_TOPIC = "raw-logs-dlq"

# Database Engine & Session
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- Senior Grade Models ---

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    log_id = Column(String, unique=True, index=True, nullable=False)
    timestamp = Column(DateTime, index=True)
    service_name = Column(String, index=True)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class FeatureEntry(Base):
    __tablename__ = "features"
    id = Column(Integer, primary_key=True)
    window_start = Column(DateTime, index=True)
    window_end = Column(DateTime)
    service_name = Column(String)
    error_count = Column(Integer)
    warn_count = Column(Integer)
    request_count = Column(Integer)
    avg_response_time = Column(Float)
    unique_ip_count = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class AnomalyEntry(Base):
    __tablename__ = "anomalies"
    id = Column(Integer, primary_key=True)
    detected_at = Column(DateTime, index=True)
    service_name = Column(String)
    anomaly_score = Column(Float)
    is_anomaly = Column(Boolean, default=True)
    alert_sent = Column(Boolean, default=False)

# Permanent Schema Initialization
Base.metadata.create_all(bind=engine)

def consume_logs():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ingestion-group-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    print(f"Ingestion Service Online. Schema authoritative: logs, features, anomalies.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            raw_val = msg.value()
            try:
                data = json.loads(raw_val.decode('utf-8'))
                with SessionLocal() as session:
                    log = LogEntry(
                        log_id=data['log_id'],
                        timestamp=datetime.fromisoformat(data['timestamp']),
                        service_name=data['service_name'],
                        log_level=data['log_level'],
                        message=data['message'],
                        response_time_ms=data['response_time_ms'],
                        ip=data['ip']
                    )
                    session.add(log)
                    session.commit()
                consumer.commit(msg)
            except IntegrityError:
                consumer.commit(msg) # Duplicate, skip
            except Exception as e:
                print(f"DLQ Event: {e}")
                dlq_producer.produce(DLQ_TOPIC, value=raw_val)
                dlq_producer.flush()
                consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
