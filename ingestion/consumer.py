import os
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel, ValidationError, Field
from prometheus_client import start_http_server, Counter, Histogram

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/log_db")
INPUT_TOPIC = "raw-logs"
DLQ_TOPIC = "raw-logs-dlq"

engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Metrics
LOGS_CONSUMED = Counter('logs_consumed_total', 'Total logs successfully ingested', ['service'])
INVALID_LOGS = Counter('invalid_logs_total', 'Total logs that failed validation')
DLQ_EVENTS = Counter('dlq_events_total', 'Total messages sent to Dead Letter Queue')
PROCESSING_TIME = Histogram('log_processing_seconds', 'Time spent processing a single log')

# Data Contract
class LogContract(BaseModel):
    log_id: str
    timestamp: str
    service_name: str
    log_level: str
    message: str
    response_time_ms: float = Field(ge=0)
    ip: str
    is_true_anomaly: bool = False

# Persistence Models
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
    is_true_anomaly = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class InvalidLogEntry(Base):
    __tablename__ = "invalid_logs"
    id = Column(Integer, primary_key=True)
    raw_data = Column(String)
    error_reason = Column(String)
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
    dominant_feature = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

def consume_logs():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ingestion-group-v3',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    start_http_server(8001)
    print("Ingestion Service Online. Authoritative schema initialized.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"[!] Kafka Network Error: {msg.error()}")
                continue

            raw_val = msg.value()
            start_t = time.time()

            print("-> Message received from Kafka")

            for attempt in range(3):
                try:
                    data = json.loads(raw_val.decode('utf-8'))

                    with SessionLocal() as session:
                        try:
                            valid = LogContract(**data)
                        except ValidationError as ve:
                            print(f"[!] Schema Error. Routing to invalid_logs.")
                            session.add(InvalidLogEntry(raw_data=raw_val.decode('utf-8'), error_reason=str(ve)))
                            session.commit()
                            INVALID_LOGS.inc()
                            break

                        log = LogEntry(
                            log_id=valid.log_id,
                            timestamp=datetime.fromisoformat(valid.timestamp.replace('Z', '+00:00')),
                            service_name=valid.service_name,
                            log_level=valid.log_level,
                            message=valid.message,
                            response_time_ms=valid.response_time_ms,
                            ip=valid.ip,
                            is_true_anomaly=valid.is_true_anomaly
                        )
                        session.add(log)
                        session.commit()
                        LOGS_CONSUMED.labels(service=valid.service_name).inc()
                        print(f"[OK] Saved to DB: {valid.log_id}")

                    break

                except IntegrityError:
                    print("[!] Duplicate Record. Skipping.")
                    break
                except Exception as e:
                    print(f"[!] Processing Fault (Attempt {attempt+1}): {e}")
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        print("[!] Max retries reached. Routing to DLQ.")
                        DLQ_EVENTS.inc()
                        dlq_producer.produce(DLQ_TOPIC, value=raw_val)
                        dlq_producer.flush()

            consumer.commit(msg)
            PROCESSING_TIME.observe(time.time() - start_t)

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
