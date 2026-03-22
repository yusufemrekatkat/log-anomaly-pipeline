import os
import json
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

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
    timestamp = Column(DateTime)
    service_name = Column(String)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)

def consume_logs():
    # Consumer for input, Producer for DLQ
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Manual commit for better reliability
    })

    dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    print(f"Ingestion Consumer with DLQ started. Listening to {INPUT_TOPIC}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            raw_value = msg.value()
            try:
                data = json.loads(raw_value.decode('utf-8'))

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

                consumer.commit(msg) # Only commit if DB write succeeds

            except Exception as e:
                print(f"Error processing message. Sending to DLQ: {e}")
                # Produce the raw failed message to the DLQ topic
                dlq_producer.produce(
                    DLQ_TOPIC,
                    value=raw_value,
                    headers=[("error", str(e).encode('utf-8'))]
                )
                dlq_producer.flush()
                consumer.commit(msg) # Commit failed message to move the offset forward

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
