import time
import json
import random
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
from loguru import logger

# Configuration from Environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "raw-logs"

def delivery_report(err, msg):
    """ Callback called once for each message produced to indicate delivery result. """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # Silent success for performance, enable for debugging
        pass

def generate_log():
    services = ["auth-service", "payment-service", "order-service", "inventory-service"]
    service = random.choice(services)
    resp_time = random.uniform(50, 200)
    level = "INFO"

    if service == "payment-service" and random.random() < 0.1:
        resp_time = random.uniform(1000, 5000)
        level = "ERROR"

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service_name": service,
        "log_level": level,
        "message": f"Processed in {resp_time:.2f}ms",
        "response_time_ms": round(resp_time, 2),
        "ip": f"192.168.1.{random.randint(1, 100)}"
    }

if __name__ == "__main__":
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    logger.info(f"Kafka Producer started. Streaming to: {KAFKA_TOPIC}")

    while True:
        try:
            log_entry = generate_log()
            # Asynchronous produce
            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps(log_entry).encode('utf-8'),
                callback=delivery_report
            )
            # Serve delivery callbacks from previous asynchronous calls
            producer.poll(0)

            if random.random() < 0.1: # Periodic status update
                logger.info(f"Streaming active: last sent {log_entry['service_name']}")

        except Exception as e:
            logger.error(f"Streaming error: {e}")

        time.sleep(0.5) # 2 logs per second
