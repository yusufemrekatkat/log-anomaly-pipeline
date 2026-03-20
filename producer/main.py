import os
import random
import time
from datetime import datetime

import httpx
from loguru import logger

# Docker'da 'http://ingestion:8000/ingest', lokalde 'http://localhost:8000/ingest'
INGEST_URL = os.getenv("INGEST_URL", "http://localhost:8000/ingest")


def generate_log():
    services = ["auth-service", "payment-service", "order-service", "inventory-service"]
    service = random.choice(services)
    resp_time = random.uniform(50, 200)
    level = "INFO"

    if service == "payment-service" and random.random() < 0.1:
        resp_time = random.uniform(1000, 5000)
        level = "ERROR"

    return {
        "timestamp": datetime.now().isoformat(),
        "service_name": service,
        "log_level": level,
        "message": f"Processed in {resp_time:.2f}ms",
        "response_time_ms": round(resp_time, 2),
        "ip": f"192.168.1.{random.randint(1, 100)}",
    }


if __name__ == "__main__":
    logger.info(f"Producer started. Sending to: {INGEST_URL}")
    with httpx.Client() as client:
        while True:
            try:
                log_entry = generate_log()
                response = client.post(INGEST_URL, json=log_entry, timeout=5.0)
                if response.status_code == 200:
                    logger.success(f"Sent: {log_entry['service_name']}")
            except Exception as e:
                logger.error(f"Target {INGEST_URL} unreachable: {e}")

            time.sleep(1)
