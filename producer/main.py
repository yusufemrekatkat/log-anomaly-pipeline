import random
import time
from datetime import datetime

import httpx
from loguru import logger

# API URL
INGEST_URL = "http://localhost:8000/ingest"

SERVICES = ["auth-service", "payment-service", "order-service", "inventory-service"]
LEVELS = ["INFO", "INFO", "INFO", "WARN", "ERROR"]


def generate_log():
    service = random.choice(SERVICES)
    level = random.choice(LEVELS)
    resp_time = random.uniform(50, 200)

    if service == "payment-service" and random.random() < 0.1:
        resp_time = random.uniform(1000, 5000)
        level = "ERROR"

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "service_name": service,
        "log_level": level,
        "message": f"Request processed in {resp_time:.2f}ms",
        "response_time_ms": round(resp_time, 2),
        "ip": f"192.168.1.{random.randint(1, 100)}",
    }


if __name__ == "__main__":
    # Tek bir client session'ı kullanmak performans için daha iyidir
    with httpx.Client() as client:
        while True:
            log_entry = generate_log()
            try:
                response = client.post(INGEST_URL, json=log_entry)
                if response.status_code == 200:
                    logger.success(
                        f"Sent: {log_entry['service_name']} - {log_entry['response_time_ms']}ms"
                    )
                else:
                    logger.error(f"Failed to send: {response.status_code}")
            except Exception as e:
                logger.error(f"Connection error: {e}")

            time.sleep(1)
