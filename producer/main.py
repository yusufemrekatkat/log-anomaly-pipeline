import json
import random
import time
from datetime import datetime

from loguru import logger

# Konsola sadece ham JSON basması için konfigüre ediyoruz
logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{message}")

SERVICES = ["auth-service", "payment-service", "order-service", "inventory-service"]
LEVELS = ["INFO", "INFO", "INFO", "WARN", "ERROR"]


def generate_log():
    service = random.choice(SERVICES)
    level = random.choice(LEVELS)

    # Anomali simülasyonu: payment-service bazen çok yavaşlar
    resp_time = random.uniform(50, 200)
    if service == "payment-service" and random.random() < 0.1:
        resp_time = random.uniform(1000, 5000)
        level = "ERROR"

    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "service_name": service,
        "log_level": level,
        "message": f"Request processed in {resp_time:.2f}ms",
        "response_time_ms": round(resp_time, 2),
        "ip": f"192.168.1.{random.randint(1, 100)}",
    }
    return log_entry


if __name__ == "__main__":
    while True:
        log = generate_log()
        logger.info(json.dumps(log))
        time.sleep(1)  # Saniyede 1 log
