import logging
import os
import time
from threading import Event

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alert")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
stop_event = Event()


def enqueue_alert(row):
    """
    Placeholder for alert delivery: push to queue / call webhook / send email.
    Replace this with real queue client (e.g., Redis, RabbitMQ, SNS).
    """
    logger.info(
        "Enqueueing alert for service=%s id=%s score=%.4f",
        row.service_name,
        row.id,
        row.anomaly_score,
    )


def fetch_and_mark_batch(conn, limit):
    """
    Atomically mark rows as processed and return them using a CTE.
    Uses Postgres-friendly pattern with FOR UPDATE SKIP LOCKED + UPDATE ... FROM.
    """
    stmt = text(
        """
        WITH candidates AS(
        SELECT id
        FROM anomalies
        WHERE alert_sent = FALSE
        ORDER BY detected_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT :limit
        )
        UPDATE anomalies
        SET alert_sent = TRUE
        FROM candidates
        WHERE anomalies.id = candidates.id
        RETURNING anomalies.id, anomalies.service_name, anomalies.anomaly_score, anomalies.detected_at;
        """
    )
    result = conn.execute(stmt, {"limit": limit})
    return result.fetchall()


def poll_anomalies():
    """Single poll iteration: fetch a batch atomically and enqueue alerts."""
    try:
        with engine.begin() as conn:
            rows = fetch_and_mark_batch(conn, BATCH_SIZE)
            if not rows:
                logger.debug("No anomalies found this cycle.")
                return
            for row in rows:
                logger.info(
                    "Anomaly detected: service=%s score=%.4f at=%s",
                    row.service_name,
                    row.anomaly_score,
                    row.detected_at,
                )
                enqueue_alert(row)

    except Exception:
        logger.exception("Alert loop error")


def shutdown(signum, frame):
    logger.info("Shutdown signal received (%s). Stopping poller...", signum)
    stop_event.set()


def main_loop():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    logger.info("Alert Service Online. Monitoring 'anomalies' table...")

    try:
        while not stop_event.is_set():
            poll_anomalies()
            # sleep in small increments to respond quickly to shutdown
            waited = 0.0
            interval = 0.5
            while waited < POLL_INTERVAL and not stop_event.is_set():
                time.sleep(interval)
                waited += interval
    finally:
        logger.info("Cleaniing up DB connections...")
        engine.dispose()
        logger.info("Stopped.")


if __name__ == "__main__":
    main_loop()
