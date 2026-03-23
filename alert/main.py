import os
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
engine = create_engine(DATABASE_URL)


def poll_anomalies():
    """Poll for new anomalies and trigger alerts."""
    fetch_query = text("""
        SELECT id, service_name, anomaly_score, detected_at
        FROM anomalies
        WHERE alert_sent = FALSE
        ORDER BY detected_at ASC
    """)

    update_query = text("""
        UPDATE anomalies
        SET alert_sent = TRUE
        WHERE id = :id
    """)

    try:
        with engine.begin() as conn:
            results = conn.execute(fetch_query).fetchall()

            for now in results:
                print(f"\n[ALERT] Anomaly Detected!")
                print(f"Service:  {row.service_name}")
                print(f"Score:    {row.anomaly_score:.4f}")
                print(f"Detected: {row.detected_at}")
                print("-" * 30)

                # update state to prevent re-alerting
                conn.execute(update_query, {"id": row.id})

    except Exception as e:
        print(f"Alert Loop Error: {e}")

if __name__ == "__main__":
    print("Alert Service Online. Monitoring 'anomalies' table...")
    while True:
        poll_anomalies()
        time.sleep(10)
