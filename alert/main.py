import time

from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)


def process_alerts():
    # Fetch anomalies that haven't been alerted yet
    fetch_query = text("""
        SELECT id, service_name, anomaly_score, detected_at
        FROM anomalies
        WHERE alert_sent = FALSE AND is_anomaly = TRUE
    """)

    update_query = text("""
        UPDATE anomalies
        SET alert_sent = TRUE
        WHERE id = :id
    """)

    try:
        with engine.begin() as conn:
            results = conn.execute(fetch_query).fetchall()

            for row in results:
                # Simulate external notification (Slack/PagerDuty/Email)
                print("--- ALERT TRIGGERED ---")
                print(f"Service: {row.service_name}")
                print(f"Score:   {row.anomaly_score:.4f}")
                print(f"Time:    {row.detected_at}")
                print("-----------------------")

                # Mark as processed
                conn.execute(update_query, {"id": row.id})

    except Exception as e:
        print(f"Alert service error: {e}")


if __name__ == "__main__":
    print("Alert Service Started (Polling mode)")
    while True:
        process_alerts()
        time.sleep(5)
