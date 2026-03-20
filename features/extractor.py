import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)

# Threshold for statistical baseline
LATENCY_THRESHOLD_MS = 400.0


def process_metrics():
    query = """
    SELECT
        date_trunc('minute', timestamp) as window_start,
        service_name,
        COUNT(*) FILTER (WHERE log_level = 'ERROR') as error_count,
        COUNT(*) FILTER (WHERE log_level = 'WARN') as warn_count,
        COUNT(*) as request_count,
        AVG(response_time_ms) as avg_response_time,
        COUNT(DISTINCT ip) as unique_ip_count
    FROM logs
    WHERE timestamp >= NOW() - INTERVAL '1 minute'
    GROUP BY window_start, service_name
    """

    try:
        df = pd.read_sql(query, engine)
        if df.empty:
            return

        df["window_end"] = df["window_start"] + pd.Timedelta(minutes=1)

        # Persist features
        df.to_sql("features", engine, if_exists="append", index=False)

        # Detect anomalies based on static baseline
        for _, row in df.iterrows():
            if row["avg_response_time"] > LATENCY_THRESHOLD_MS:
                insert_anomaly(row["service_name"], row["avg_response_time"])

    except Exception as e:
        print(f"Error processing metrics: {e}")


def insert_anomaly(service_name, score):
    query = text("""
        INSERT INTO anomalies (service_name, anomaly_score, is_anomaly, detected_at)
        VALUES (:service, :score, TRUE, :ts)
    """)
    with engine.begin() as conn:
        conn.execute(
            query,
            {"service": service_name, "score": float(score), "ts": datetime.utcnow()},
        )
    print(f"Anomaly detected: {service_name} | Latency: {score:.2f}ms")


if __name__ == "__main__":
    print("Feature Extraction Service Started")
    while True:
        process_metrics()
        time.sleep(10)
