import time
from datetime import UTC, datetime

import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)

FEATURE_COLUMNS = [
    "error_count",
    "warn_count",
    "request_count",
    "avg_response_time",
    "unique_ip_count",
]


def get_training_data():
    query = f"SELECT {', '.join(FEATURE_COLUMNS)} FROM features ORDER BY window_start DESC LIMIT 100"
    return pd.read_sql(query, engine)


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
        df.to_sql("features", engine, if_exists="append", index=False)

        training_df = get_training_data()

        if len(training_df) > 10:
            # Training with feature names intact
            model = IsolationForest(contamination=0.05, random_state=42)
            model.fit(training_df[FEATURE_COLUMNS])

            for _, row in df.iterrows():
                # Prediction using DataFrame with feature names to suppress warnings
                current_sample = pd.DataFrame(
                    [row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS
                )

                prediction = model.predict(current_sample)[0]
                score = model.decision_function(current_sample)[0]

                if prediction == -1:
                    insert_anomaly(row["service_name"], score)
    except Exception as e:
        print(f"Pipeline error: {e}")


def insert_anomaly(service_name, score):
    query = text("""
        INSERT INTO anomalies (service_name, anomaly_score, is_anomaly, detected_at)
        VALUES (:service, :score, TRUE, :ts)
    """)
    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "service": service_name,
                "score": float(score),
                "ts": datetime.now(UTC),
            },
        )
    print(f"ML Anomaly: {service_name} | Score: {score:.4f}")


if __name__ == "__main__":
    print("Clean ML Service Started")
    while True:
        process_metrics()
        time.sleep(10)
