import time
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)


def get_training_data():
    # Fetch last 100 feature sets to train/calibrate the model
    query = "SELECT error_count, warn_count, request_count, avg_response_time, unique_ip_count FROM features ORDER BY window_start DESC LIMIT 100"
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

        # ML Training and Prediction
        training_df = get_training_data()

        # We need a minimum amount of data to train a meaningful model
        if len(training_df) > 10:
            model = IsolationForest(contamination=0.05, random_state=42)
            model.fit(training_df)

            for _, row in df.iterrows():
                # Prepare current record for prediction
                current_features = np.array(
                    [
                        [
                            row["error_count"],
                            row["warn_count"],
                            row["request_count"],
                            row["avg_response_time"],
                            row["unique_ip_count"],
                        ]
                    ]
                )

                # -1 is anomaly, 1 is normal
                prediction = model.predict(current_features)[0]
                score = model.decision_function(current_features)[0]

                if prediction == -1:
                    insert_anomaly(row["service_name"], score, is_ml=True)
        else:
            print("Insufficient data for ML training. Waiting for more features...")

    except Exception as e:
        print(f"Error in ML pipeline: {e}")


def insert_anomaly(service_name, score, is_ml=False):
    query = text("""
        INSERT INTO anomalies (service_name, anomaly_score, is_anomaly, detected_at)
        VALUES (:service, :score, TRUE, :ts)
    """)
    with engine.begin() as conn:
        conn.execute(
            query,
            {"service": service_name, "score": float(score), "ts": datetime.utcnow()},
        )
    print(f"ML Anomaly detected: {service_name} | Score: {score:.4f}")


if __name__ == "__main__":
    print("ML-Enhanced Feature Extraction Service Started")
    while True:
        process_metrics()
        time.sleep(10)
