import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
from prometheus_client import start_http_server, Counter, Histogram


# --- configuration ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
engine = create_engine(DATABASE_URL)

FEATURE_COLUMNS = ["error_count", "warn_count", "request_count", "avg_response_time", "unique_ip_count"]

# --- metrics ---
FEATURES_EXTRACTED = Counter('features_extracted_total', 'Total feature windows processed')
ANOMALIES_DETECTED = Counter('anomalies_detected_total', 'Total anomalies found', ['service'])
MODEL_RETRAIN_TIME = Histogram('model_retrain_seconds', 'Time spent retraining the model')


# --- State Management ---
model = None
scaler = StandardScaler()
scores_buffer = []
DRIFT_WINDOW = 5
DRIFT_THRESHOLD = -0.05

def train_model():
    """Fetches historical features and fits the isolation forest."""
    global model
    start_t = time.time()
    query = f"SELECT {', '.join(FEATURE_COLUMNS)} FROM features ORDER BY window_start DESC LIMIT 1000"

    try:
        df = pd.read_sql(query, engine)
        if len(df) < 50:
            return False

        # scaling is mandatory for variance-based detection
        scaled_data = scaler.fit_transform(df[FEATURE_COLUMNS])

        m = IsolationForest(contamination=0.05, random_state=42)
        m.fit(scaled_data)

        model = m
        MODEL_RETRAIN_TIME.observe(time.time() - start_t)
        print(f"Model retrained with {len(df)} samples.")
        return True
    except Exception as e:
        print(f"Training Error: {e}")
        return False


def run_pipeline():
    global model
    # 1. aggregate raw logs into 1-minute feature windows
    query = """
    SELECT date_trunc('minute', timestamp) as window_start, service_name,
           COUNT(*) FILTER (WHERE log_level = 'ERROR') as error_count,
           COUNT(*) FILTER (WHERE log_level = 'WARN') as warn_count,
           COUNT(*) as request_count, AVG(response_time_ms) as avg_response_time,
           COUNT(DISTINCT ip) as unique_ip_count
    FROM logs WHERE timestamp >= NOW() - INTERVAL '1 minute'
    GROUP BY 1, 2
    """
    try:
        df = pd.read_sql(query, engine)
        if df.empty: return

        # persistence: save the calculated features
        df['window_end'] = df['window_start'] + pd.Timedelta(minutes=1)
        df.to_sql('features', engine, if_exists='append', index=False)
        FEATURES_EXTRACTED.inc(len(df))

        if model is None and not train_model(): return

        current_batch_scores = []
        for _, row in df.iterrows():
            # 2. transform and predict
            feature_vector = row[FEATURE_COLUMNS].values.reshape(1, -1)
            scaled_vector = scaler.transform(feature_vector)

            prediction = model.predict(scaled_vector)[0]  # 1:normal   -1:anomaly
            score = model.decision_function(scaled_vector)[0]
            current_batch_scores.append(score)

            if prediction == -1:
                # root cause analysis (z-score attribution): compare current sample against scaler means and scales
                z_score = (feature_vector[0] - scaler.mean_) / scaler.scale_
                dominant_idx = np.argmax(np.abs(z_scores))
                dominant_feature = FEATURE_COLUMNS[dominant_idx]

                with engine.begin() as conn:
                    conn.execute(text("""
                                      INSERT INTO anomalies (detected_at, service_name, anomaly_score, dominant_feature)
                                      VALUES (:ts, :svc, :sc, :df)
                                      """),{
                                          "ts": datetime.now(timezone.utc),
                                          "svc": row['service_name'],
                                          "sc": float(score),
                                          "df": dominant_feature
                                        })
                ANOMALIES_DETECTED.labels(service=row['service_name']).inc()
                print(f"Anomaly: {row['service_name']} | Root Cause: {dominant_feature}")

        # 4. simple concept drift detection
        avg_score = np.mean(current_batch_scores)
        scores_buffer.append(avg_score)
        if len(scores_buffer) > DRIFT_WINDOW:
            scores_buffer.pop(0)
            if np.mean(scores_buffer) < DRIFT_THRESHOLD:
                print("Concept drift detected. Triggering retrain...")
                train_model()

    except Exception as e:
        print(f"Pipeline Error: {e}")


if __name__ == "__main__":
    start_http_server(8002)
    print("Extractor Active. Monitoring Features and Drift.")
    while True:
        run_pipeline()
        time.sleep(30)
