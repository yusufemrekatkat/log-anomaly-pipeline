import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")
engine = create_engine(DATABASE_URL)

FEATURE_COLUMNS = ["error_count", "warn_count", "request_count", "avg_response_time", "unique_ip_count"]
DRIFT_THRESHOLD = -0.05
DRIFT_WINDOW = 5

# State
model = None
scores = []

def train_or_update():
    global model
    query = f"SELECT {', '.join(FEATURE_COLUMNS)} FROM features ORDER BY window_start DESC LIMIT 500"
    df = pd.read_sql(query, engine)
    if len(df) > 25:
        m = IsolationForest(contamination=0.05, random_state=42)
        m.fit(df[FEATURE_COLUMNS])
        model = m
        print(f"Model update complete. Training set size: {len(df)}")
        return True
    return False

def check_drift(batch_scores):
    global scores
    if not batch_scores: return False
    scores.append(np.mean(batch_scores))
    if len(scores) > DRIFT_WINDOW: scores.pop(0)
    if len(scores) == DRIFT_WINDOW and np.mean(scores) < DRIFT_THRESHOLD:
        print(f"Drift alert: Avg score {np.mean(scores):.4f}. Retraining...")
        return True
    return False

def run_pipeline():
    global model
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

        df['window_end'] = df['window_start'] + pd.Timedelta(minutes=1)
        df.to_sql('features', engine, if_exists='append', index=False)

        if model is None and not train_or_update(): return

        batch_scores = []
        for _, row in df.iterrows():
            sample = pd.DataFrame([row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
            pred = model.predict(sample)[0]
            score = model.decision_function(sample)[0]
            batch_scores.append(score)

            if pred == -1:
                with engine.begin() as conn:
                    conn.execute(text("INSERT INTO anomalies (detected_at, service_name, anomaly_score) VALUES (:ts, :svc, :sc)"),
                                 {"ts": datetime.now(timezone.utc), "svc": row['service_name'], "sc": float(score)})
                print(f"Anomaly: {row['service_name']} | Score: {score:.4f}")

        if check_drift(batch_scores): train_or_update()
    except Exception as e:
        print(f"Loop Error: {e}")

if __name__ == "__main__":
    print("Extractor Active. Monitoring features and drift.")
    while True:
        run_pipeline()
        time.sleep(15)
