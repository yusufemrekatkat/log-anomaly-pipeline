import time

import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)


def extract_features():
    # Son 1 dakikalık veriyi çek ve aggregate et
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
    df = pd.read_sql(query, engine)
    return df


if __name__ == "__main__":
    print("Feature Extractor başlatıldı. Veri bekleniyor...")
    while True:
        try:
            features_df = extract_features()
            if not features_df.empty:
                print(
                    f"\n--- Feature Window: {features_df['window_start'].iloc[0]} ---"
                )
                print(
                    features_df[["service_name", "request_count", "avg_response_time"]]
                )
            else:
                print(".", end="", flush=True)
            time.sleep(10)
        except Exception as e:
            print(f"Hata: {e}")
            time.sleep(5)
