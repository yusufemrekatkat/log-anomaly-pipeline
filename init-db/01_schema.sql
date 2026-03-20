CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    service_name VARCHAR(50) NOT NULL,
    log_level VARCHAR(10) NOT NULL,
    message TEXT,
    response_time_ms FLOAT,
    ip VARCHAR(45),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_logs_timestamp ON logs(timestamp);
CREATE INDEX idx_logs_service_name ON logs(service_name);

CREATE TABLE IF NOT EXISTS features (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    service_name VARCHAR(50) NOT NULL,
    error_count INTEGER DEFAULT 0,
    warn_count INTEGER DEFAULT 0,
    request_count INTEGER DEFAULT 0,
    avg_response_time FLOAT DEFAULT 0,
    unique_ip_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    service_name VARCHAR(50) NOT NULL,
    anomaly_score FLOAT,
    is_anomaly BOOLEAN,
    alert_sent BOOLEAN DEFAULT FALSE
);
