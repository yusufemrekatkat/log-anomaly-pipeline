import os
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from datetime import datetime, timezone
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- MODELS ---

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True)
    service_name = Column(String, index=True)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class FeatureEntry(Base):
    __tablename__ = "features"
    id = Column(Integer, primary_key=True)
    window_start = Column(DateTime, index=True)
    window_end = Column(DateTime)
    service_name = Column(String)
    error_count = Column(Integer)
    warn_count = Column(Integer)
    request_count = Column(Integer)
    avg_response_time = Column(Float)
    unique_ip_count = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class AnomalyEntry(Base):
    __tablename__ = "anomalies"
    id = Column(Integer, primary_key=True)
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    service_name = Column(String)
    anomaly_score = Column(Float)
    is_anomaly = Column(Boolean, default=True)
    alert_sent = Column(Boolean, default=False)

# Create Tables
Base.metadata.create_all(bind=engine)

app = FastAPI()

# --- API ---

class LogCreate(BaseModel):
    timestamp: datetime
    service_name: str
    log_level: str
    message: str
    response_time_ms: float
    ip: str

@app.post("/ingest")
def ingest_log(log: LogCreate):
    with SessionLocal() as db:
        db_log = LogEntry(**log.model_dump())
        db.add(db_log)
        db.commit()
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}
