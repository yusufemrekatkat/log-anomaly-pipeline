import os
from datetime import UTC, datetime

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@db:5432/log_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- MODELLER ---


class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, index=True)
    service_name = Column(String, index=True)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))


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
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))


class AnomalyEntry(Base):
    __tablename__ = "anomalies"
    id = Column(Integer, primary_key=True)
    detected_at = Column(DateTime, default=lambda: datetime.now(UTC))
    service_name = Column(String)
    anomaly_score = Column(Float)
    is_anomaly = Column(Boolean, default=True)
    alert_sent = Column(Boolean, default=False)


# Tabloları oluştur
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
