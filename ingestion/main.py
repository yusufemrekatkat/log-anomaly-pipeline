from datetime import datetime

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

# DB Ayarları
DATABASE_URL = "postgresql://admin:secret@localhost:5432/log_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# DB Modeli
class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime)
    service_name = Column(String)
    log_level = Column(String)
    message = Column(String)
    response_time_ms = Column(Float)
    ip = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


app = FastAPI()


# Pydantic Şeması (Validation)
class LogCreate(BaseModel):
    timestamp: datetime
    service_name: str
    log_level: str
    message: str
    response_time_ms: float
    ip: str


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/ingest")
def ingest_log(log: LogCreate, db: Session = Depends(get_db)):
    db_log = LogEntry(**log.model_dump())
    db.add(db_log)
    db.commit()
    return {"status": "success", "id": db_log.id}


@app.get("/health")
def health_check():
    return {"status": "healthy"}
