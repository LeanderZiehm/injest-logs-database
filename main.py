import os
import logging
import asyncio
import time
from datetime import datetime

from fastapi import FastAPI, HTTPException
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Text,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------------
REQUIRED_ENVS = [
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]

missing = [e for e in REQUIRED_ENVS if not os.environ.get(e)]
if missing:
    logger.warning(f"Missing environment variables: {missing}")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "logs")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# -------------------------------------------------------------------
# Database setup
# -------------------------------------------------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class NginxLog(Base):
    __tablename__ = "nginx_logs"

    id = Column(Integer, primary_key=True)
    remote_addr = Column(String)
    method = Column(String)
    path = Column(String)
    status_code = Column(Integer)
    raw = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class SSHLog(Base):
    __tablename__ = "ssh_logs"

    id = Column(Integer, primary_key=True)
    user = Column(String)
    ip_address = Column(String)
    action = Column(String)
    raw = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized")
    except OperationalError as e:
        logger.error(f"Database connection failed: {e}")


# -------------------------------------------------------------------
# Log parsers (intentionally simple)
# -------------------------------------------------------------------
def parse_nginx_log_line(line: str):
    try:
        parts = line.split()
        return {
            "remote_addr": parts[0],
            "method": parts[5].replace('"', ""),
            "path": parts[6],
            "status_code": int(parts[8]),
            "raw": line.strip(),
        }
    except Exception:
        return None


def parse_ssh_log_line(line: str):
    try:
        action = "unknown"
        if "Accepted" in line:
            action = "accepted"
        elif "Failed" in line:
            action = "failed"

        ip_address = next((t for t in line.split() if t.count(".") == 3), None)

        user = None
        if "for" in line.split():
            idx = line.split().index("for")
            user = line.split()[idx + 1]

        return {
            "user": user,
            "ip_address": ip_address,
            "action": action,
            "raw": line.strip(),
        }
    except Exception:
        return None


# -------------------------------------------------------------------
# Ingestion logic
# -------------------------------------------------------------------
def ingest_nginx_logs(path="/var/log/nginx/access.log"):
    if not os.path.exists(path):
        logger.warning(f"Nginx log not found: {path}")
        return

    db = SessionLocal()
    with open(path, "r") as f:
        for line in f:
            parsed = parse_nginx_log_line(line)
            if parsed:
                db.add(NginxLog(**parsed))
    db.commit()
    db.close()


def ingest_ssh_logs(path="/var/log/auth.log"):
    if not os.path.exists(path):
        logger.warning(f"SSH log not found: {path}")
        return

    db = SessionLocal()
    with open(path, "r") as f:
        for line in f:
            parsed = parse_ssh_log_line(line)
            if parsed:
                db.add(SSHLog(**parsed))
    db.commit()
    db.close()


def run_ingestion():
    logger.info("Starting log ingestion")
    ingest_nginx_logs()
    ingest_ssh_logs()
    logger.info("Log ingestion finished")


# -------------------------------------------------------------------
# Scheduler + cooldown
# -------------------------------------------------------------------
INGEST_INTERVAL_SECONDS = 60 * 60 * 24  # 24 hours
MANUAL_COOLDOWN_SECONDS = 60

_last_manual_run = 0.0
_ingest_lock = asyncio.Lock()


async def ingestion_loop():
    while True:
        await asyncio.sleep(INGEST_INTERVAL_SECONDS)
        async with _ingest_lock:
            run_ingestion()


# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI(title="VPS Log Collector")


@app.on_event("startup")
async def startup():
    init_db()
    run_ingestion()  # initial run on boot
    asyncio.create_task(ingestion_loop())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def manual_ingest():
    global _last_manual_run

    now = time.time()
    if now - _last_manual_run < MANUAL_COOLDOWN_SECONDS:
        raise HTTPException(
            status_code=429,
            detail="Ingest cooldown active (60s)",
        )

    if _ingest_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="Ingest already running",
        )

    async with _ingest_lock:
        _last_manual_run = now
        run_ingestion()

    return {"status": "ingestion triggered"}


@app.get("/nginx/count")
def nginx_count():
    db = SessionLocal()
    count = db.query(NginxLog).count()
    db.close()
    return {"count": count}


@app.get("/ssh/count")
def ssh_count():
    db = SessionLocal()
    count = db.query(SSHLog).count()
    db.close()
    return {"count": count}
