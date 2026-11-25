import logging
import os
from contextlib import asynccontextmanager
from datetime import date
from threading import Event, Thread

from fastapi import FastAPI, HTTPException

from aggregation import aggregate
from database import clickhouse, lock_cycle, postgres
from export import BUCKET, ensure_bucket, export_date, storage
from scheduler import schedule, utc_today

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    interval = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "30"))
    if interval < 1:
        raise ValueError("AGGREGATION_INTERVAL_SECONDS must be positive")
    stop = Event()
    scheduler = Thread(target=schedule, args=(stop, interval), daemon=True)
    scheduler.start()
    yield
    stop.set()
    scheduler.join(timeout=10)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    try:
        with clickhouse() as client:
            client.command("SELECT 1")
        with postgres() as connection:
            connection.execute("SELECT 1 FROM aggregates LIMIT 1")
        storage().head_bucket(Bucket=BUCKET)
    except Exception:
        logger.exception("aggregation service health check failed")
        raise HTTPException(503, "A dependency is unavailable")
    return {"status": "ok"}


@app.post("/aggregate")
def aggregate_date(date: date):
    today = utc_today()
    if date > today or date.year < 1900:
        raise HTTPException(422, "Date must be between 1900-01-01 and today in UTC")
    try:
        with postgres() as connection:
            lock_cycle(connection)
            processed = aggregate(connection, [date], today)
    except Exception:
        logger.exception("manual aggregation failed date=%s", date)
        raise HTTPException(503, "Aggregation failed")
    return {"date": date, "processed_records": processed[date]}


@app.post("/export")
def export(date: date):
    try:
        with postgres() as connection:
            lock_cycle(connection)
            with connection.transaction():
                queued = connection.execute(
                    """
                    INSERT INTO daily_exports (date)
                    SELECT date FROM aggregates WHERE date = %s AND metric_name = 'dau'
                    ON CONFLICT (date) DO UPDATE SET exported_at = NULL
                    """,
                    (date,),
                ).rowcount
            if queued:
                client = storage()
                ensure_bucket(client)
                path = export_date(connection, client, date)
            else:
                path = None
    except Exception:
        logger.exception("manual export failed date=%s", date)
        raise HTTPException(503, "Export failed")
    if path is None:
        raise HTTPException(404, "Daily aggregates were not found")
    return {"date": date, "path": path}
