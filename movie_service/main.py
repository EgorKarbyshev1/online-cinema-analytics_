import logging
import math
import os
from contextlib import asynccontextmanager
from threading import Event, Thread

from fastapi import FastAPI, HTTPException

from generator import generate
from models import MovieEvent
from producer import Publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app):
    app.state.publisher = Publisher()
    stop = Event()
    generator = None
    if os.getenv("GENERATOR_ENABLED", "true").lower() == "true":
        days = int(os.getenv("GENERATOR_HISTORY_DAYS", "14"))
        rate = float(os.getenv("GENERATOR_EVENTS_PER_SECOND", "2"))
        if days < 0 or not math.isfinite(rate) or rate <= 0:
            raise ValueError("Generator history days and event rate are out of range")
        generator = Thread(target=generate, args=(app.state.publisher, stop, days, rate), daemon=True)
        generator.start()
    yield
    stop.set()
    if generator:
        generator.join(timeout=10)
    app.state.publisher.producer.flush(2)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    try:
        if app.state.publisher.healthy():
            return {"status": "ok"}
    except Exception:
        logger.exception("Kafka health check failed")
    raise HTTPException(503, "Kafka is unavailable")


@app.post("/events", status_code=201)
def publish(event: MovieEvent):
    try:
        app.state.publisher.publish(event)
    except Exception:
        logger.exception("delivery failed event_id=%s event_type=%s timestamp=%s",
                         event.event_id, event.event_type, event.timestamp)
        raise HTTPException(503, "Event delivery could not be confirmed")
    return {"event_id": str(event.event_id)}
