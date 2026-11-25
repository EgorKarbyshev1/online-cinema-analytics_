import logging
import random
from datetime import datetime, time, timedelta, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

from models import DeviceType, EventType, MovieEvent

logger = logging.getLogger(__name__)


def session_events(rng, user, start, session_id, historical):
    movie = f"demo-movie-{rng.randrange(1, 21)}"
    device = rng.choice(list(DeviceType))
    sequence = [(EventType.SEARCHED, 0), (EventType.VIEW_STARTED, 0)]
    progress = rng.randint(60, 1200)
    if rng.random() < 0.65:
        sequence.extend([(EventType.VIEW_PAUSED, progress), (EventType.VIEW_RESUMED, progress)])
    if rng.random() < 0.75:
        progress += rng.randint(900, 6200)
        sequence.append((EventType.VIEW_FINISHED, progress))
    if rng.random() < 0.35:
        sequence.append((EventType.LIKED, progress if len(sequence) > 2 else 0))

    elapsed = 0
    previous = 0
    for index, (kind, progress) in enumerate(sequence):
        elapsed += max(1, progress - previous)
        previous = progress
        yield MovieEvent(
            event_id=uuid5(NAMESPACE_URL, f"{session_id}/{index}") if historical else uuid4(),
            user_id=f"demo-user-{user}", movie_id=movie, event_type=kind,
            timestamp=start + timedelta(seconds=elapsed), device_type=device,
            session_id=session_id, progress_seconds=progress,
        )


def generate(publisher, stop, days, rate):
    rng = random.Random(42)
    today = datetime.now(timezone.utc).date()

    def publish(event):
        try:
            publisher.publish(event)
        except Exception:
            logger.exception("generator delivery failed event_id=%s", event.event_id)
            return False
        return True

    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        daily = random.Random(f"42/{day}")
        for user in range(1, 61):
            if daily.random() >= 0.24:
                continue
            start = datetime.combine(day, time(12), timezone.utc) + timedelta(minutes=user)
            for event in session_events(daily, user, start, f"demo/{day}/{user}", True):
                while not stop.is_set() and not publish(event):
                    stop.wait(2)
                if stop.is_set():
                    return

    while not stop.is_set():
        start = datetime.now(timezone.utc) - timedelta(hours=3)
        for event in session_events(rng, rng.randint(1, 60), start, str(uuid4()), False):
            publish(event)
            if stop.wait(1 / rate):
                return
