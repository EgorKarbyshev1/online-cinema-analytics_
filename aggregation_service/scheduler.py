import logging
import time
from datetime import datetime, timedelta, timezone

from aggregation import aggregate
from database import lock_cycle, postgres
from export import ensure_bucket, export_date, storage

logger = logging.getLogger(__name__)


def utc_today():
    return datetime.now(timezone.utc).date()


def run_cycle():
    today = utc_today()
    yesterday = today - timedelta(days=1)
    with postgres() as connection:
        lock_cycle(connection)
        with connection.transaction():
            connection.execute(
                """
                INSERT INTO daily_exports (date, scheduled)
                SELECT first_date + days.day_offset, true
                FROM (
                    SELECT least(coalesce(max(date) FILTER (WHERE scheduled) + 1, %(yesterday)s), %(yesterday)s) AS first_date
                    FROM daily_exports
                ) AS dates
                CROSS JOIN LATERAL generate_series(0, %(yesterday)s - first_date) AS days(day_offset)
                ON CONFLICT (date) DO UPDATE SET scheduled = true
                WHERE NOT daily_exports.scheduled
                """,
                {"yesterday": yesterday},
            )
            pending = {row[0] for row in connection.execute(
                "SELECT date FROM daily_exports WHERE exported_at IS NULL"
            )}

        window = {today - timedelta(days=offset) for offset in range(15)}
        aggregate(connection, sorted(window | pending), today)
        client = storage()
        ensure_bucket(client)
        with connection.transaction():
            pending = [
                row[0] for row in connection.execute(
                    "SELECT date FROM daily_exports WHERE exported_at IS NULL ORDER BY date"
                )
            ]
        for day in pending:
            try:
                export_date(connection, client, day)
            except Exception:
                logger.exception("scheduled export failed date=%s", day)


def schedule(stop, interval):
    while not stop.is_set():
        started = time.monotonic()
        try:
            run_cycle()
        except Exception:
            logger.exception("scheduled cycle failed date=%s duration=%.3fs",
                             utc_today(), time.monotonic() - started)
        stop.wait(interval)
