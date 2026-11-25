import logging
import os
import time

import clickhouse_connect
import psycopg

logger = logging.getLogger(__name__)


def clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=3,
        send_receive_timeout=20,
    )


def postgres():
    for attempt in range(4):
        try:
            return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=3)
        except psycopg.OperationalError:
            if attempt == 3:
                raise
            logger.warning("PostgreSQL connection failed attempt=%s", attempt + 1)
            time.sleep(0.25 * 2 ** attempt)


def lock_cycle(connection):
    connection.execute("SELECT pg_advisory_lock(502025)")
    connection.commit()


def save_metrics(connection, rows, movies):
    with connection.transaction(), connection.cursor() as cursor:
        for day, top in movies.items():
            cursor.execute(
                """
                DELETE FROM aggregates
                WHERE date = %s AND metric_name = 'top_movies' AND dimension != ALL(%s)
                """,
                (day, [movie for movie, _ in top]),
            )
            if cursor.rowcount:
                cursor.execute("UPDATE daily_exports SET exported_at = NULL WHERE date = %s", (day,))

        cursor.executemany(
            """
            INSERT INTO aggregates (date, metric_name, dimension, value, calculated_at)
            VALUES (%s, %s, %s, %s, transaction_timestamp())
            ON CONFLICT (date, metric_name, dimension)
            DO UPDATE SET value = EXCLUDED.value, calculated_at = EXCLUDED.calculated_at
            WHERE aggregates.value IS DISTINCT FROM EXCLUDED.value
            """,
            rows,
        )
        cursor.execute(
            """
            UPDATE daily_exports SET exported_at = NULL
            WHERE exported_at IS NOT NULL AND EXISTS (
                SELECT 1 FROM aggregates
                WHERE aggregates.date = daily_exports.date AND calculated_at = transaction_timestamp()
            )
            """
        )
