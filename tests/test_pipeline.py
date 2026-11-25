import json
import os
import time
from datetime import datetime, time as clock, timedelta, timezone
from uuid import uuid4

import boto3
import clickhouse_connect
import httpx
import psycopg
import pytest

MOVIE_URL = os.getenv("MOVIE_URL", "http://localhost:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://localhost:8001")


@pytest.fixture
def clickhouse():
    with clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        username=os.getenv("CLICKHOUSE_USER", "cinema"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "cinema-local"),
    ) as client:
        yield client


def event(**changes):
    payload = {
        "event_id": str(uuid4()),
        "user_id": str(uuid4()),
        "movie_id": "integration-movie",
        "event_type": "VIEW_FINISHED",
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        "device_type": "TABLET",
        "session_id": str(uuid4()),
        "progress_seconds": 406,
    }
    payload.update(changes)
    return payload


def publish(payload):
    response = httpx.post(f"{MOVIE_URL}/events", json=payload, timeout=15)
    assert response.status_code == 201, response.text
    assert response.json() == {"event_id": payload["event_id"]}


def ingested(client, ids):
    deadline = time.monotonic() + 30
    rows = []
    while time.monotonic() < deadline:
        rows = client.query(
            """
            SELECT toString(event_id), user_id, movie_id, event_type,
                   timestamp, device_type, session_id, progress_seconds
            FROM movie_events FINAL WHERE toString(event_id) IN {ids:Array(String)}
            """,
            parameters={"ids": ids},
        ).result_rows
        if len(rows) >= len(ids):
            return rows
        time.sleep(0.25)
    pytest.fail(f"expected {len(ids)} ingested events, saw {len(rows)}")


def test_event_pipeline(clickhouse):
    payload = event()
    publish(payload)
    row, = ingested(clickhouse, [payload["event_id"]])
    stored = dict(zip(payload, row))
    stored["timestamp"] = stored["timestamp"].replace(tzinfo=timezone.utc).isoformat(timespec="microseconds")
    assert stored == payload


@pytest.mark.parametrize(("field", "value"), [
    ("event_id", "invalid"),
    ("event_type", "UNKNOWN"),
    ("device_type", "UNKNOWN"),
    ("timestamp", "2025-02-30T12:00:00Z"),
    ("timestamp", "2025-01-01T12:00:00"),
    ("progress_seconds", -1),
    ("progress_seconds", 2147483648),
    ("progress_seconds", "12"),
    ("user_id", None),
])
def test_invalid_event(field, value):
    payload = event()
    if value is None:
        del payload[field]
    else:
        payload[field] = value
    response = httpx.post(f"{MOVIE_URL}/events", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]


def test_metrics_and_export(clickhouse):
    first = datetime.now(timezone.utc).date() - timedelta(days=40)
    occupied = {
        row[0] for row in clickhouse.query(
            "SELECT DISTINCT toDate32(timestamp) FROM movie_events"
        ).result_rows
    }
    while any(first + timedelta(days=offset) in occupied for offset in range(-1, 8)):
        first -= timedelta(days=8)

    user_a, user_b, visitor = (str(uuid4()) for _ in range(3))

    def make(user, kind, offset=0, progress=0, movie="movie-a", hour=12):
        timestamp = datetime.combine(first + timedelta(days=offset), clock(hour), timezone.utc)
        return event(user_id=user, event_type=kind, timestamp=timestamp.isoformat(),
                     progress_seconds=progress, movie_id=movie)

    start_a = make(user_a, "VIEW_STARTED")
    events = [
        start_a, start_a,
        make(user_a, "VIEW_FINISHED", progress=120, hour=13),
        make(user_b, "VIEW_STARTED", movie="movie-b"),
        make(user_b, "VIEW_FINISHED", progress=180, movie="movie-b", hour=13),
        make(user_b, "VIEW_STARTED", hour=14),
        make(visitor, "SEARCHED"),
        make(user_a, "LIKED", offset=1),
        make(user_b, "SEARCHED", offset=7),
    ]
    for payload in events:
        publish(payload)
    ids = sorted({item["event_id"] for item in events})
    assert len(ingested(clickhouse, ids)) == len(ids)

    expected_retention = [100.0, 50.0, 0.0, 0.0, 0.0, 0.0, 0.0, 50.0]
    params = {"date": first.isoformat()}

    for _ in range(2):
        response = httpx.post(f"{AGGREGATION_URL}/aggregate", params=params, timeout=60)
        assert response.status_code == 200, response.text
        assert response.json()["processed_records"] == 6

        daily, = clickhouse.query(
            """
            SELECT event_count, dau, average_watch_time, view_conversion
            FROM daily_metrics FINAL WHERE date = {day:Date32}
            """, parameters={"day": first},
        ).result_rows
        assert daily == pytest.approx((6, 3, 150, 200 / 3))

        top, = clickhouse.query(
            "SELECT movies FROM top_movies FINAL WHERE date = {day:Date32}",
            parameters={"day": first},
        ).result_rows
        assert top[0] == [("movie-a", 2), ("movie-b", 1)]

        cohort, = clickhouse.query(
            "SELECT cohort_size, retention FROM retention_cohorts FINAL WHERE date = {day:Date32}",
            parameters={"day": first},
        ).result_rows
        assert cohort == (2, expected_retention)

    with psycopg.connect(os.getenv(
        "POSTGRES_DSN", "postgresql://cinema:cinema-local@localhost:5432/cinema"
    )) as connection:
        rows = connection.execute(
            "SELECT metric_name, dimension, value FROM aggregates WHERE date = %s",
            (first,),
        ).fetchall()
    expected = {
        ("dau", ""): 3, ("average_watch_time", ""): 150, ("view_conversion", ""): 200 / 3,
        ("top_movies", "movie-a"): 2, ("top_movies", "movie-b"): 1,
        ("retention_d1", "1"): 50, ("retention_d7", "7"): 50,
    }
    expected.update({("retention", str(i)): value for i, value in enumerate(expected_retention)})
    assert len(rows) == len(expected)
    assert {(name, dimension): value for name, dimension, value in rows} == pytest.approx(expected)

    client = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "cinema"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "cinema-local"),
    )
    key = f"daily/{first}/aggregates.json"
    for _ in range(2):
        response = httpx.post(f"{AGGREGATION_URL}/export", params=params, timeout=30)
        assert response.status_code == 200, response.text
        assert response.json()["path"] == f"s3://movie-analytics/{key}"
        body = client.get_object(Bucket="movie-analytics", Key=key)["Body"]
        with body:
            exported = json.load(body)
        assert len(exported) == len(expected)
        assert all(item["date"] == first.isoformat() for item in exported)
        assert all(datetime.fromisoformat(item["calculated_at"]).utcoffset() is not None for item in exported)
        assert {
            (item["metric_name"], item["dimension"]): item["value"] for item in exported
        } == pytest.approx(expected)



    late = make(user_a, "VIEW_STARTED", offset=-1)
    publish(late)
    ingested(clickhouse, [late["event_id"]])
    response = httpx.post(
        f"{AGGREGATION_URL}/aggregate",
        params={"date": (first - timedelta(days=1)).isoformat()},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    cohort, = clickhouse.query(
        "SELECT cohort_size, retention FROM retention_cohorts FINAL WHERE date = {day:Date32}",
        parameters={"day": first},
    ).result_rows
    assert cohort == (1, [100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0])
