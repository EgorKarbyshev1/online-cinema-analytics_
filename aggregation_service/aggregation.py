import logging
import time
from collections import defaultdict
from datetime import timedelta

from database import clickhouse, save_metrics

logger = logging.getLogger(__name__)

DAILY_SQL = """
SELECT toDate32(timestamp) AS day,
       count(),
       uniqExact(user_id),
       if(countIf(event_type = 'VIEW_FINISHED') = 0, NULL,
           avgIf(progress_seconds, event_type = 'VIEW_FINISHED')),
       coalesce(100.0 * countIf(event_type = 'VIEW_FINISHED')
           / nullIf(countIf(event_type = 'VIEW_STARTED'), 0), 0)
FROM movie_events FINAL
WHERE toDate32(timestamp) IN {days:Array(Date32)}
GROUP BY day
"""

TOP_SQL = """
SELECT toDate32(timestamp) AS day, movie_id, count() AS starts
FROM movie_events FINAL
WHERE toDate32(timestamp) IN {days:Array(Date32)} AND event_type = 'VIEW_STARTED'
GROUP BY day, movie_id
ORDER BY day, starts DESC, movie_id
LIMIT 10 BY day
"""

RETENTION_SQL = """
WITH activity AS
(
    SELECT user_id, toDate32(timestamp) AS active_day,
           countIf(event_type = 'VIEW_STARTED') AS starts
    FROM movie_events FINAL
    WHERE toDate32(timestamp) <= {today:Date32}
    GROUP BY user_id, active_day
),
first_views AS
(
    SELECT user_id, min(active_day) AS cohort
    FROM activity WHERE starts > 0
    GROUP BY user_id
),
visits AS
(
    SELECT cohort, activity.user_id AS member, groupUniqArray(
        dateDiff('day', cohort, active_day)
    ) AS ages
    FROM activity INNER JOIN first_views ON activity.user_id = first_views.user_id
    WHERE active_day BETWEEN cohort AND addDays(cohort, 7)
    GROUP BY cohort, member
)
SELECT cohort, max(cohort_size),
       arrayMap(item -> item.2, arraySort(groupArray((age, percentage))))
FROM
(
    SELECT cohort, arrayJoin(range(8)) AS age, count() AS cohort_size,
           100.0 * sum(has(ages, toInt64(age))) / count() AS percentage
    FROM visits
    GROUP BY cohort, age
)
GROUP BY cohort
"""


def aggregate(connection, dates, today):
    started = time.monotonic()
    processed = 0
    logger.info("aggregation started dates=%s records=0 duration=0", dates)
    try:
        with clickhouse() as client:
            parameters = {"days": dates}
            daily = {
                day: (count, dau, average, conversion)
                for day, count, dau, average, conversion in client.query(
                    DAILY_SQL, parameters=parameters
                ).result_rows
            }
            processed = sum(values[0] for values in daily.values())
            movies = defaultdict(list)
            for day, movie, starts in client.query(TOP_SQL, parameters=parameters).result_rows:
                movies[day].append((movie, starts))
            cohorts = {
                day: (size, [
                    value if day + timedelta(days=age) <= today else None
                    for age, value in enumerate(values)
                ])
                for day, size, values in client.query(
                    RETENTION_SQL, parameters={"today": today}
                ).result_rows
            }
            existing = client.query(
                "SELECT date FROM retention_cohorts FINAL WHERE cohort_size > 0"
            ).result_rows
            retention_dates = sorted(set(cohorts) | set(dates) | {row[0] for row in existing})
            rows = []
            daily_rows = []
            for day in dates:
                count, dau, average, conversion = daily.get(day, (0, 0, None, 0.0))
                daily_rows.append((day, count, dau, average, conversion))
                rows.extend([
                    (day, "dau", "", dau),
                    (day, "average_watch_time", "", average),
                    (day, "view_conversion", "", conversion),
                ])
                rows.extend((day, "top_movies", movie, starts) for movie, starts in movies[day])

            retention_rows = []
            for day in retention_dates:
                size, values = cohorts.get(day, (0, [None] * 8))
                retention_rows.append((day, size, values))
                rows.extend((day, "retention", str(age), value) for age, value in enumerate(values))
                rows.extend((day, f"retention_d{age}", str(age), values[age]) for age in (1, 7))

            client.insert("daily_metrics", daily_rows)
            client.insert("top_movies", [(day, movies[day]) for day in dates])
            client.insert("retention_cohorts", retention_rows)
        save_metrics(connection, rows, movies)
    except Exception:
        logger.exception("aggregation failed dates=%s records=%s duration=%.3fs",
                         dates, processed, time.monotonic() - started)
        raise
    logger.info("aggregation succeeded dates=%s records=%s duration=%.3fs",
                dates, processed, time.monotonic() - started)
    return {row[0]: row[1] for row in daily_rows}
