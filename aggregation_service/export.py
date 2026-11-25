import json
import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = "movie-analytics"
logger = logging.getLogger(__name__)


def storage():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
        config=Config(connect_timeout=2, read_timeout=4, retries={"total_max_attempts": 2},
                      s3={"addressing_style": "path"}),
    )


def ensure_bucket(client):
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        client.create_bucket(Bucket=BUCKET)


def export_date(connection, client, day):
    with connection.transaction():
        rows = connection.execute(
            """
            SELECT date, metric_name, dimension, value, calculated_at
            FROM aggregates WHERE date = %s
            ORDER BY metric_name, dimension
            """,
            (day,),
        ).fetchall()
    if not any(row[1] == "dau" for row in rows):
        return None

    payload = [
        {
            "date": metric_date.isoformat(), "metric_name": name,
            "dimension": dimension, "value": value,
            "calculated_at": calculated_at.isoformat(),
        }
        for metric_date, name, dimension, value, calculated_at in rows
    ]
    key = f"daily/{day}/aggregates.json"
    client.put_object(
        Bucket=BUCKET, Key=key, ContentType="application/json",
        Body=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode(),
    )
    with connection.transaction():
        connection.execute(
            """
            UPDATE daily_exports SET exported_at = now()
            WHERE date = %s
            """,
            (day,),
        )
    path = f"s3://{BUCKET}/{key}"
    logger.info("export succeeded date=%s records=%s path=%s", day, len(rows), path)
    return path
