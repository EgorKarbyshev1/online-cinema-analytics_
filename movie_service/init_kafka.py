import logging
import os
import time
from pathlib import Path

import httpx
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic

from producer import TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def registry_request(registry, method, path, payload):
    for attempt in range(4):
        try:
            return registry.request(method, path, json=payload).raise_for_status()
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if attempt == 3 or isinstance(error, httpx.HTTPStatusError) and error.response.status_code < 500:
                raise
            logger.warning("registry request failed path=%s attempt=%s", path, attempt + 1)
            time.sleep(0.5 * 2 ** attempt)


def initialize():
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=2, config={"min.insync.replicas": "1"})
    try:
        admin.create_topics([topic], request_timeout=30)[TOPIC].result()
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC, incremental_configs=[
        ConfigEntry("min.insync.replicas", "1", incremental_operation=AlterConfigOpType.SET),
    ])
    admin.incremental_alter_configs([resource], request_timeout=30)[resource].result()
    partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    if len(partitions) != 3 or any(len(partition.replicas) != 2 for partition in partitions.values()):
        raise RuntimeError("movie-events must have three partitions and two replicas per partition")

    subject = f"{TOPIC}-value"
    media_type = "application/vnd.schemaregistry.v1+json"
    with httpx.Client(
        base_url=os.environ["SCHEMA_REGISTRY_URL"], timeout=10,
        headers={"Content-Type": media_type, "Accept": media_type},
    ) as registry:
        registry_request(registry, "PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        response = registry_request(registry, "POST", f"/subjects/{subject}/versions", {
            "schema": Path("/schema/movie_event.avsc").read_text(encoding="utf-8"),
        })
        logger.info("topic=%s partitions=%s subject=%s schema_id=%s",
                    TOPIC, len(partitions), subject, response.json()["id"])


if __name__ == "__main__":
    initialize()
