import logging
import os
import time
from concurrent.futures import Future

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "movie-events"
logger = logging.getLogger(__name__)


class Publisher:
    def __init__(self):
        self.registry = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"], "timeout": 3})
        schema = self.registry.get_latest_version(f"{TOPIC}-value").schema
        self.serializer = AvroSerializer(self.registry, schema, conf={"auto.register.schemas": False})
        self.producer = Producer({
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 6500,
            "retry.backoff.ms": 200,
            "retry.backoff.max.ms": 1200,
            "message.send.max.retries": 5,
        })

    def publish(self, event):
        data = event.model_dump(mode="json")
        data["timestamp"] = event.timestamp
        value = self.serializer(data, SerializationContext(TOPIC, MessageField.VALUE))
        delivered = Future()
        for attempt in range(3):
            try:
                self.producer.produce(
                    TOPIC, key=event.user_id.encode(), value=value,
                    on_delivery=lambda error, _: delivered.set_result(error),
                )
                break
            except BufferError:
                if attempt == 2:
                    raise
                self.producer.poll(0.2 * 2 ** attempt)
        deadline = time.monotonic() + 8
        while not delivered.done() and time.monotonic() < deadline:
            self.producer.poll(0.1)
        error = delivered.result() if delivered.done() else KafkaError(KafkaError._MSG_TIMED_OUT)
        if error is not None:
            raise KafkaException(error)
        logger.info("published event_id=%s event_type=%s timestamp=%s",
                    event.event_id, event.event_type, event.timestamp.isoformat())

    def healthy(self):
        metadata = self.producer.list_topics(TOPIC, timeout=2).topics[TOPIC]
        return metadata.error is None and all(partition.leader >= 0 for partition in metadata.partitions.values())
