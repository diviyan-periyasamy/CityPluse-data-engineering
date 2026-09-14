"""Kafka producer/consumer helpers for the weather_raw topic."""
import json
import logging

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

TOPIC = "weather_raw"


def get_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        acks="all",
    )


def publish_reading(producer: KafkaProducer, reading: dict) -> None:
    try:
        future = producer.send(TOPIC, key=reading["city_name"], value=reading)
        future.get(timeout=10)  # block briefly so delivery errors surface here
        logger.info("Published reading for %s", reading["city_name"])
    except KafkaError:
        logger.exception("Failed to publish reading for %s", reading.get("city_name"))
        raise


def get_consumer(bootstrap_servers: str, group_id: str = "staging-loader") -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # we commit manually, only after a successful DB write
        group_id=group_id,
        consumer_timeout_ms=10000,  # stop iterating after 10s of no new messages
    )
