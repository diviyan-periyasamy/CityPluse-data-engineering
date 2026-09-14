"""Shared logic: drain the weather_raw Kafka topic into staging.weather_raw."""
import logging

import psycopg2
import psycopg2.extras

from common.kafka_utils import get_consumer

logger = logging.getLogger(__name__)


def load_to_staging(bootstrap_servers: str, pg_conn: dict, group_id: str = "staging-loader") -> int:
    """Consume any new messages from 'weather_raw' and insert them into
    staging.weather_raw. Idempotent: the (kafka_partition, kafka_offset)
    unique constraint means re-running never creates duplicate rows, and
    the Kafka offset is only committed after the DB write succeeds.
    Returns the number of rows inserted.
    """
    consumer = get_consumer(bootstrap_servers, group_id=group_id)
    conn = psycopg2.connect(**pg_conn)
    conn.autocommit = False
    cur = conn.cursor()
    inserted = 0
    try:
        for msg in consumer:
            try:
                cur.execute(
                    """
                    INSERT INTO staging.weather_raw
                        (city_name, raw_payload, kafka_partition, kafka_offset)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
                    """,
                    (msg.value["city_name"], psycopg2.extras.Json(msg.value), msg.partition, msg.offset),
                )
                conn.commit()
                consumer.commit()
                inserted += 1
            except Exception:
                conn.rollback()
                logger.exception("Failed to insert message at offset %s", msg.offset)
    finally:
        cur.close()
        conn.close()
        consumer.close()
    logger.info("Inserted %s new row(s) into staging.weather_raw", inserted)
    return inserted
