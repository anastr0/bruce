"""
Outbox publisher:
- Polls Postgres outbox_events table
- Publishes pending events to Kafka
- Marks rows as published on success (or re-queues with error on failure)
"""

import json
import logging
import os
import time

import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from kafka.errors import KafkaError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _get_db_conn():
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bruce"),
        user=os.getenv("POSTGRES_USER", "bruce"),
        password=os.getenv("POSTGRES_PASSWORD", "bruce"),
    )


def _get_producer() -> KafkaProducer:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")
    brokers = [b.strip() for b in brokers if b.strip()]
    return KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        metadata_max_age_ms=300000,
    )


def _claim_batch(conn, batch_size: int):
    """
    Atomically claim a batch of pending rows by switching them to 'processing'.
    Uses SKIP LOCKED so multiple publishers can run concurrently.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH cte AS (
              SELECT id
              FROM outbox_events
              WHERE status = 'pending'
                AND dispatcher_delivered_at IS NULL
              ORDER BY id
              LIMIT %s
              FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox_events o
            SET status = 'processing'
            FROM cte
            WHERE o.id = cte.id
            RETURNING o.id, o.aggregate_id, o.event_type, o.payload;
            """,
            (batch_size,),
        )
        rows = cur.fetchall()
    conn.commit()
    return rows


def _mark_published(conn, row_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox_events
            SET status='published', published_at=now()
            WHERE id=%s;
            """,
            (row_id,),
        )
    conn.commit()


def _mark_failed(conn, row_id: int, err: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox_events
            SET status='pending', attempt_count=attempt_count+1, last_error=%s
            WHERE id=%s;
            """,
            (err[:2000], row_id),
        )
    conn.commit()


def main():
    """
    Polls Postgres outbox_events table
    Publishes pending events to Kafka
    Marks rows as published on success (or re-queues with error on failure)

    TODO: Add a health check endpoint to verify Kafka and Postgres connectivity, then only try producing to Kafka if both are healthy
    """
    topic = os.getenv("KAFKA_TOPIC", "payment-webhooks")
    poll_interval = float(os.getenv("OUTBOX_POLL_INTERVAL_SEC", "5"))
    batch_size = int(os.getenv("OUTBOX_BATCH_SIZE", "25"))

    producer = _get_producer()
    conn = _get_db_conn()
    conn.autocommit = False

    logger.info("Outbox publisher started topic=%s batch=%d", topic, batch_size)

    try:
        while True:
            rows = _claim_batch(conn, batch_size)
            if not rows:
                time.sleep(poll_interval)
                continue

            for row in rows:
                row_id = row["id"]
                key = row["aggregate_id"]
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                try:
                    future = producer.send(topic, value=payload, key=key)
                    future.get(timeout=30)
                    _mark_published(conn, row_id)
                    logger.info("Published outbox id=%s key=%s", row_id, key)
                except KafkaError as e:
                    _mark_failed(conn, row_id, str(e))
                    logger.warning("Kafka publish failed outbox id=%s err=%s", row_id, e)
                except Exception as e:
                    _mark_failed(conn, row_id, str(e))
                    logger.warning("Publish failed outbox id=%s err=%s", row_id, e)
    finally:
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

