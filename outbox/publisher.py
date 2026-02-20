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
from typing import Optional

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


_producer: Optional[KafkaProducer] = None


def _get_producer() -> Optional[KafkaProducer]:
    """Get or create Kafka producer. Returns None if connection cannot be established."""
    global _producer
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092").split(",")
    brokers = [b.strip() for b in brokers if b.strip()]
    
    # If producer exists, check if it's still connected
    if _producer is not None:
        try:
            # Try to get metadata to verify connection
            # _producer.list_topics(timeout=5)
            return _producer
        except Exception:
            # Connection lost, reset producer
            try:
                _producer.close()
            except Exception:
                pass
            _producer = None
    
    # Create new producer
    try:
        _producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
            retries=2,
            max_block_ms=500,
            reconnect_backoff_ms=5000,
            request_timeout_ms=1000,
            metadata_max_age_ms=300000,
        )
        # Verify connection by listing topics (with timeout to avoid hanging)
        try:
            # _producer.list_topics(timeout=5)
            logger.info("Kafka producer connected successfully")
        except Exception:
            # Connection verification failed, but producer might still work
            # Close it and return None to be safe
            try:
                _producer.close()
            except Exception:
                pass
            _producer = None
            logger.error("Kafka connection verification failed")
            return None
        return _producer
    except Exception as e:
        # Kafka brokers unavailable, ignore and return None
        logger.error(f"Kafka connection cannot be established: {str(e)}")
        _producer = None
        return None


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
        logger.info(f"Claimed batch of {len(rows)} rows")
        for row in rows:
            logger.info(f"Claimed row: {row}")
    conn.commit()
    return rows


def _mark_published(conn, row_id: int):
    logger.error(f"\n\nMarking row {row_id} as published\n\n")
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
    If Kafka connection cannot be established, ignores and retries after poll interval
    """
    global _producer
    
    topic = os.getenv("KAFKA_TOPIC", "payment-webhooks")
    poll_interval = float(os.getenv("OUTBOX_POLL_INTERVAL_SEC", "30"))
    batch_size = int(os.getenv("OUTBOX_BATCH_SIZE", "25"))

    conn = _get_db_conn()
    conn.autocommit = False

    logger.info("Outbox publisher started topic=%s batch=%d poll_interval=%.1fs", topic, batch_size, poll_interval)

    try:
        while True:
            # Try to get producer, if connection fails, wait and retry
            producer = _get_producer()
            if producer is None:
                logger.warning("Kafka connection unavailable, waiting %.1fs before retry", poll_interval)
                time.sleep(poll_interval)
                continue

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
                
                # Re-check producer before each publish in case connection was lost
                producer = _get_producer()
                if producer is None:
                    logger.warning("Kafka connection lost during batch processing, re-queuing remaining rows")
                    # Re-queue all remaining rows in this batch
                    for remaining_row in rows[rows.index(row):]:
                        _mark_failed(conn, remaining_row["id"], "Kafka connection unavailable")
                    break
                
                try:
                    logger.info(f"\n\nSending payment event to Kafka topic: {topic}")
                    logger.info(f"Key: {key}")
                    logger.info(f"Payload: {payload}\n\n")
                    future = producer.send(topic, value=payload, key=key)
                    future.get(timeout=30)
                    _mark_published(conn, row_id)
                    logger.info("Published outbox id=%s key=%s", row_id, key)
                except KafkaError as e:
                    _mark_failed(conn, row_id, str(e))
                    logger.warning("Kafka publish failed outbox id=%s err=%s", row_id, e)
                    # Connection might be lost, reset producer
                    try:
                        if _producer:
                            _producer.close()
                    except Exception:
                        pass
                    _producer = None
                except Exception as e:
                    _mark_failed(conn, row_id, str(e))
                    logger.warning("Publish failed outbox id=%s err=%s", row_id, e)
    finally:
        try:
            if _producer:
                _producer.flush(timeout=10)
                _producer.close(timeout=10)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

