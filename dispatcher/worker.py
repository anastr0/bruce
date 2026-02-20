"""
Kafka consumer worker that consumes messages from a topic and delivers them
to a webhook listener API URL via HTTP POST with HMAC signature.
"""

import json
import hmac
import hashlib
import logging
import os
import signal
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Defaults (override via env)
DEFAULT_TOPIC = "payment-webhooks"
DEFAULT_GROUP_ID = "webhook-dispatcher"
MAX_RETRIES = 5
RETRY_BACKOFF_SEC = 2
REQUEST_TIMEOUT_SEC = 3
RETRY_DELAY_MINUTES = 5  # Wait 5 minutes before retrying after max retries exhausted

_shutdown = False
_producer: Optional[KafkaProducer] = None


def _get_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    """Get or create Kafka producer for re-queuing messages."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            metadata_max_age_ms=300000,
        )
    return _producer


def _requeue_webhook(
    producer: KafkaProducer,
    topic: str,
    payload: dict,
) -> None:
    """Re-queue failed webhook back to Kafka topic."""
    try:
        future = producer.send(topic, value=payload)
        future.get(timeout=10)  # Wait for send to complete
        logger.info(
            "Re-queued failed webhook to Kafka tid=%s topic=%s",
            payload.get("Transaction ID", "?"),
            topic,
        )
    except Exception as e:
        logger.error("Failed to re-queue message to Kafka: %s", e)


def _generate_signature(payload: dict, secret: str) -> str:
    """Generate HMAC-SHA256 signature for the payload (matches ingestion API)."""
    payload_str = json.dumps(payload, sort_keys=True)
    payload_bytes = payload_str.encode("utf-8")
    secret_bytes = secret.encode("utf-8")
    return hmac.new(
        secret_bytes,
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()


def _deliver_to_webhook(webhook_url: str, payload: dict, hmac_secret: str) -> bool:
    """POST payload to webhook URL with X-HMAC-Signature header. Returns True on success."""
    transaction_id = payload.get("Transaction ID", "?")
    signature = _generate_signature(payload, hmac_secret)
    headers = {
        "Content-Type": "application/json",
        "X-HMAC-Signature": signature,
    }
    try:
        resp = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        logger.info(
            "✓ WEBHOOK DELIVERED SUCCESSFULLY tid=%s url=%s status=%s",
            transaction_id,
            webhook_url,
            resp.status_code,
        )
        return True
    except requests.RequestException as e:
        logger.error("Webhook delivery failed: %s", e)
        return False


def _consume_and_deliver(
    consumer: KafkaConsumer,
    webhook_url: str,
    hmac_secret: str,
    topic: str,
    producer: KafkaProducer,
) -> None:
    global _shutdown
    for message in consumer:
        if _shutdown:
            break
        try:
            payload = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error("Invalid message (skip): %s", e)
            continue
        
        transaction_id = payload.get("Transaction ID", "?")
        logger.info("Processing webhook delivery tid=%s", transaction_id)
        
        delivered = False
        for attempt in range(1, MAX_RETRIES + 1):
            if _deliver_to_webhook(webhook_url, payload, hmac_secret):
                delivered = True
                logger.info(
                    "✓ WEBHOOK DELIVERY COMPLETED tid=%s attempts=%d/%d",
                    transaction_id,
                    attempt,
                    MAX_RETRIES,
                )
                break
            if attempt < MAX_RETRIES:
                # Binary exponential backoff: base * 2^(attempt-1)
                backoff_delay = RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                logger.info(
                    "Retrying webhook delivery tid=%s attempt=%d/%d after %.1fs backoff (exponential)",
                    transaction_id,
                    attempt + 1,
                    MAX_RETRIES,
                    backoff_delay,
                )
                time.sleep(backoff_delay)
        
        if not delivered:
            logger.error(
                "✗ WEBHOOK DELIVERY FAILED PERMANENTLY tid=%s url=%s attempts=%d - RE-QUEUING AND PAUSING CONSUMER",
                transaction_id,
                webhook_url,
                MAX_RETRIES,
            )
            
            # Re-queue the failed webhook back to Kafka
            _requeue_webhook(producer, topic, payload)
            
            # Pause consumer to stop processing new messages
            try:
                # Get all assigned partitions and pause them
                # Note: assignment() returns the partitions assigned to this consumer
                partitions = list(consumer.assignment())
                if partitions:
                    consumer.pause(*partitions)
                    logger.info(
                        "Consumer paused (%d partitions). Waiting %d minutes before resuming. tid=%s",
                        len(partitions),
                        RETRY_DELAY_MINUTES,
                        transaction_id,
                    )
                    
                    # Wait for the specified delay (checking for shutdown periodically)
                    # delay_seconds = RETRY_DELAY_MINUTES * 60
                    delay_seconds = 10
                    elapsed = 0
                    check_interval = 10  # Check every 10 seconds
                    while elapsed < delay_seconds and not _shutdown:
                        sleep_time = min(check_interval, delay_seconds - elapsed)
                        time.sleep(sleep_time)
                        elapsed += sleep_time
                    
                    if not _shutdown:
                        # Resume consumer
                        consumer.resume(*partitions)
                        logger.info(
                            "Consumer resumed after %d minute delay. tid=%s",
                            RETRY_DELAY_MINUTES,
                            transaction_id,
                        )
                    else:
                        logger.info("Shutdown requested during pause, resuming consumer before exit")
                        consumer.resume(*partitions)
                else:
                    logger.warning("No partitions assigned to consumer, cannot pause/resume")
            except Exception as e:
                logger.error("Error pausing/resuming consumer: %s", e)
                # Try to resume if pause succeeded but resume failed
                try:
                    partitions = list(consumer.assignment())
                    if partitions:
                        consumer.resume(*partitions)
                except Exception:
                    pass


def main() -> None:
    global _shutdown

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    bootstrap_list = [b.strip() for b in bootstrap.split(",") if b.strip()]
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    group_id = os.getenv("KAFKA_CONSUMER_GROUP", DEFAULT_GROUP_ID)
    webhook_url = os.getenv("WEBHOOK_URL")
    hmac_secret = os.getenv("HMAC_SECRET")

    if not webhook_url:
        logger.error("WEBHOOK_URL environment variable is required")
        sys.exit(1)
    if not hmac_secret:
        logger.error("HMAC_SECRET environment variable is required")
        sys.exit(1)

    def on_signal(_signum, _frame):
        global _shutdown
        _shutdown = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    logger.info(
        "Starting worker bootstrap=%s topic=%s group=%s webhook=%s retry_delay_minutes=%d",
        bootstrap,
        topic,
        group_id,
        webhook_url,
        RETRY_DELAY_MINUTES,
    )

    # Initialize producer for re-queuing messages
    try:
        producer = _get_producer(bootstrap_list)
    except Exception as e:
        logger.error("Failed to create Kafka producer: %s", e)
        sys.exit(1)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_list,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: v,
        )
    except KafkaError as e:
        logger.error("Failed to create Kafka consumer: %s", e)
        sys.exit(1)

    try:
        _consume_and_deliver(consumer, webhook_url, hmac_secret, topic, producer)
    finally:
        consumer.close()
        if _producer:
            _producer.close()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
