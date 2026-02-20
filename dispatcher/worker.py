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
from kafka import KafkaConsumer
from kafka.errors import KafkaError

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
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 2
REQUEST_TIMEOUT_SEC = 30

_shutdown = False


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
            "Delivered to webhook tid=%s status=%s",
            payload.get("Transaction ID", "?"),
            resp.status_code,
        )
        return True
    except requests.RequestException as e:
        logger.warning("Webhook delivery failed: %s", e)
        return False


def _consume_and_deliver(
    consumer: KafkaConsumer,
    webhook_url: str,
    hmac_secret: str,
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
        delivered = False
        for attempt in range(1, MAX_RETRIES + 1):
            if _deliver_to_webhook(webhook_url, payload, hmac_secret):
                delivered = True
                break
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
        if not delivered:
            logger.error(
                "Failed to deliver after %d attempts, tid=%s",
                MAX_RETRIES,
                payload.get("Transaction ID", "?"),
            )


def main() -> None:
    global _shutdown

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
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
        "Starting worker bootstrap=%s topic=%s group=%s webhook=%s",
        bootstrap,
        topic,
        group_id,
        webhook_url,
    )

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: v,
        )
    except KafkaError as e:
        logger.error("Failed to create Kafka consumer: %s", e)
        sys.exit(1)

    try:
        _consume_and_deliver(consumer, webhook_url, hmac_secret)
    finally:
        consumer.close()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
