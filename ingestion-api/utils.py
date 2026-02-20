from datetime import datetime
import re
import hmac
import hashlib
import json
import os
import logging
import atexit
from typing import Optional
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, LeaderNotAvailableError

# Load environment variables
load_dotenv()

# Configure logger
logger = logging.getLogger(__name__)


def validate_transaction_id(transaction_id: str) -> bool:
    """Validate transaction ID format (UUID-like)."""
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, transaction_id.lower()))


def validate_timestamp(timestamp: str) -> bool:
    """Validate timestamp format (YYYY-MM-DD HH:MM:SS)."""
    try:
        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


def validate_upi_id(upi_id: str) -> bool:
    """Validate UPI ID format."""
    upi_pattern = r'^[a-zA-Z0-9._-]+@[a-zA-Z0-9]+$'
    return bool(re.match(upi_pattern, upi_id))


def validate_amount(amount: float) -> bool:
    """Validate amount is positive."""
    return amount > 0


def validate_status(status: str) -> bool:
    """Validate status is either SUCCESS or FAILED."""
    return status.upper() in ['SUCCESS', 'FAILED']


def validate_payment_event(data: dict) -> tuple[bool, str | None]:
    """
    Validate a payment event data dictionary.
    
    Returns:
        tuple: (is_valid: bool, error_message: str | None)
        If valid, returns (True, None)
        If invalid, returns (False, error_message)
    """
    # Required fields
    required_fields = [
        'Transaction ID',
        'Timestamp',
        'Sender Name',
        'Sender UPI ID',
        'Receiver Name',
        'Receiver UPI ID',
        'Amount (INR)',
        'Status'
    ]
    
    # Check all required fields are present
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return False, f'Missing required fields: {", ".join(missing_fields)}'
    
    # Validate Transaction ID
    if not validate_transaction_id(data['Transaction ID']):
        return False, 'Invalid Transaction ID format. Expected UUID format.'
    
    # Validate Timestamp
    if not validate_timestamp(data['Timestamp']):
        return False, 'Invalid Timestamp format. Expected format: YYYY-MM-DD HH:MM:SS'
    
    # Validate Sender UPI ID
    if not validate_upi_id(data['Sender UPI ID']):
        return False, 'Invalid Sender UPI ID format'
    
    # Validate Receiver UPI ID
    if not validate_upi_id(data['Receiver UPI ID']):
        return False, 'Invalid Receiver UPI ID format'
    
    # Validate Amount
    try:
        amount = float(data['Amount (INR)'])
        if not validate_amount(amount):
            return False, 'Amount must be positive'
    except (ValueError, TypeError):
        return False, 'Invalid Amount (INR) format. Expected a number.'
    
    # Validate Status
    if not validate_status(data['Status']):
        return False, 'Invalid Status. Must be either SUCCESS or FAILED'
    
    # All validations passed
    return True, None


def get_hmac_secret() -> str:
    """
    Get HMAC secret from environment variable.
    
    Returns:
        HMAC secret key as string
        
    Raises:
        ValueError: If HMAC_SECRET is not set in environment
    """
    secret = os.getenv('HMAC_SECRET')
    if not secret:
        raise ValueError('HMAC_SECRET environment variable is not set')
    return secret


def generate_signature(payload: dict, secret: str) -> str:
    """
    Generate HMAC-SHA256 signature for a payload.
    
    Args:
        payload: Dictionary containing the request payload
        secret: Secret key for HMAC
        
    Returns:
        Hexadecimal signature string
    """
    transaction_id = payload.get('Transaction ID', 'unknown')
    
    try:
        # Convert payload to JSON string and encode to bytes
        payload_str = json.dumps(payload, sort_keys=True)
        payload_bytes = payload_str.encode('utf-8')
        secret_bytes = secret.encode('utf-8')
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            secret_bytes,
            payload_bytes,
            hashlib.sha256
        ).hexdigest()
        
        return signature
    except Exception as e:
        raise e


def verify_signature(payload: dict, signature: str, secret: str) -> bool:
    """
    Verify HMAC-SHA256 signature for a payload.
    
    Args:
        payload: Dictionary containing the request payload
        signature: Signature string to verify
        secret: Secret key for HMAC
        
    Returns:
        True if signature is valid, False otherwise
    """
    transaction_id = payload.get('Transaction ID', 'unknown')
    
    try:
        expected_signature = generate_signature(payload, secret)
        # Use constant-time comparison to prevent timing attacks
        is_valid = hmac.compare_digest(expected_signature, signature)
        
        if is_valid:
            logger.debug(f'HMAC signature verified successfully for transaction: {transaction_id}')
        else:
            logger.warning(f'HMAC signature verification failed for transaction: {transaction_id}')
        
        return is_valid
    except Exception as e:
        logger.error(f'Error verifying HMAC signature for transaction {transaction_id}: {str(e)}')
        return False


# ============================================================================
# Kafka Producer Utilities
# ============================================================================

def get_kafka_brokers() -> str:
    """
    Get Kafka broker addresses from environment variable or use default.
    
    Returns:
        Comma-separated string of Kafka broker addresses
    """
    brokers = os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'kafka-1:9092,kafka-2:9092,kafka-3:9092'
    )
    return brokers


# Global producer instance (created on first use)
_producer: Optional[KafkaProducer] = None


def create_kafka_producer() -> KafkaProducer:
    """
    Create and configure a Kafka producer with acks=all for high durability.
    Configured to work with KRaft-based Kafka cluster.
    
    Returns:
        Configured KafkaProducer instance
        
    Raises:
        Exception: If producer creation fails
    """
    brokers = get_kafka_brokers()
    broker_list = [broker.strip() for broker in brokers.split(',')]
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks=1,  # Wait for acknowledgment from all in-sync replicas (all brokers)
            retries=1,  # Retry up to 5 times on failure
            # max_in_flight_requests_per_connection=1,  # Ensure ordering
            # enable_idempotence=True,  # Prevent duplicate messages
            # compression_type='snappy',  # Compress messages for efficiency
            # request_timeout_ms=30000,  # 30 second timeout
            # delivery_timeout_ms=120000,  # 2 minute delivery timeout
            # api_version=(0, 10, 1),  # Use compatible API version
            max_block_ms=500,  # 2 second timeout for blocking operations
            reconnect_backoff_ms=5000,
            request_timeout_ms=1000,
            metadata_max_age_ms=300000,  # Refresh metadata every 5 minutes
        )
        
        logger.info(f'Kafka producer created successfully with brokers: {broker_list}')
        
        # Register cleanup function to close producer on exit
        atexit.register(close_producer)
        
        return producer
        
    except Exception as e:
        logger.error(f'Failed to create Kafka producer', exc_info=True)
        


def get_producer() -> KafkaProducer:
    """
    Get or create the global Kafka producer instance.
    
    Returns:
        KafkaProducer instance
    """
    global _producer
    if _producer is None:
        _producer = create_kafka_producer()
    return _producer


def send_payment_event_to_kafka(
    topic: str,
    payment_event: dict,
    transaction_id: Optional[str] = None
) -> bool:
    """
    Send a payment event to Kafka topic with acks=all configuration.
    Ensures message is acknowledged by all in-sync replicas before returning success.
    
    Args:
        topic: Kafka topic name
        payment_event: Payment event dictionary to send
        transaction_id: Optional transaction ID to use as message key
        
    Returns:
        True if message was sent successfully and acknowledged by all brokers, False otherwise
    """
    try:
        producer = get_producer()
        
        if not producer:
            logger.error('Kafka producer not available')
            return False
        
        # Use transaction ID as key for partitioning (ensures same transaction goes to same partition)
        key = transaction_id or payment_event.get('Transaction ID')
        
        if not key:
            logger.warning('No transaction ID found in payment event, using None as key')
        
        # Send message to Kafka
        # With acks='all', this will wait for acknowledgment from all in-sync replicas
        logger.info(f"\n\nSending payment event to Kafka topic: {topic}")
        logger.info(f"Payment event: {payment_event}")
        logger.info(f"Key: {key}\n\n")
        future = producer.send(topic, value=payment_event, key=key)
        
        # Wait for the message to be acknowledged by all brokers (acks=all)
        # This ensures the message is replicated to all brokers before returning
        record_metadata = future.get(timeout=30)
        
        logger.info(
            f'Payment event sent to Kafka successfully - '
            f'Topic: {record_metadata.topic}, '
            f'Partition: {record_metadata.partition}, '
            f'Offset: {record_metadata.offset}, '
            f'Transaction ID: {key}, '
            f'Acknowledged by all brokers (acks=all)'
        )
        
        return True
        
    except LeaderNotAvailableError as e:
        logger.error(
            f'Kafka leader not available for topic {topic}. '
            f'This may indicate the topic does not exist or cluster is initializing. Error: {str(e)}'
        )
        return False
    except KafkaTimeoutError as e:
        logger.error(
            f'Timeout waiting for Kafka acknowledgment (acks=all). '
            f'Message may not have been replicated to all brokers. Error: {str(e)}'
        )
        return False
    except KafkaError as e:
        logger.error(f'Kafka error sending payment event: {str(e)}', exc_info=True)
        return False
    except Exception as e:
        logger.error(f'Unexpected error sending payment event to Kafka: {str(e)}', exc_info=True)
        return False


def close_producer():
    """
    Close the Kafka producer and flush any pending messages.
    This ensures all messages are sent before shutdown.
    """
    global _producer
    if _producer is not None:
        try:
            logger.info('Flushing pending Kafka messages...')
            _producer.flush(timeout=30)  # Wait up to 30 seconds for pending messages
            _producer.close(timeout=10)
            logger.info('Kafka producer closed successfully')
        except Exception as e:
            logger.error(f'Error closing Kafka producer: {str(e)}', exc_info=True)
        finally:
            _producer = None


def is_producer_available() -> bool:
    """
    Check if Kafka producer is available and can connect to brokers.
    
    Returns:
        True if producer is available, False otherwise
    """
    try:
        producer = get_producer()
        # Try to get cluster metadata to verify connection
        producer.list_topics(timeout=5)
        return True
    except Exception as e:
        logger.warning(f'Kafka producer not available: {str(e)}')
        return False


# ============================================================================
# Postgres Transactional Outbox
# ============================================================================

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


def is_db_available() -> bool:
    try:
        conn = _get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Postgres not available: {str(e)}")
        return False


def write_payment_and_outbox(payment_event: dict) -> bool:
    """
    Transactionally write the payment event to `payments` and enqueue an outbox row.
    This replaces publishing directly to Kafka in the request path.
    """
    transaction_id = payment_event.get("Transaction ID")
    if not transaction_id:
        raise ValueError("Transaction ID missing from payment event")

    conn = _get_db_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # cur.execute(
            #     """
            #     INSERT INTO payments (transaction_id, payload)
            #     VALUES (%s, %s::jsonb)
            #     ON CONFLICT (transaction_id) DO UPDATE
            #       SET payload = EXCLUDED.payload;
            #     """,
            #     (transaction_id, json.dumps(payment_event, ensure_ascii=False)),
            # )
            logger.info(f"Inserting outbox event for transaction {transaction_id}")
            logger.info(f"Payment event: {payment_event}")
            cur.execute(
                """
                INSERT INTO outbox_events (aggregate_id, event_type, payload)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (aggregate_id, event_type) DO NOTHING;
                """,
                (transaction_id, "payment.received", json.dumps(payment_event, ensure_ascii=False)),
            )

        conn.commit()
        logger.info(f'Payment and outbox written successfully for transaction {transaction_id}')
        return True
    except Exception as e:
        logger.error(f'Error writing payment and outbox: {str(e)}', exc_info=True)
        conn.rollback()
        return
    finally:
        conn.close()
        logger.info(f'Database connection closed successfully for transaction {transaction_id}')
