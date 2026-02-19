from datetime import datetime
import re
import hmac
import hashlib
import json
import os
import logging
from dotenv import load_dotenv

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
