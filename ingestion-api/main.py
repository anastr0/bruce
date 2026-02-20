from flask import Flask, request, jsonify
from utils import (
    validate_payment_event,
    verify_signature,
    get_hmac_secret,
    write_payment_and_outbox,
    send_payment_event_to_kafka,
    is_db_available,
    is_producer_available,
)
import logging
import os

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def validate_request(request_obj) -> tuple[bool, dict | None, int | None]:
    """
    Validate incoming request including JSON data, HMAC signature, and payment event data.
    
    Args:
        request_obj: Flask request object
        
    Returns:
        Tuple of (is_valid: bool, error_response: dict | None, status_code: int | None)
        If valid, returns (True, None, None)
        If invalid, returns (False, error_response_dict, status_code)
    """
    # Check if JSON data exists
    data = request_obj.get_json()
    if not data:
        logger.warning('Received request with no JSON data')
        return False, {'error': 'No JSON data provided'}, 400
    
    # Verify HMAC signature header exists
    signature = request_obj.headers.get('X-HMAC-Signature')
    if not signature:
        logger.warning('Received request without HMAC signature')
        return False, {'error': 'Forbidden'}, 403
    
    # Verify HMAC signature
    try:
        secret = get_hmac_secret()
        if not verify_signature(data, signature, secret):
            transaction_id = data.get('Transaction ID', 'unknown')
            logger.warning(f'Invalid HMAC signature for transaction {transaction_id}')
            return False, {'error': 'Forbidden'}, 403
    except ValueError as e:
        logger.error(f'HMAC secret configuration error: {str(e)}')
        return False, {'error': 'Internal server error'}, 500
    
    # Validate payment event data
    is_valid, error_message = validate_payment_event(data)
    if not is_valid:
        transaction_id = data.get('Transaction ID', 'unknown')
        logger.warning(f'Validation failed for transaction {transaction_id}: {error_message}')
        return False, {'error': error_message}, 400
    
    # All validations passed
    return True, None, None


@app.route('/events', methods=['POST'])
def receive_payment_event():
    """
    Receive a payment event matching the transactions.csv schema.
    
    Expected JSON schema:
    {
        "Transaction ID": "uuid-string",
        "Timestamp": "YYYY-MM-DD HH:MM:SS",
        "Sender Name": "string",
        "Sender UPI ID": "string@bank",
        "Receiver Name": "string",
        "Receiver UPI ID": "string@bank",
        "Amount (INR)": float,
        "Status": "SUCCESS" | "FAILED"
    }
    """
    try:
        # Validate request (JSON data, HMAC signature, payment event data)
        # is_valid, error_response, status_code = validate_request(request)
        # if not is_valid:
        #     return jsonify(error_response), status_code
        
        # Get validated data
        data = request.get_json()
        
        # Log payment event at info level
        transaction_id = data['Transaction ID']
        amount = data['Amount (INR)']
        status = data['Status']
        sender = data['Sender Name']
        receiver = data['Receiver Name']
        timestamp = data['Timestamp']
        
        logger.info(
            f'Payment event received - '
            f'Transaction ID: {transaction_id}, '
            f'Amount: {amount} INR, '
            f'Status: {status}, '
            f'Sender: {sender}, '
            f'Receiver: {receiver}, '
            f'Timestamp: {timestamp}'
        )

        # Write to Postgres (payments + outbox_events)
        write_payment_and_outbox(data)

        # Only produce to Kafka if connection can be established
        kafka_topic = os.getenv('KAFKA_TOPIC', 'payment-webhooks')

        try:
            kafka_ok = send_payment_event_to_kafka(
                topic=kafka_topic,
                payment_event=data,
                transaction_id=transaction_id,
            )
            if not kafka_ok:
                logger.error(f'Failed to produce payment event to Kafka for transaction {transaction_id}')
        except Exception as e:
            # Ignore Kafka errors and move on
            logger.error(f'Kafka production error for transaction {transaction_id}: {str(e)}. Event saved to database/outbox.')

        return jsonify({
            'message': 'Payment event received successfully',
            'transaction_id': data['Transaction ID']
        }), 201
        
    except Exception as e:
        logger.error(f'Internal server error: {str(e)}', exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


# @app.route('/health', methods=['GET'])
# def health_check():
#     """
#     Health check endpoint to verify API, Postgres, and Kafka connectivity.
#     """
#     db_available = is_db_available()
#     kafka_available = is_producer_available()
#     healthy = db_available and kafka_available

#     status = {
#         'status': 'healthy' if healthy else 'degraded',
#         'postgres': 'connected' if db_available else 'disconnected',
#         'kafka': 'connected' if kafka_available else 'disconnected',
#     }

#     status_code = 200 if healthy else 503
#     return jsonify(status), status_code


def main():
    print("Hello from bruce!")


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
