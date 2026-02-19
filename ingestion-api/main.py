from flask import Flask, request, jsonify
from utils import validate_payment_event
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


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
        data = request.get_json()
        
        if not data:
            logger.warning('Received request with no JSON data')
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate payment event data
        is_valid, error_message = validate_payment_event(data)
        if not is_valid:
            transaction_id = data.get('Transaction ID', 'unknown')
            logger.warning(f'Validation failed for transaction {transaction_id}: {error_message}')
            return jsonify({'error': error_message}), 400
        
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
        
        # If all validations pass, return success response
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


def main():
    print("Hello from bruce!")


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
