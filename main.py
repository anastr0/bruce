from flask import Flask, request, jsonify
from utils import validate_payment_event

app = Flask(__name__)


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
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate payment event data
        is_valid, error_message = validate_payment_event(data)
        if not is_valid:
            return jsonify({'error': error_message}), 400
        
        # If all validations pass, return success response
        return jsonify({
            'message': 'Payment event received successfully',
            'transaction_id': data['Transaction ID']
        }), 201
        
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


def main():
    print("Hello from bruce!")


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
