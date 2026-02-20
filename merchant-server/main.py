"""
Flask API that accepts webhooks from the dispatcher.
No HMAC verification. Simulates an unreliable listener:
- Responds with success before RESPONSE_TIMEOUT when "healthy"
- ~70% of the time: either returns an error or responds after the timeout (delayed)
"""

import logging
import os
import random
import time

from flask import Flask, request, jsonify

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Response must be sent within this many seconds to count as success (for simulation)
RESPONSE_TIMEOUT_SEC = float(os.getenv("RESPONSE_TIMEOUT", "3"))
# Fraction of requests that fail or are delayed beyond timeout (0.0–1.0)
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.7"))

accepted_transactions = set()

@app.route("/webhook_listener", methods=["POST"])
def receive_webhook():
    """Accept webhook from dispatcher: JSON body only (no signature verification)."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data provided"}), 400

    # logger.info("Webhook received payload=%s", data)
    tid = data.get("Transaction ID", "?")
    # Simulate unreliable behavior: 70% fail or delayed beyond timeout
    if random.random() < FAILURE_RATE:
        if random.random() < 0.5:
            # Immediate failure
            logger.info("Simulated failure for tid=%s", tid)
            return jsonify({"error": "Internal server error"}), 500
        # Delayed beyond response timeout (client will typically timeout)
        delay = RESPONSE_TIMEOUT_SEC + random.uniform(1, 5)
        logger.info("Simulated delay %.1fs for tid=%s", delay, tid)
        time.sleep(delay)
        return jsonify({"status": "accepted", "transaction_id": tid}), 200

    # Success: respond within timeout
    logger.info("Accepted webhook tid=%s", tid)
    accepted_transactions.add(tid)
    logger.info("Accepted transactions so far :-")
    logger.info(accepted_transactions)
    return jsonify({"status": "accepted", "transaction_id": tid}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, threaded=True)
