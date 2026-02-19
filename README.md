# Bruce - Payment Event API

A Flask API for receiving and validating payment events, with a client script to simulate real-time traffic.

## Features

- RESTful API endpoint for receiving payment events
- Comprehensive validation of payment transaction data
- HMAC-SHA256 signature verification for secure communication
- Real-time traffic simulation client
- Memory-efficient chunk-based CSV processing
- Detailed logging of all payment events

## Prerequisites

- Python >= 3.11
- `uv` package manager (or pip)

## Installation

1. Install dependencies using `uv`:
```bash
uv sync
```

Or using pip:
```bash
pip install -e .
```

2. **Set up HMAC secret** (required for API and client):

Copy the example environment file and set your HMAC secret:
```bash
cp .env.example .env
```

Generate a secure random key:
```bash
openssl rand -hex 32
```

Edit `.env` and set `HMAC_SECRET` to your generated key:
```
HMAC_SECRET=your-generated-secret-key-here
```

**Important:** The same `HMAC_SECRET` must be used by both the API server and the client script.

## Running the API Server

**Note:** Make sure `.env` file exists with `HMAC_SECRET` set before starting the server.

Start the Flask API server:

```bash
cd ingestion-api
python main.py
```

Or from the root directory:
```bash
python ingestion-api/main.py
```

The API will start on `http://localhost:5000` by default.

The API verifies HMAC signatures on every incoming request. Requests without a valid signature will be rejected with a `401 Unauthorized` response.

### API Endpoint

**POST** `/events`

Receives payment event data matching the transactions.csv schema.

**Request Headers:**
- `Content-Type: application/json`
- `X-HMAC-Signature: <hmac-signature>` (required)

**Request Body:**
```json
{
  "Transaction ID": "4d3db980-46cd-4158-a812-dcb77055d0d2",
  "Timestamp": "2024-06-22 04:06:38",
  "Sender Name": "Tiya Mall",
  "Sender UPI ID": "4161803452@okaxis",
  "Receiver Name": "Mohanlal Golla",
  "Receiver UPI ID": "7776849307@okybl",
  "Amount (INR)": 3907.34,
  "Status": "FAILED"
}
```

**Response:**
- `201 Created` - Event received successfully
- `400 Bad Request` - Validation errors
- `401 Unauthorized` - Invalid or missing HMAC signature
- `500 Internal Server Error` - Server errors

All payment events are logged at INFO level with transaction details.

## Running the Client Script

The client script reads transactions from CSV and sends POST requests to simulate real-time traffic.

### Basic Usage

**Note:** Make sure `.env` file exists with `HMAC_SECRET` set before running the client.

```bash
python client.py
```

This will:
- Read transactions from `dataset/transactions.csv`
- Generate HMAC signatures for each request
- Send requests in chunks of 10 transactions
- Use default delay of 0.1 seconds between requests
- Send to `http://localhost:5000/events`

### Command-Line Options

```bash
python client.py [OPTIONS]
```

**Available Options:**

- `--csv PATH` - Path to transactions CSV file (default: `dataset/transactions.csv`)
- `--url URL` - Base URL of the Flask API (default: `http://localhost:5000`)
- `--delay SECONDS` - Delay in seconds between requests (default: `0.1`)
- `--max N` - Maximum number of transactions to send (default: all)
- `--start-from N` - Index to start from, for resuming (default: `0`)
- `--chunk-size N` - Number of transactions to read per chunk (default: `10`)
- `--hmac-secret SECRET` - HMAC secret key (default: reads from `HMAC_SECRET` env var)

### Examples

**Send only first 50 transactions:**
```bash
python client.py --max 50
```

**Faster traffic simulation (0.05s delay):**
```bash
python client.py --delay 0.05
```

**Custom chunk size (20 transactions per chunk):**
```bash
python client.py --chunk-size 20
```

**Resume from transaction 100:**
```bash
python client.py --start-from 100
```

**Send to different API endpoint:**
```bash
python client.py --url http://localhost:8000
```

**Combine multiple options:**
```bash
python client.py --max 100 --chunk-size 5 --delay 0.2
```

## Complete Workflow Example

1. **Set up environment** (one-time setup):
```bash
cp .env.example .env
# Edit .env and set HMAC_SECRET to a secure random key
```

2. **Start the API server** (in one terminal):
```bash
cd ingestion-api
python main.py
```

3. **Run the client script** (in another terminal):
```bash
python client.py --max 20
```

You should see:
- API server logs showing incoming payment events
- Client script progress and summary statistics
- Successful HMAC signature verification

## Project Structure

```
bruce/
├── ingestion-api/
│   ├── main.py          # Flask API server
│   ├── utils.py         # Validation utilities
│   └── hmac_utils.py    # HMAC signature utilities
├── client.py            # Client script for traffic simulation
├── dataset/
│   └── transactions.csv # Sample transaction data
├── .env                 # Environment variables (HMAC_SECRET) - not in git
├── .env.example         # Example environment file
├── pyproject.toml       # Project dependencies
└── README.md            # This file
```

## Security

### HMAC Signature Verification

All requests to the API must include a valid HMAC-SHA256 signature in the `X-HMAC-Signature` header. The signature is computed from the JSON payload using the shared secret key.

**Signature Generation:**
1. Serialize the JSON payload with sorted keys
2. Compute HMAC-SHA256 using the secret key
3. Send the hexadecimal signature in the `X-HMAC-Signature` header

**Signature Verification:**
- The API verifies the signature before processing any request
- Invalid or missing signatures result in `401 Unauthorized` responses
- Uses constant-time comparison to prevent timing attacks

## Logging

The API server logs all payment events at INFO level with the following details:
- Transaction ID
- Amount (INR)
- Status (SUCCESS/FAILED)
- Sender and Receiver names
- Timestamp

Validation failures are logged at WARNING level, invalid signatures at WARNING level, and server errors at ERROR level.
