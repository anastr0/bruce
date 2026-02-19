# Bruce - Payment Event API

A Flask API for receiving and validating payment events, with a client script to simulate real-time traffic.

## Features

- RESTful API endpoint for receiving payment events
- Comprehensive validation of payment transaction data
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

## Running the API Server

Start the Flask API server:

```bash
python main.py
```

The API will start on `http://localhost:5000` by default.

### API Endpoint

**POST** `/events`

Receives payment event data matching the transactions.csv schema.

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
- `500 Internal Server Error` - Server errors

All payment events are logged at INFO level with transaction details.

## Running the Client Script

The client script reads transactions from CSV and sends POST requests to simulate real-time traffic.

### Basic Usage

```bash
python client.py
```

This will:
- Read transactions from `dataset/transactions.csv`
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

1. **Start the API server** (in one terminal):
```bash
python main.py
```

2. **Run the client script** (in another terminal):
```bash
python client.py --max 20
```

You should see:
- API server logs showing incoming payment events
- Client script progress and summary statistics

## Project Structure

```
bruce/
├── main.py              # Flask API server
├── client.py            # Client script for traffic simulation
├── utils.py             # Validation utilities
├── dataset/
│   └── transactions.csv # Sample transaction data
├── pyproject.toml       # Project dependencies
└── README.md            # This file
```

## Logging

The API server logs all payment events at INFO level with the following details:
- Transaction ID
- Amount (INR)
- Status (SUCCESS/FAILED)
- Sender and Receiver names
- Timestamp

Validation failures are logged at WARNING level, and server errors at ERROR level.
