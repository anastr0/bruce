#!/usr/bin/env python3
"""
Client script to simulate real-time payment event traffic.
Reads transactions from CSV and sends POST requests to the Flask API.
"""

import csv
import json
import time
import argparse
import sys
from typing import Dict, Any
import requests


def read_transactions_csv(file_path: str) -> list[Dict[str, Any]]:
    """
    Read transactions from CSV file and convert to list of dictionaries.
    
    Args:
        file_path: Path to the transactions CSV file
        
    Returns:
        List of transaction dictionaries
    """
    transactions = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Convert Amount (INR) to float
                transaction = dict(row)
                try:
                    transaction['Amount (INR)'] = float(transaction['Amount (INR)'])
                except (ValueError, KeyError):
                    print(f"Warning: Invalid amount for transaction {transaction.get('Transaction ID', 'unknown')}")
                    continue
                
                transactions.append(transaction)
        
        return transactions
    
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)


def send_payment_event(api_url: str, transaction: Dict[str, Any]) -> tuple[bool, str]:
    """
    Send a payment event to the Flask API.
    
    Args:
        api_url: Base URL of the Flask API
        transaction: Transaction data dictionary
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        response = requests.post(
            f"{api_url}/events",
            json=transaction,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 201:
            return True, f"Success: {response.json().get('message', 'Event received')}"
        else:
            error_msg = response.json().get('error', 'Unknown error')
            return False, f"Failed ({response.status_code}): {error_msg}"
    
    except requests.exceptions.ConnectionError:
        return False, "Error: Could not connect to API. Is the Flask server running?"
    except requests.exceptions.Timeout:
        return False, "Error: Request timed out"
    except Exception as e:
        return False, f"Error: {str(e)}"


def simulate_traffic(
    csv_file: str,
    api_url: str = "http://localhost:5000",
    delay: float = 0.1,
    max_transactions: int = None,
    start_from: int = 0
):
    """
    Simulate real-time traffic by reading CSV and sending POST requests.
    
    Args:
        csv_file: Path to transactions CSV file
        api_url: Base URL of the Flask API
        delay: Delay in seconds between requests (default: 0.1s)
        max_transactions: Maximum number of transactions to send (None for all)
        start_from: Index to start from (for resuming)
    """
    print(f"Reading transactions from {csv_file}...")
    transactions = read_transactions_csv(csv_file)
    
    total_transactions = len(transactions)
    print(f"Found {total_transactions} transactions")
    
    if start_from > 0:
        transactions = transactions[start_from:]
        print(f"Starting from transaction {start_from + 1}")
    
    if max_transactions:
        transactions = transactions[:max_transactions]
        print(f"Limiting to {max_transactions} transactions")
    
    print(f"\nSending {len(transactions)} transactions to {api_url}/events")
    print(f"Delay between requests: {delay}s\n")
    print("-" * 80)
    
    success_count = 0
    failure_count = 0
    
    for idx, transaction in enumerate(transactions, start=1):
        transaction_id = transaction.get('Transaction ID', 'unknown')
        
        print(f"[{idx}/{len(transactions)}] Sending transaction: {transaction_id[:8]}...", end=" ")
        
        success, message = send_payment_event(api_url, transaction)
        
        if success:
            success_count += 1
            print(f"✓ {message}")
        else:
            failure_count += 1
            print(f"✗ {message}")
        
        # Add delay between requests (except for the last one)
        if idx < len(transactions):
            time.sleep(delay)
    
    print("-" * 80)
    print(f"\nSummary:")
    print(f"  Total sent: {len(transactions)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failure_count}")
    print(f"  Success rate: {(success_count/len(transactions)*100):.1f}%" if transactions else "N/A")


def main():
    parser = argparse.ArgumentParser(
        description='Simulate real-time payment event traffic by sending transactions from CSV to Flask API'
    )
    parser.add_argument(
        '--csv',
        default='dataset/transactions.csv',
        help='Path to transactions CSV file (default: dataset/transactions.csv)'
    )
    parser.add_argument(
        '--url',
        default='http://localhost:5000',
        help='Base URL of the Flask API (default: http://localhost:5000)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay in seconds between requests (default: 0.1)'
    )
    parser.add_argument(
        '--max',
        type=int,
        default=None,
        help='Maximum number of transactions to send (default: all)'
    )
    parser.add_argument(
        '--start-from',
        type=int,
        default=0,
        help='Index to start from (for resuming, default: 0)'
    )
    
    args = parser.parse_args()
    
    simulate_traffic(
        csv_file=args.csv,
        api_url=args.url,
        delay=args.delay,
        max_transactions=args.max,
        start_from=args.start_from
    )


if __name__ == "__main__":
    main()
