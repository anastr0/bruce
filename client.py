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


def read_transactions_csv_chunks(file_path: str, chunk_size: int = 10):
    """
    Generator that reads transactions from CSV file in chunks.
    
    Args:
        file_path: Path to the transactions CSV file
        chunk_size: Number of transactions to read per chunk (default: 10)
        
    Yields:
        List of transaction dictionaries (chunk of transactions)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            chunk = []
            
            for row in reader:
                # Convert Amount (INR) to float
                transaction = dict(row)
                try:
                    transaction['Amount (INR)'] = float(transaction['Amount (INR)'])
                except (ValueError, KeyError):
                    print(f"Warning: Invalid amount for transaction {transaction.get('Transaction ID', 'unknown')}")
                    continue
                
                chunk.append(transaction)
                
                # Yield chunk when it reaches the desired size
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            
            # Yield remaining transactions if any
            if chunk:
                yield chunk
    
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
    start_from: int = 0,
    chunk_size: int = 10
):
    """
    Simulate real-time traffic by reading CSV in chunks and sending POST requests.
    
    Args:
        csv_file: Path to transactions CSV file
        api_url: Base URL of the Flask API
        delay: Delay in seconds between requests (default: 0.1s)
        max_transactions: Maximum number of transactions to send (None for all)
        start_from: Index to start from (for resuming)
        chunk_size: Number of transactions to read per chunk (default: 10)
    """
    print(f"Reading transactions from {csv_file} in chunks of {chunk_size}...")
    print(f"Sending requests to {api_url}/events")
    print(f"Delay between requests: {delay}s\n")
    print("-" * 80)
    
    success_count = 0
    failure_count = 0
    total_processed = 0
    transaction_counter = 0
    
    # Create generator for reading chunks
    chunk_generator = read_transactions_csv_chunks(csv_file, chunk_size=chunk_size)
    
    # Skip chunks until we reach start_from
    skipped = 0
    first_chunk = None
    first_chunk_num = 1
    
    if start_from > 0:
        print(f"Skipping to transaction {start_from + 1}...")
        for chunk in chunk_generator:
            if skipped + len(chunk) <= start_from:
                skipped += len(chunk)
                first_chunk_num += 1
                continue
            else:
                # Adjust chunk to start from the right position
                offset = start_from - skipped
                first_chunk = chunk[offset:]
                break
    
    # Process chunks
    chunk_num = first_chunk_num
    
    # Process first chunk if it was adjusted
    if first_chunk:
        chunk = first_chunk
        
        for transaction in chunk:
            # Check if we've reached max_transactions limit
            if max_transactions and transaction_counter >= max_transactions:
                print(f"\nReached maximum transaction limit ({max_transactions})")
                break
            
            transaction_counter += 1
            transaction_id = transaction.get('Transaction ID', 'unknown')
            
            print(f"  [{transaction_counter}] Sending transaction: {transaction_id[:8]}...", end=" ")
            
            success, message = send_payment_event(api_url, transaction)
            
            if success:
                success_count += 1
                print(f"✓ {message}")
            else:
                failure_count += 1
                print(f"✗ {message}")
            
            total_processed += 1
            
            # Add delay between requests
            time.sleep(delay)
        
        print(f"  Chunk {chunk_num} completed. Total processed: {total_processed}")
        chunk_num += 1
        
        # Break if we've reached max_transactions
        if max_transactions and transaction_counter >= max_transactions:
            chunk_generator = iter([])  # Empty iterator to stop processing
    
    # Process remaining chunks from generator
    for chunk in chunk_generator:
        # Break if we've reached max_transactions
        if max_transactions and transaction_counter >= max_transactions:
            print(f"\nReached maximum transaction limit ({max_transactions})")
            break
        
        print(f"\n[Chunk {chunk_num}] Processing {len(chunk)} transactions...")
        
        for transaction in chunk:
            # Check if we've reached max_transactions limit
            if max_transactions and transaction_counter >= max_transactions:
                print(f"\nReached maximum transaction limit ({max_transactions})")
                break
            
            transaction_counter += 1
            transaction_id = transaction.get('Transaction ID', 'unknown')
            
            print(f"  [{transaction_counter}] Sending transaction: {transaction_id[:8]}...", end=" ")
            
            success, message = send_payment_event(api_url, transaction)
            
            if success:
                success_count += 1
                print(f"✓ {message}")
            else:
                failure_count += 1
                print(f"✗ {message}")
            
            total_processed += 1
            
            # Add delay between requests
            time.sleep(delay)
        
        print(f"  Chunk {chunk_num} completed. Total processed: {total_processed}")
        chunk_num += 1
        
        # Break if we've reached max_transactions
        if max_transactions and transaction_counter >= max_transactions:
            break
    
    print("-" * 80)
    print(f"\nSummary:")
    print(f"  Total sent: {total_processed}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failure_count}")
    print(f"  Success rate: {(success_count/total_processed*100):.1f}%" if total_processed > 0 else "N/A")


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
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=10,
        help='Number of transactions to read per chunk (default: 10)'
    )
    
    args = parser.parse_args()
    
    simulate_traffic(
        csv_file=args.csv,
        api_url=args.url,
        delay=args.delay,
        max_transactions=args.max,
        start_from=args.start_from,
        chunk_size=args.chunk_size
    )


if __name__ == "__main__":
    main()
