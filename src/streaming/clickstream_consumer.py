"""
Snowpipe Streaming Consumer
Uses Snowflake's Streaming Ingest API for low-latency ingestion
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import time
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

load_dotenv()


class SnowpipeStreamingConsumer:
    
    def __init__(self, batch_size=100, flush_interval_seconds=5):
        """
        Initialize Kafka consumer and Snowpipe Streaming client
        
        Args:
            batch_size: Number of events to buffer before inserting
            flush_interval_seconds: Max time to wait before flushing buffer
        """
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.buffer = []
        self.last_flush_time = time.time()
        
        # Kafka consumer
        print("Connecting to Kafka...")
        self.consumer = KafkaConsumer(
            'clickstream_events',
            bootstrap_servers=['localhost:9093'],
            group_id='clickstream-snowpipe-streaming',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=1000  # Return from iterator after 1s of no messages
        )
        print("Connected to Kafka")
        
        # Load RSA private key for Snowflake authentication
        private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH', 'rsa_key.p8')
        private_key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None)
        
        print(f"Loading private key from {private_key_path}...")
        with open(private_key_path, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=private_key_passphrase.encode() if private_key_passphrase else None,
                backend=default_backend()
            )
        
        # Get private key bytes for Snowflake
        self.private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Snowpipe Streaming configuration
        from snowflake.ingest import (
            SnowflakeStreamingIngestClient,
            SnowflakeStreamingIngestClientConfig
        )
        
        print("Initializing Snowpipe Streaming client...")
        config = SnowflakeStreamingIngestClientConfig(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            private_key=self.private_key_bytes,
            role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        )
        
        self.streaming_client = SnowflakeStreamingIngestClient(
            name='clickstream_ingest_client',
            config=config
        )
        
        # Open a channel to the target table
        print("Opening streaming channel...")
        self.channel = self.streaming_client.open_channel(
            name='clickstream_channel',
            database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
            schema='RAW',
            table='CLICKSTREAM_EVENTS'
        )
        
        print("=" * 70)
        print("Snowpipe Streaming Consumer Initialized")
        print("=" * 70)
        print(f"   Kafka: localhost:9093")
        print(f"   Topic: clickstream_events")
        print(f"   Snowflake: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"   Target: RAW.CLICKSTREAM_EVENTS")
        print(f"   Batch size: {batch_size}")
        print(f"   Flush interval: {flush_interval_seconds}s")
        print("=" * 70)
    
    def _flush_buffer(self):
        """Insert buffered events to Snowflake via streaming channel"""
        if not self.buffer:
            return 0
        
        rows_inserted = 0
        
        try:
            for event in self.buffer:
                row = {
                    'EVENT_ID': event['event_id'],
                    'EVENT_TYPE': event['event_type'],
                    'EVENT_TIMESTAMP': event['timestamp'],
                    'SESSION_ID': event.get('session_id'),
                    'USER_ID': event.get('user_id'),
                    'DEVICE_TYPE': event.get('device_type'),
                    'BROWSER': event.get('browser'),
                    'OS': event.get('os'),
                    'LOCATION': event.get('location'),
                    'PAGE_URL': event.get('page_url'),
                    'REFERRER': event.get('referrer'),
                    'PRODUCT_ID': event.get('product_id'),
                    'PRODUCT_CATEGORY': event.get('product_category'),
                    'SEARCH_QUERY': event.get('search_query'),
                    'QUANTITY': event.get('quantity'),
                    'PRICE': event.get('price'),
                    'CART_VALUE': event.get('cart_value'),
                    'ORDER_ID': event.get('order_id'),
                    'TOTAL_AMOUNT': event.get('total_amount'),
                    'PAYMENT_METHOD': event.get('payment_method'),
                    'ITEMS_COUNT': event.get('items_count'),
                    'DURATION_SECONDS': event.get('duration_seconds'),
                    'RESULTS_COUNT': event.get('results_count'),
                    'INGESTED_AT': datetime.now().isoformat()
                }
                
                # Insert row via streaming channel
                self.channel.insert_row(row)
                rows_inserted += 1
            
            # Commit the Kafka offsets after successful insert
            self.consumer.commit()
            
            self.buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            print(f"Error flushing buffer: {e}")
            raise
        
        return rows_inserted
    
    def _should_flush(self):
        """Check if buffer should be flushed"""
        buffer_full = len(self.buffer) >= self.batch_size
        timeout_reached = (time.time() - self.last_flush_time) >= self.flush_interval
        return buffer_full or (self.buffer and timeout_reached)
    
    def consume_events(self):
        """Consume events and stream to Snowflake"""
        
        print("\nStarting streaming ingestion...")
        print("   Method: Snowpipe Streaming API")
        print("   Latency: ~1-10 seconds")
        print("   Press Ctrl+C to stop\n")
        
        events_processed = 0
        start_time = time.time()
        
        try:
            while True:
                # Poll for messages (with timeout from consumer_timeout_ms)
                try:
                    for message in self.consumer:
                        event = message.value
                        self.buffer.append(event)
                        
                        # Flush if needed
                        if self._should_flush():
                            flushed = self._flush_buffer()
                            events_processed += flushed
                            
                            elapsed = time.time() - start_time
                            rate = events_processed / elapsed if elapsed > 0 else 0
                            print(f"Streamed: {events_processed:,} events | Rate: {rate:.1f}/sec")
                    
                except StopIteration:
                    # No messages received within timeout, check if we should flush
                    pass
                
                # Flush on timeout even if batch not full
                if self._should_flush():
                    flushed = self._flush_buffer()
                    if flushed > 0:
                        events_processed += flushed
                        elapsed = time.time() - start_time
                        rate = events_processed / elapsed if elapsed > 0 else 0
                        print(f"Streamed: {events_processed:,} events | Rate: {rate:.1f}/sec")
        
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        
        except Exception as e:
            print(f"Error: {e}")
            raise
        
        finally:
            # Flush remaining buffer
            if self.buffer:
                flushed = self._flush_buffer()
                events_processed += flushed
                print(f"Flushed remaining {flushed} events")
            
            # Close resources
            print("Closing channel...")
            self.channel.close()
            
            print("Closing streaming client...")
            self.streaming_client.close()
            
            print("Closing Kafka consumer...")
            self.consumer.close()
            
            elapsed = time.time() - start_time
            print("\n" + "=" * 70)
            print("Consumer Stopped")
            print("=" * 70)
            print(f"   Total events streamed: {events_processed:,}")
            print(f"   Duration: {elapsed:.1f} seconds")
            if elapsed > 0:
                print(f"   Average rate: {events_processed/elapsed:.1f} events/sec")
            print("=" * 70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Consume clickstream events to Snowflake')
    parser.add_argument('--batch-size', type=int, default=100, help='Events per batch (default: 100)')
    parser.add_argument('--flush-interval', type=int, default=5, help='Flush interval in seconds (default: 5)')
    
    args = parser.parse_args()
    
    consumer = SnowpipeStreamingConsumer(
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval
    )
    consumer.consume_events()
