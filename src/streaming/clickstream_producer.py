"""
Clickstream Event Producer
Generates realistic user activity events and sends to Kafka
"""

import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

class ClickstreamProducer:
    
    def __init__(self, kafka_broker='localhost:9093'):
        """
        Initialize producer
        
        Args:
            kafka_broker: Kafka broker address (localhost:9093 for local)
        """
        print(f"Connecting to Kafka at {kafka_broker}...")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  
                retries=3,
                max_block_ms=5000  
            )
            print("Connected to Kafka successfully")
            
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            raise
        
        self.faker = Faker()
        
        # Event types and their probabilities
        self.event_types = [
            ('page_view', 0.40),        # 40%
            ('product_click', 0.25),     # 25%
            ('add_to_cart', 0.15),       # 15%
            ('remove_from_cart', 0.05),  # 5%
            ('search', 0.10),            # 10%
            ('checkout_start', 0.03),    # 3%
            ('purchase', 0.02),          # 2%
        ]
        
        # Sample data
        self.pages = [
            '/', '/products', '/cart', '/checkout', '/account',
            '/products/vehicle', '/products/kitchen-accessories', '/products/home',
            '/products/beauty', '/products/sports', '/about', '/contact'
        ]
        
        self.product_categories = [
            'beauty', 'laptops','mens-shoes','motorcycle','skin-care','smartphones','vehicle',
            'womens-bags','kitchen-accessories','sports-accessories','fragrances','furniture','groceries',
            'home-decoration','mens-shirts','mens-watches','mobile-accessories','tops','womens-watches',
            'womens-dresses','womens-jewellery','sunglasses','tablets','womens-shoes',
        ]
        
        self.search_terms = [
            'laptop', 'phone', 'shoes', 'dress', 'watch', 'headphones',
            'camera', 'tablet', 'tv', 'chair', 'backpack', 'sunglasses'
        ]
        
        # Simulate active user sessions (100 concurrent users)
        self.active_sessions = {}
    
    def generate_event(self):
        """Generate a single clickstream event"""
        
        # Choose event type based on probabilities
        event_type = random.choices(
            [e[0] for e in self.event_types],
            weights=[e[1] for e in self.event_types]
        )[0]
        
        # Generate or reuse session (simulate 100 concurrent users)
        session_id = f"session_{random.randint(1, 100)}"
        
        # Get or create session data
        if session_id not in self.active_sessions:
            self.active_sessions[session_id] = {
                'user_id': random.randint(1, 10000),
                'device': random.choice(['desktop', 'mobile', 'tablet']),
                'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
                'os': random.choice(['windows', 'macos', 'linux', 'ios', 'android']),
                'location': self.faker.city(),
                'start_time': datetime.now().isoformat()
            }
        
        session_data = self.active_sessions[session_id]
        
        # Base event
        event = {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'session_id': session_id,
            'user_id': session_data['user_id'],
            'device_type': session_data['device'],
            'browser': session_data['browser'],
            'os': session_data['os'],
            'location': session_data['location'],
        }
        
        # Add event-specific data
        if event_type == 'page_view':
            event['page_url'] = random.choice(self.pages)
            event['referrer'] = random.choice(self.pages + ['direct', 'google', 'facebook', 'instagram'])
            event['duration_seconds'] = random.randint(5, 300)
            
        elif event_type == 'product_click':
            event['product_id'] = random.randint(1, 194)
            event['product_category'] = random.choice(self.product_categories)
            event['page_url'] = '/products'
            
        elif event_type == 'add_to_cart':
            event['product_id'] = random.randint(1, 194)
            event['product_category'] = random.choice(self.product_categories)
            event['quantity'] = random.randint(1, 3)
            event['price'] = round(random.uniform(10, 500), 2)
            
        elif event_type == 'remove_from_cart':
            event['product_id'] = random.randint(1, 194)
            
        elif event_type == 'search':
            event['search_query'] = random.choice(self.search_terms)
            event['results_count'] = random.randint(0, 100)
            
        elif event_type == 'checkout_start':
            event['cart_value'] = round(random.uniform(50, 500), 2)
            event['items_count'] = random.randint(1, 5)
            
        elif event_type == 'purchase':
            event['order_id'] = f"ORD-{datetime.now().strftime('%Y%m%d')}-{random.randint(1, 99999):05d}"
            event['total_amount'] = round(random.uniform(50, 1000), 2)
            event['items_count'] = random.randint(1, 5)
            event['payment_method'] = random.choice(['credit_card', 'paypal', 'apple_pay', 'google_pay'])
        
        return event
    
    def produce_events(self, events_per_second=100, duration_seconds=None):
        """
        Produce clickstream events continuously
        
        Args:
            events_per_second: Rate of event generation (default: 10)
            duration_seconds: How long to run (None = forever)
        """
        
        print("=" * 70)
        print("Clickstream Producer Started")
        print("=" * 70)
        print(f"   Kafka broker: {self.producer.config['bootstrap_servers']}")
        print(f"   Target topic: clickstream_events")
        print(f"   Events/sec: {events_per_second}")
        print(f"   Duration: {'Infinite (Ctrl+C to stop)' if duration_seconds is None else f'{duration_seconds} seconds'}")
        print("=" * 70)
        print()
        
        event_count = 0
        start_time = time.time()
        
        try:
            while True:
                # Generate event
                event = self.generate_event()
                
                # Send to Kafka
                try:
                    future = self.producer.send('clickstream_events', value=event)
                    # Optional: wait for confirmation (slows down but ensures delivery)
                    # future.get(timeout=10)
                    
                    event_count += 1
                    
                    # Log progress every 100 events
                    if event_count % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = event_count / elapsed
                        print(f"Sent: {event_count:,} events | Rate: {rate:.1f}/sec | Last: {event['event_type']}")
                    
                except Exception as e:
                    print(f"Error sending event: {e}")
                
                # Sleep to maintain desired rate
                time.sleep(1.0 / events_per_second)
                
                # Check duration
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                    
        except KeyboardInterrupt:
            print("\n")
            print("Stopping producer...")
        
        except Exception as e:
            print(f"\n Error: {e}")
        
        finally:
            # Flush and close
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            print("\n" + "=" * 70)
            print("Producer Stopped")
            print("=" * 70)
            print(f"   Total events: {event_count:,}")
            print(f"   Duration: {elapsed:.1f} seconds")
            if elapsed > 0:
                print(f"   Average rate: {event_count/elapsed:.1f} events/sec")
            print("=" * 70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate clickstream events')
    parser.add_argument('--rate', type=int, default=10, help='Events per second (default: 10)')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds (default: infinite)')
    parser.add_argument('--broker', type=str, default='localhost:9093', help='Kafka broker (default: localhost:9093)')
    
    args = parser.parse_args()
    
    # Create producer
    producer = ClickstreamProducer(kafka_broker=args.broker)
    
    # Produce events
    producer.produce_events(
        events_per_second=args.rate,
        duration_seconds=args.duration
    )