"""
Kafka producer that sends orders to Kafka topic.
"""
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from data_generator import OrderGenerator
import config

def create_producer(max_retries=10, retry_delay=5):
    """Create Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=5,
                max_in_flight_requests_per_connection=1
            )
            print("✅ Connected to Kafka successfully!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"⏳ Waiting for Kafka... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                raise

def main():
    print(f"Starting Kafka Producer...")
    print(f"Kafka Server: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"Orders/second: {config.ORDERS_PER_SECOND}")
    print(f"Quality issue rate: {config.QUALITY_ISSUE_PERCENTAGE * 100}%\n")
    
    producer = create_producer()
    generator = OrderGenerator(issue_percentage=config.QUALITY_ISSUE_PERCENTAGE)
    
    order_count = 0
    issue_count = 0
    
    try:
        while True:
            order = generator.generate_order()
            
            # Send to Kafka
            producer.send(
                config.KAFKA_TOPIC,
                key=order['order_id'],
                value=order
            )
            
            order_count += 1
            if order.get('has_quality_issue'):
                issue_count += 1
            
            if order_count % 10 == 0:
                print(f"📦 Sent {order_count} orders ({issue_count} with issues)")
            
            time.sleep(1.0 / config.ORDERS_PER_SECOND)
            
    except KeyboardInterrupt:
        print(f"\n✅ Shutting down... Sent {order_count} total orders")
    finally:
        producer.close()

if __name__ == "__main__":
    main()