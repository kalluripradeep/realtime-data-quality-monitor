"""
Kafka producer that sends orders to Kafka topic.
"""
import json
import time
from kafka import KafkaProducer
from data_generator import OrderGenerator
import config

def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

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
                print(f"Sent {order_count} orders ({issue_count} with issues)")
            
            time.sleep(1.0 / config.ORDERS_PER_SECOND)
            
    except KeyboardInterrupt:
        print(f"\nShutting down... Sent {order_count} total orders")
    finally:
        producer.close()

if __name__ == "__main__":
    main()