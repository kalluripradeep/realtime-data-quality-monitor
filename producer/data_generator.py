"""
Data generator that creates e-commerce orders with intentional quality issues.
"""
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

class OrderGenerator:
    """Generate e-commerce orders with configurable quality issues."""
    
    PRODUCTS = [
        {"id": "PROD-001", "name": "Laptop", "price": 999.99},
        {"id": "PROD-002", "name": "Mouse", "price": 29.99},
        {"id": "PROD-003", "name": "Keyboard", "price": 79.99},
        {"id": "PROD-004", "name": "Monitor", "price": 299.99},
        {"id": "PROD-005", "name": "Headphones", "price": 149.99},
    ]
    
    def __init__(self, issue_percentage=0.3):
        self.issue_percentage = issue_percentage
    
    def generate_clean_order(self):
        product = random.choice(self.PRODUCTS)
        
        return {
            "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product_id": product["id"],
            "product_name": product["name"],
            "quantity": random.randint(1, 5),
            "price": product["price"],
            "total_amount": round(product["price"] * random.randint(1, 5), 2),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal"]),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": "pending"
        }
    
    def introduce_quality_issues(self, order):
        issue_type = random.choice([
            'missing_customer',
            'invalid_quantity',
            'invalid_price',
            'delayed_timestamp',
            'negative_amount'
        ])
        
        if issue_type == 'missing_customer':
            order['customer_id'] = None
        elif issue_type == 'invalid_quantity':
            order['quantity'] = random.choice([-1, 0, 999])
        elif issue_type == 'invalid_price':
            order['price'] = -99.99
        elif issue_type == 'delayed_timestamp':
            old_time = datetime.utcnow() - timedelta(hours=2)
            order['timestamp'] = old_time.isoformat() + "Z"
        elif issue_type == 'negative_amount':
            order['total_amount'] = -abs(order['total_amount'])
        
        return order
    
    def generate_order(self):
        order = self.generate_clean_order()
        
        if random.random() < self.issue_percentage:
            order = self.introduce_quality_issues(order)
            order['has_quality_issue'] = True
        else:
            order['has_quality_issue'] = False
        
        return order