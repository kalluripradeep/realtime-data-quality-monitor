"""
Data quality checker module.
Implements various quality checks for streaming data.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Any


class QualityChecker:
    """Performs various data quality checks on orders."""
    
    def __init__(self, max_latency_seconds=300):
        self.max_latency_seconds = max_latency_seconds
        self.required_fields = [
            'order_id', 'customer_id', 'product_id', 
            'quantity', 'price', 'total_amount', 'timestamp'
        ]
    
    def check_completeness(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check for missing or null values.
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list of field names with issues,
                'missing_count': int
            }
        """
        issues = []
        
        for field in self.required_fields:
            if field not in order or order[field] is None:
                issues.append(f"missing_{field}")
        
        missing_count = len(issues)
        total_fields = len(self.required_fields)
        score = ((total_fields - missing_count) / total_fields) * 100
        
        return {
            'dimension': 'completeness',
            'score': round(score, 2),
            'issues': issues,
            'missing_count': missing_count
        }
    
    def check_timeliness(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if data arrived within acceptable time window.
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list,
                'latency_seconds': float
            }
        """
        issues = []
        
        try:
            # Parse timestamp
            order_time = datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))
            current_time = datetime.utcnow().replace(tzinfo=order_time.tzinfo)
            
            # Calculate latency
            latency = (current_time - order_time).total_seconds()
            
            # Check if within acceptable range
            if latency > self.max_latency_seconds:
                issues.append(f"high_latency_{int(latency)}s")
            
            if latency < 0:
                issues.append("future_timestamp")
                latency = abs(latency)
            
            # Calculate score (100 if within limit, decreases as latency increases)
            if latency <= self.max_latency_seconds:
                score = 100.0
            else:
                score = max(0, 100 - ((latency - self.max_latency_seconds) / 60))
            
        except Exception as e:
            issues.append(f"invalid_timestamp")
            latency = 0
            score = 0
        
        return {
            'dimension': 'timeliness',
            'score': round(score, 2),
            'issues': issues,
            'latency_seconds': latency
        }
    
    def check_accuracy(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if values are within valid ranges and correct types.
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list,
                'invalid_count': int
            }
        """
        issues = []
        checks = 0
        passed = 0
        
        # Check quantity
        checks += 1
        if 'quantity' in order and order['quantity'] is not None:
            if isinstance(order['quantity'], (int, float)) and order['quantity'] > 0:
                passed += 1
            else:
                issues.append(f"invalid_quantity_{order['quantity']}")
        
        # Check price
        checks += 1
        if 'price' in order and order['price'] is not None:
            if isinstance(order['price'], (int, float)) and order['price'] > 0:
                passed += 1
            else:
                issues.append(f"invalid_price_{order['price']}")
        
        # Check total_amount
        checks += 1
        if 'total_amount' in order and order['total_amount'] is not None:
            if isinstance(order['total_amount'], (int, float)) and order['total_amount'] > 0:
                passed += 1
            else:
                issues.append(f"invalid_total_amount_{order['total_amount']}")
        
        # Check product_id format
        checks += 1
        if 'product_id' in order and order['product_id'] is not None:
            if isinstance(order['product_id'], str) and order['product_id'].startswith('PROD-'):
                passed += 1
            else:
                issues.append(f"invalid_product_id_format")
        
        score = (passed / checks) * 100 if checks > 0 else 0
        
        return {
            'dimension': 'accuracy',
            'score': round(score, 2),
            'issues': issues,
            'invalid_count': len(issues)
        }
    
    def check_all(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all quality checks and return aggregated results.
        
        Returns:
            dict: {
                'order_id': str,
                'completeness': dict,
                'timeliness': dict,
                'accuracy': dict,
                'overall_score': float,
                'has_issues': bool,
                'issue_count': int
            }
        """
        completeness = self.check_completeness(order)
        timeliness = self.check_timeliness(order)
        accuracy = self.check_accuracy(order)
        
        # Calculate overall score (weighted average)
        overall_score = (
            completeness['score'] * 0.4 +  # 40% weight
            timeliness['score'] * 0.3 +     # 30% weight
            accuracy['score'] * 0.3          # 30% weight
        )
        
        # Aggregate all issues
        all_issues = (
            completeness['issues'] + 
            timeliness['issues'] + 
            accuracy['issues']
        )
        
        return {
            'order_id': order.get('order_id', 'unknown'),
            'timestamp': order.get('timestamp'),
            'completeness': completeness,
            'timeliness': timeliness,
            'accuracy': accuracy,
            'overall_score': round(overall_score, 2),
            'has_issues': len(all_issues) > 0,
            'issue_count': len(all_issues),
            'issues': all_issues
        }


# Test the quality checker
if __name__ == "__main__":
    checker = QualityChecker(max_latency_seconds=300)
    
    # Test with clean order
    clean_order = {
        'order_id': 'ORD-TEST1',
        'customer_id': 'CUST-001',
        'product_id': 'PROD-001',
        'quantity': 2,
        'price': 99.99,
        'total_amount': 199.98,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    result = checker.check_all(clean_order)
    print("Clean Order Quality Check:")
    print(f"  Overall Score: {result['overall_score']}")
    print(f"  Has Issues: {result['has_issues']}")
    print()
    
    # Test with problematic order
    bad_order = {
        'order_id': 'ORD-TEST2',
        'customer_id': None,  # Missing
        'product_id': 'PROD-001',
        'quantity': -5,  # Invalid
        'price': 99.99,
        'total_amount': -499.95,  # Invalid
        'timestamp': (datetime.utcnow() - timedelta(hours=6)).isoformat() + 'Z'  # Late
    }
    
    result = checker.check_all(bad_order)
    print("Problematic Order Quality Check:")
    print(f"  Overall Score: {result['overall_score']}")
    print(f"  Has Issues: {result['has_issues']}")
    print(f"  Issues: {result['issues']}")