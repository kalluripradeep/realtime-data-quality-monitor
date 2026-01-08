"""
Data quality checker module.
Implements various quality checks for streaming data.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Any, Set
import logging

logger = logging.getLogger(__name__)


class QualityChecker:
    """Performs various data quality checks on orders."""
    
    def __init__(self, max_latency_seconds=300):
        self.max_latency_seconds = max_latency_seconds
        self.required_fields = [
            'order_id', 'customer_id', 'product_id', 
            'quantity', 'price', 'total_amount', 'timestamp'
        ]
        
        # Initialize alert manager
        self.alert_manager = None  # Will be set by kafka_consumer
        
        # Track recent order IDs for uniqueness check (sliding window)
        self.recent_order_ids: Set[str] = set()
        self.max_recent_orders = 10000  # Keep last 10k order IDs
        
        # Track stats for alerting
        self.window_stats = {
            'total_records': 0,
            'clean_records': 0,
            'issues_found': 0,
            'critical_issues': 0
        }
    
    def set_alert_manager(self, alert_manager):
        """Set the alert manager instance"""
        self.alert_manager = alert_manager
    
    def reset_window_stats(self):
        """Reset stats for new window"""
        self.window_stats = {
            'total_records': 0,
            'clean_records': 0,
            'issues_found': 0,
            'critical_issues': 0
        }
    
    def update_window_stats(self, quality_result: Dict[str, Any]):
        """Update window statistics"""
        self.window_stats['total_records'] += 1
        
        if quality_result['has_issues']:
            self.window_stats['issues_found'] += 1
            
            # Check for critical issues
            if quality_result['overall_score'] < 50:
                self.window_stats['critical_issues'] += 1
        else:
            self.window_stats['clean_records'] += 1
    
    def check_window_alerts(self):
        """Check if window stats trigger any alerts"""
        if self.alert_manager is None:
            return
        
        total = self.window_stats['total_records']
        if total == 0:
            return
        
        # Calculate overall quality score for window
        clean_percentage = (self.window_stats['clean_records'] / total) * 100
        
        # Alert on low quality
        self.alert_manager.check_quality_score(clean_percentage, self.window_stats)
        
        # Alert on high issue rate
        self.alert_manager.check_high_issue_rate(
            self.window_stats['issues_found'], 
            total
        )
        
        # Alert on critical issues
        self.alert_manager.check_critical_issues(
            self.window_stats['critical_issues']
        )
    
    def _add_to_recent_orders(self, order_id: str):
        """Add order ID to recent set and maintain size limit"""
        if order_id:
            self.recent_order_ids.add(order_id)
            
            # Keep set size manageable
            if len(self.recent_order_ids) > self.max_recent_orders:
                # Remove oldest (approximate - removes random items)
                for _ in range(1000):
                    self.recent_order_ids.pop()
    
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
    
    def check_consistency(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if data formats are consistent.
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list,
                'inconsistent_count': int
            }
        """
        issues = []
        checks = 0
        passed = 0
        
        # Check order_id format (should be ORD-XXXXX)
        checks += 1
        if 'order_id' in order and order['order_id'] is not None:
            if isinstance(order['order_id'], str) and order['order_id'].startswith('ORD-'):
                passed += 1
            else:
                issues.append(f"inconsistent_order_id_format")
        
        # Check customer_id format (should be CUST-XXXXX)
        checks += 1
        if 'customer_id' in order and order['customer_id'] is not None:
            if isinstance(order['customer_id'], str) and order['customer_id'].startswith('CUST-'):
                passed += 1
            else:
                issues.append(f"inconsistent_customer_id_format")
        
        # Check timestamp format
        checks += 1
        if 'timestamp' in order and order['timestamp'] is not None:
            try:
                datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))
                passed += 1
            except:
                issues.append("inconsistent_timestamp_format")
        
        score = (passed / checks) * 100 if checks > 0 else 0
        
        return {
            'dimension': 'consistency',
            'score': round(score, 2),
            'issues': issues,
            'inconsistent_count': len(issues)
        }
    
    def check_uniqueness(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if order is unique (not a duplicate).
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list,
                'is_duplicate': bool
            }
        """
        issues = []
        order_id = order.get('order_id')
        
        is_duplicate = order_id in self.recent_order_ids if order_id else False
        
        if is_duplicate:
            issues.append(f"duplicate_order_{order_id}")
        
        score = 0.0 if is_duplicate else 100.0
        
        return {
            'dimension': 'uniqueness',
            'score': score,
            'issues': issues,
            'is_duplicate': is_duplicate
        }
    
    def check_validity(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check business rule validity.
        
        Returns:
            dict: {
                'score': float (0-100),
                'issues': list,
                'invalid_rules': int
            }
        """
        issues = []
        checks = 0
        passed = 0
        
        # Rule 1: Total amount should equal quantity * price
        checks += 1
        if all(k in order and order[k] is not None for k in ['quantity', 'price', 'total_amount']):
            expected_total = order['quantity'] * order['price']
            actual_total = order['total_amount']
            
            # Allow small floating point differences
            if abs(expected_total - actual_total) < 0.01:
                passed += 1
            else:
                issues.append(f"invalid_calculation_expected_{expected_total:.2f}_got_{actual_total:.2f}")
        
        # Rule 2: Quantity should be reasonable (1-1000)
        checks += 1
        if 'quantity' in order and order['quantity'] is not None:
            if 1 <= order['quantity'] <= 1000:
                passed += 1
            else:
                issues.append(f"invalid_quantity_range_{order['quantity']}")
        
        # Rule 3: Price should be reasonable ($0.01 - $10,000)
        checks += 1
        if 'price' in order and order['price'] is not None:
            if 0.01 <= order['price'] <= 10000:
                passed += 1
            else:
                issues.append(f"invalid_price_range_{order['price']}")
        
        score = (passed / checks) * 100 if checks > 0 else 0
        
        return {
            'dimension': 'validity',
            'score': round(score, 2),
            'issues': issues,
            'invalid_rules': len(issues)
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
                'consistency': dict,
                'uniqueness': dict,
                'validity': dict,
                'overall_score': float,
                'has_issues': bool,
                'issue_count': int
            }
        """
        completeness = self.check_completeness(order)
        timeliness = self.check_timeliness(order)
        accuracy = self.check_accuracy(order)
        consistency = self.check_consistency(order)
        uniqueness = self.check_uniqueness(order)
        validity = self.check_validity(order)
        
        # Add order to recent set for future uniqueness checks
        self._add_to_recent_orders(order.get('order_id'))
        
        # Calculate overall score (weighted average)
        overall_score = (
            completeness['score'] * 0.25 +  # 25% weight
            timeliness['score'] * 0.15 +     # 15% weight
            accuracy['score'] * 0.20 +       # 20% weight
            consistency['score'] * 0.15 +    # 15% weight
            uniqueness['score'] * 0.10 +     # 10% weight
            validity['score'] * 0.15          # 15% weight
        )
        
        # Aggregate all issues
        all_issues = (
            completeness['issues'] + 
            timeliness['issues'] + 
            accuracy['issues'] +
            consistency['issues'] +
            uniqueness['issues'] +
            validity['issues']
        )
        
        result = {
            'order_id': order.get('order_id', 'unknown'),
            'timestamp': order.get('timestamp'),
            'completeness': completeness,
            'timeliness': timeliness,
            'accuracy': accuracy,
            'consistency': consistency,
            'uniqueness': uniqueness,
            'validity': validity,
            'overall_score': round(overall_score, 2),
            'has_issues': len(all_issues) > 0,
            'issue_count': len(all_issues),
            'issues': all_issues
        }
        
        # Update window stats for alerting
        self.update_window_stats(result)
        
        return result


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
    print(f"  Completeness: {result['completeness']['score']}")
    print(f"  Timeliness: {result['timeliness']['score']}")
    print(f"  Accuracy: {result['accuracy']['score']}")
    print(f"  Consistency: {result['consistency']['score']}")
    print(f"  Uniqueness: {result['uniqueness']['score']}")
    print(f"  Validity: {result['validity']['score']}")
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