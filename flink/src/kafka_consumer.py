"""
Kafka consumer with real-time quality checking and alerting.
Simplified version without Flink for easier deployment.
"""
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from collections import defaultdict
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import config
from src.quality_checker import QualityChecker
from src.postgres_writer import PostgresWriter
from src.alerting import AlertManager


class QualityMonitor:
    """Real-time quality monitoring for streaming orders with alerting."""
    
    def __init__(self):
        self.checker = QualityChecker(max_latency_seconds=config.MAX_LATENCY_SECONDS)
        self.writer = PostgresWriter()
        
        # Initialize alert manager
        self.alert_manager = AlertManager(
            quality_threshold=90.0,  # Alert if quality drops below 90%
            issue_rate_threshold=40.0,  # Alert if more than 40% have issues
            critical_issue_threshold=100,  # Alert if more than 100 critical issues
            email_enabled=False,  # Set to True when you configure email
            email_to=None,  # Your email address
            email_from=None,  # Sender email
            smtp_password=None  # Email password or app password
        )
        
        # Set alert manager in quality checker
        self.checker.set_alert_manager(self.alert_manager)
        
        self.consumer = self._create_consumer()
        
        # Window tracking
        self.window_size = config.WINDOW_SIZE_SECONDS
        self.window_start = datetime.utcnow()
        self.window_data = {
            'total': 0,
            'clean': 0,
            'issues': 0,
            'completeness_scores': [],
            'timeliness_scores': [],
            'accuracy_scores': [],
            'overall_scores': []
        }
    
    def _create_consumer(self):
        """Create Kafka consumer with retry logic."""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    config.KAFKA_TOPIC,
                    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=config.KAFKA_GROUP_ID,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True
                )
                print(f"✅ Connected to Kafka successfully!")
                self.alert_manager.log_system_health(True, "Kafka connection established")
                return consumer
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"⏳ Waiting for Kafka... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(5)
                else:
                    self.alert_manager.log_system_health(
                        False, 
                        f"Failed to connect to Kafka after {max_retries} attempts: {e}"
                    )
                    raise
    
    def _should_flush_window(self):
        """Check if we should flush the current window."""
        elapsed = (datetime.utcnow() - self.window_start).total_seconds()
        return elapsed >= self.window_size
    
    def _flush_window(self):
        """Calculate and write window statistics with alerting."""
        if self.window_data['total'] == 0:
            return
        
        window_end = datetime.utcnow()
        
        # Calculate average scores
        avg_completeness = sum(self.window_data['completeness_scores']) / len(self.window_data['completeness_scores']) if self.window_data['completeness_scores'] else 0
        avg_timeliness = sum(self.window_data['timeliness_scores']) / len(self.window_data['timeliness_scores']) if self.window_data['timeliness_scores'] else 0
        avg_accuracy = sum(self.window_data['accuracy_scores']) / len(self.window_data['accuracy_scores']) if self.window_data['accuracy_scores'] else 0
        avg_overall = sum(self.window_data['overall_scores']) / len(self.window_data['overall_scores']) if self.window_data['overall_scores'] else 0
        
        # Check for alerts based on window quality checker stats
        self.checker.check_window_alerts()
        
        # Write to database
        self.writer.write_stats(
            window_start=self.window_start,
            window_end=window_end,
            total_records=self.window_data['total'],
            clean_records=self.window_data['clean'],
            issues_found=self.window_data['issues'],
            completeness_score=round(avg_completeness, 2),
            timeliness_score=round(avg_timeliness, 2),
            accuracy_score=round(avg_accuracy, 2),
            overall_score=round(avg_overall, 2)
        )
        
        # Log window completion
        self.alert_manager.log_info(
            f"Window completed: {self.window_data['total']} records processed, "
            f"{self.window_data['clean']} clean, "
            f"{self.window_data['issues']} with issues, "
            f"avg quality: {avg_overall:.2f}%"
        )
        
        # Reset window stats in quality checker
        self.checker.reset_window_stats()
        
        # Reset window
        self.window_start = datetime.utcnow()
        self.window_data = {
            'total': 0,
            'clean': 0,
            'issues': 0,
            'completeness_scores': [],
            'timeliness_scores': [],
            'accuracy_scores': [],
            'overall_scores': []
        }
    
    def process_order(self, order):
        """Process a single order and check quality."""
        try:
            # Run quality checks (now includes alert tracking)
            result = self.checker.check_all(order)
            
            # Update window data
            self.window_data['total'] += 1
            if not result['has_issues']:
                self.window_data['clean'] += 1
            else:
                self.window_data['issues'] += 1
            
            # Collect scores
            self.window_data['completeness_scores'].append(result['completeness']['score'])
            self.window_data['timeliness_scores'].append(result['timeliness']['score'])
            self.window_data['accuracy_scores'].append(result['accuracy']['score'])
            self.window_data['overall_scores'].append(result['overall_score'])
            
            # Write individual metrics
            self.writer.write_metric(
                'completeness_score',
                result['completeness']['score'],
                'completeness',
                result['completeness']
            )
            
            self.writer.write_metric(
                'timeliness_score',
                result['timeliness']['score'],
                'timeliness',
                result['timeliness']
            )
            
            self.writer.write_metric(
                'accuracy_score',
                result['accuracy']['score'],
                'accuracy',
                result['accuracy']
            )
            
            # Write issues if any
            if result['has_issues']:
                for issue in result['issues']:
                    severity = self._determine_severity(issue)
                    self.writer.write_issue(
                        order_id=result['order_id'],
                        issue_type=issue,
                        issue_description=f"Quality issue detected: {issue}",
                        severity=severity,
                        order_data=order
                    )
            
            # Print summary every 10 records
            if self.window_data['total'] % 10 == 0:
                print(f"📊 Processed {self.window_data['total']} orders "
                      f"({self.window_data['clean']} clean, "
                      f"{self.window_data['issues']} with issues)")
        
        except Exception as e:
            print(f"❌ Error processing order: {e}")
            self.alert_manager.log_system_health(False, f"Error processing order: {e}")
    
    def _determine_severity(self, issue: str) -> str:
        """Determine severity level of an issue."""
        if 'missing_customer_id' in issue or 'missing_order_id' in issue:
            return 'critical'
        elif 'invalid' in issue or 'negative' in issue:
            return 'high'
        elif 'high_latency' in issue:
            return 'medium'
        else:
            return 'low'
    
    def run(self):
        """Main processing loop with alerting."""
        print(f"🚀 Starting Quality Monitor with Alerting...")
        print(f"📊 Window size: {self.window_size} seconds")
        print(f"⏰ Max latency: {config.MAX_LATENCY_SECONDS} seconds")
        print(f"🔔 Quality threshold: {self.alert_manager.quality_threshold}%")
        print(f"📧 Email alerts: {'ENABLED' if self.alert_manager.email_enabled else 'DISABLED'}")
        print(f"📝 Alert log: /app/logs/alerts.log\n")
        
        self.alert_manager.log_info("Quality Monitor started successfully")
        
        try:
            for message in self.consumer:
                order = message.value
                
                # Process the order
                self.process_order(order)
                
                # Check if we should flush the window
                if self._should_flush_window():
                    self._flush_window()
                    
        except KeyboardInterrupt:
            print("\n✋ Shutting down gracefully...")
            self.alert_manager.log_info("Quality Monitor shutting down (user interrupt)")
            self._flush_window()  # Flush remaining data
        except Exception as e:
            print(f"\n❌ Error in main loop: {e}")
            self.alert_manager.log_system_health(False, f"Fatal error in main loop: {e}")
            raise
        finally:
            self.consumer.close()
            self.writer.close()
            self.alert_manager.log_info("Quality Monitor stopped")
            print("👋 Quality Monitor stopped")


if __name__ == "__main__":
    monitor = QualityMonitor()
    monitor.run()