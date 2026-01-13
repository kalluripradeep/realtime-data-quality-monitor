"""
PostgreSQL writer for quality metrics.
"""
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, Any, List
import config


class PostgresWriter:
    """Writes quality metrics and issues to PostgreSQL."""
    
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD
            )
            print("✅ Connected to PostgreSQL")
        except Exception as e:
            print(f"❌ Failed to connect to PostgreSQL: {e}")
            raise
    
    def get_connection(self):
        """Get database connection for external use (like ML training)."""
        return self.conn
    
    def write_metric(self, metric_name: str, metric_value: float, 
                     dimension: str, details: Dict[str, Any] = None):
        """
        Write a single quality metric.
        
        Args:
            metric_name: Name of the metric (e.g., 'completeness_score')
            metric_value: Numeric value of the metric
            dimension: Quality dimension (completeness, timeliness, accuracy)
            details: Additional JSON details
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO quality_metrics 
                (metric_name, metric_value, dimension, details)
                VALUES (%s, %s, %s, %s)
            """, (metric_name, metric_value, dimension, Json(details or {})))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error writing metric: {e}")
            self.conn.rollback()
    
    def write_issue(self, order_id: str, issue_type: str, 
                    issue_description: str, severity: str,
                    order_data: Dict[str, Any]):
        """
        Write a quality issue.
        
        Args:
            order_id: Order identifier
            issue_type: Type of issue (e.g., 'missing_customer_id')
            issue_description: Human-readable description
            severity: Severity level (low, medium, high, critical)
            order_data: Full order data as JSON
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO quality_issues 
                (order_id, issue_type, issue_description, severity, order_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, issue_type, issue_description, severity, Json(order_data)))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(f"Error writing issue: {e}")
            self.conn.rollback()
    
    def write_stats(self, window_start: datetime, window_end: datetime,
                    total_records: int, clean_records: int, issues_found: int,
                    completeness_score: float, timeliness_score: float,
                    accuracy_score: float, overall_score: float):
        """
        Write aggregated statistics for a time window.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO quality_stats 
                (window_start, window_end, total_records, clean_records, issues_found,
                 completeness_score, timeliness_score, accuracy_score, overall_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (window_start, window_end, total_records, clean_records, issues_found,
                  completeness_score, timeliness_score, accuracy_score, overall_score))
            self.conn.commit()
            cursor.close()
            print(f"✅ Wrote stats: {total_records} records, {issues_found} issues, score: {overall_score:.2f}")
        except Exception as e:
            print(f"Error writing stats: {e}")
            self.conn.rollback()
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Closed PostgreSQL connection")