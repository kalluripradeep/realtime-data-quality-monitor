"""
Performance Monitoring for ML Models
Tracks inference latency, accuracy, and resource usage
"""
import time
import psutil
import logging
from datetime import datetime
from collections import deque
from typing import Dict

logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """
    Monitors ML model performance metrics.
    Tracks latency, throughput, accuracy, and resources.
    """
    
    def __init__(self, window_size=100):
        """
        Initialize performance monitor.
        
        Args:
            window_size: Number of recent predictions to track
        """
        self.window_size = window_size
        
        # Metrics storage
        self.latencies = {
            'isolation_forest': deque(maxlen=window_size),
            'lstm': deque(maxlen=window_size),
            'autoencoder': deque(maxlen=window_size),
            'ensemble': deque(maxlen=window_size)
        }
        
        self.predictions = {
            'isolation_forest': deque(maxlen=window_size),
            'lstm': deque(maxlen=window_size),
            'autoencoder': deque(maxlen=window_size),
            'ensemble': deque(maxlen=window_size)
        }
        
        self.start_time = datetime.now()
        self.total_predictions = 0
        
        logger.info("PerformanceMonitor initialized")
    
    def record_prediction(self, model_name: str, latency_ms: float, is_anomaly: bool):
        """
        Record a prediction.
        
        Args:
            model_name: Name of the model
            latency_ms: Prediction latency in milliseconds
            is_anomaly: Whether anomaly was detected
        """
        if model_name in self.latencies:
            self.latencies[model_name].append(latency_ms)
            self.predictions[model_name].append(1 if is_anomaly else 0)
            self.total_predictions += 1
    
    def get_metrics(self) -> Dict:
        """
        Get current performance metrics.
        
        Returns:
            Dictionary with performance metrics
        """
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'uptime_hours': (datetime.now() - self.start_time).total_seconds() / 3600,
            'total_predictions': self.total_predictions,
            'models': {}
        }
        
        for model_name in self.latencies.keys():
            if len(self.latencies[model_name]) > 0:
                latencies = list(self.latencies[model_name])
                predictions = list(self.predictions[model_name])
                
                metrics['models'][model_name] = {
                    'avg_latency_ms': sum(latencies) / len(latencies),
                    'min_latency_ms': min(latencies),
                    'max_latency_ms': max(latencies),
                    'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 20 else max(latencies),
                    'anomaly_rate': sum(predictions) / len(predictions) * 100,
                    'predictions_count': len(predictions)
                }
        
        # System resources
        metrics['system'] = {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_mb': psutil.Process().memory_info().rss / 1024 / 1024,
            'memory_percent': psutil.virtual_memory().percent
        }
        
        return metrics
    
    def log_summary(self):
        """Log performance summary."""
        metrics = self.get_metrics()
        
        logger.info("=" * 60)
        logger.info("📊 ML PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Uptime: {metrics['uptime_hours']:.1f} hours")
        logger.info(f"Total Predictions: {metrics['total_predictions']}")
        
        for model_name, model_metrics in metrics['models'].items():
            logger.info(f"\n{model_name.upper()}:")
            logger.info(f"  Avg Latency: {model_metrics['avg_latency_ms']:.2f}ms")
            logger.info(f"  P95 Latency: {model_metrics['p95_latency_ms']:.2f}ms")
            logger.info(f"  Anomaly Rate: {model_metrics['anomaly_rate']:.1f}%")
        
        logger.info(f"\nSYSTEM:")
        logger.info(f"  CPU: {metrics['system']['cpu_percent']:.1f}%")
        logger.info(f"  Memory: {metrics['system']['memory_mb']:.1f}MB ({metrics['system']['memory_percent']:.1f}%)")
        logger.info("=" * 60)
    
    def save_metrics(self, conn):
        """Save metrics to database."""
        try:
            metrics = self.get_metrics()
            
            cursor = conn.cursor()
            
            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ml_performance (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    model_name VARCHAR(50),
                    avg_latency_ms FLOAT,
                    p95_latency_ms FLOAT,
                    anomaly_rate FLOAT,
                    predictions_count INTEGER,
                    cpu_percent FLOAT,
                    memory_mb FLOAT
                )
            """)
            
            # Insert metrics for each model
            for model_name, model_metrics in metrics['models'].items():
                cursor.execute("""
                    INSERT INTO ml_performance (
                        model_name, avg_latency_ms, p95_latency_ms,
                        anomaly_rate, predictions_count, cpu_percent, memory_mb
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    model_name,
                    model_metrics['avg_latency_ms'],
                    model_metrics['p95_latency_ms'],
                    model_metrics['anomaly_rate'],
                    model_metrics['predictions_count'],
                    metrics['system']['cpu_percent'],
                    metrics['system']['memory_mb']
                ))
            
            conn.commit()
            cursor.close()
            
            logger.info("Performance metrics saved to database")
            
        except Exception as e:
            logger.error(f"Failed to save performance metrics: {e}")