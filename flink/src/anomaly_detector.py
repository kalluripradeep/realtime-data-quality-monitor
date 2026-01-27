"""
ML-based Anomaly Detection for Quality Metrics
Uses Isolation Forest to detect unusual quality patterns
"""
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import psycopg2
from datetime import datetime, timedelta
import logging
import json
import time
from collections import deque
from src.explainability import AnomalyExplainer
from src.drift_detector import DriftDetector
from src.lstm_detector import LSTMAnomalyDetector
from src.autoencoder_detector import AutoencoderDetector
from src.ensemble_detector import EnsembleDetector
from src.performance_monitor import PerformanceMonitor

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    ML-based anomaly detector using Isolation Forest algorithm.
    Detects unusual patterns in quality metrics.
    """
    
    def __init__(self, contamination=0.1):
        """
        Initialize anomaly detector.
        
        Args:
            contamination: Expected proportion of anomalies (0.1 = 10%)
        """
        self.contamination = contamination
        self.model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_columns = [
            'completeness_score',
            'timeliness_score', 
            'accuracy_score',
            'consistency_score',
            'uniqueness_score',
            'validity_score',
            'issue_rate'
        ]
        
        # Explainability
        self.explainer = None
        
        # Drift detection
        self.drift_detector = DriftDetector(reference_window_hours=24)
        self.last_drift_check = None
        
        # Advanced ML models
        self.lstm_detector = LSTMAnomalyDetector(sequence_length=60)
        self.autoencoder = AutoencoderDetector(encoding_dim=4)
        self.ensemble = None
        
        # Performance monitoring
        self.performance_monitor = PerformanceMonitor(window_size=100)
        
        # Sequence buffer for LSTM
        self.sequence_buffer = deque(maxlen=60)
        
        logger.info(f"AnomalyDetector initialized with contamination={contamination}")
    
    def get_training_data(self, conn, hours=24):
        """
        Get historical data for training.
        
        Args:
            conn: Database connection
            hours: Hours of historical data to use
            
        Returns:
            DataFrame with training data
        """
        try:
            query = """
                SELECT 
                    window_end,
                    completeness_score,
                    timeliness_score,
                    accuracy_score,
                    overall_score,
                    total_records,
                    issues_found,
                    CAST(issues_found AS FLOAT) / NULLIF(total_records, 0) * 100 as issue_rate
                FROM quality_stats
                WHERE window_end > NOW() - INTERVAL '%s hours'
                ORDER BY window_end ASC
            """
            
            df = pd.read_sql_query(query, conn, params=(hours,))
            
            # Get consistency, uniqueness, validity from quality_metrics
            metrics_query = """
                SELECT 
                    DATE_TRUNC('minute', timestamp) as window_time,
                    AVG(CASE WHEN metric_name = 'consistency_score' THEN metric_value END) as consistency_score,
                    AVG(CASE WHEN metric_name = 'uniqueness_score' THEN metric_value END) as uniqueness_score,
                    AVG(CASE WHEN metric_name = 'validity_score' THEN metric_value END) as validity_score
                FROM quality_metrics
                WHERE timestamp > NOW() - INTERVAL '%s hours'
                GROUP BY DATE_TRUNC('minute', timestamp)
                ORDER BY window_time ASC
            """
            
            df_metrics = pd.read_sql_query(metrics_query, conn, params=(hours,))
            
            # Merge the dataframes
            if not df_metrics.empty:
                df['window_time'] = pd.to_datetime(df['window_end']).dt.floor('min')
                df_metrics['window_time'] = pd.to_datetime(df_metrics['window_time'])
                df = df.merge(df_metrics, on='window_time', how='left')
            
            # Fill missing values with mean
            for col in ['consistency_score', 'uniqueness_score', 'validity_score']:
                if col in df.columns:
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    df[col] = 100.0  # Default high score if no data
            
            logger.info(f"Retrieved {len(df)} records for training")
            return df
            
        except Exception as e:
            logger.error(f"Error getting training data: {e}")
            return pd.DataFrame()
    
    def train(self, conn, hours=24, min_samples=50):
        """
        Train the anomaly detection model.
        
        Args:
            conn: Database connection
            hours: Hours of historical data
            min_samples: Minimum samples needed for training
            
        Returns:
            True if training successful, False otherwise
        """
        try:
            df = self.get_training_data(conn, hours)
            
            if len(df) < min_samples:
                logger.warning(f"Not enough data for training: {len(df)} < {min_samples}")
                return False
            
            # Select features for training
            X = df[self.feature_columns].values
            
            # Remove any rows with NaN
            mask = ~np.isnan(X).any(axis=1)
            X = X[mask]
            
            if len(X) < min_samples:
                logger.warning(f"Not enough valid data after cleaning: {len(X)} < {min_samples}")
                return False
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train model
            self.model.fit(X_scaled)
            self.is_trained = True
            
            logger.info(f"Model trained successfully on {len(X)} samples")
            
            # Initialize explainer after training
            try:
                self.explainer = AnomalyExplainer(self.model, self.feature_columns)
                # Use subset of training data as background
                background_size = min(100, len(df))
                background_data = df[self.feature_columns].sample(n=background_size)
                self.explainer.create_explainer(background_data)
                logger.info("✅ Explainability module initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize explainer: {e}")
            
            # Set reference data for drift detection
            try:
                self.drift_detector.set_reference_data(conn, self.feature_columns)
                logger.info("✅ Drift detector initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize drift detector: {e}")
            
            # Train LSTM
            try:
                logger.info("Training LSTM detector...")
                if self.lstm_detector.train(conn, hours=hours, min_samples=200):
                    logger.info("✅ LSTM detector trained successfully")
                else:
                    logger.warning("⚠️ LSTM training skipped - insufficient data")
            except Exception as e:
                logger.warning(f"LSTM training failed: {e}")
            
            # Train Autoencoder
            try:
                logger.info("Training Autoencoder...")
                if self.autoencoder.train(conn, hours=hours, min_samples=100):
                    logger.info("✅ Autoencoder trained successfully")
                else:
                    logger.warning("⚠️ Autoencoder training skipped - insufficient data")
            except Exception as e:
                logger.warning(f"Autoencoder training failed: {e}")
            
            # Initialize ensemble if all models trained
            if self.is_trained and self.lstm_detector.is_trained and self.autoencoder.is_trained:
                self.ensemble = EnsembleDetector(self, self.lstm_detector, self.autoencoder)
                logger.info("✅ Ensemble detector initialized with all 3 models")
            else:
                logger.info("ℹ️  Ensemble detector not initialized - some models not trained")
            
            return True
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
            return False
    
    def predict(self, data):
        """
        Predict if data point is anomaly.
        
        Args:
            data: Dictionary with quality metrics
            
        Returns:
            Tuple of (is_anomaly, anomaly_score)
            is_anomaly: True if anomaly detected
            anomaly_score: Anomaly score (-1 to 1, lower = more anomalous)
        """
        if not self.is_trained:
            logger.warning("Model not trained yet")
            return False, 0.0
        
        try:
            # Extract features
            features = [data.get(col, 100.0) for col in self.feature_columns]
            X = np.array(features).reshape(1, -1)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Predict
            prediction = self.model.predict(X_scaled)[0]  # -1 = anomaly, 1 = normal
            score = self.model.score_samples(X_scaled)[0]  # Lower = more anomalous
            
            is_anomaly = (prediction == -1)
            
            if is_anomaly:
                logger.warning(f"Anomaly detected! Score: {score:.3f}")
            
            return is_anomaly, float(score)
            
        except Exception as e:
            logger.error(f"Error predicting anomaly: {e}")
            return False, 0.0
    
    def predict_with_explanation(self, data):
        """
        Predict anomaly with explanation.
        
        Args:
            data: Dictionary with feature values
            
        Returns:
            Tuple of (is_anomaly, anomaly_score, explanation)
        """
        # Standard prediction
        is_anomaly, anomaly_score = self.predict(data)
        
        # Get explanation if anomaly detected
        explanation = None
        if is_anomaly and self.explainer:
            try:
                explanation = self.explainer.explain_anomaly(data)
            except Exception as e:
                logger.error(f"Explanation failed: {e}")
                explanation = {'error': str(e)}
        
        return is_anomaly, anomaly_score, explanation
    
    def predict_ensemble(self, data):
        """
        Predict using ensemble of all models with performance tracking.
        
        Args:
            data: Dictionary with feature values
            
        Returns:
            Tuple of (is_anomaly, ensemble_score, details)
        """
        # Add to sequence buffer
        self.sequence_buffer.append(data)
        
        # Use ensemble if available
        if self.ensemble:
            start_time = time.time()
            
            # Get ensemble prediction
            is_anomaly, ensemble_score, details = self.ensemble.predict(
                data, 
                list(self.sequence_buffer) if len(self.sequence_buffer) >= 60 else None
            )
            
            # Record performance
            latency_ms = (time.time() - start_time) * 1000
            self.performance_monitor.record_prediction('ensemble', latency_ms, is_anomaly)
            
            return is_anomaly, ensemble_score, details
        
        # Fallback to single model with performance tracking
        else:
            start_time = time.time()
            is_anomaly, score = self.predict(data)
            latency_ms = (time.time() - start_time) * 1000
            self.performance_monitor.record_prediction('isolation_forest', latency_ms, is_anomaly)
            
            return is_anomaly, score, {'model': 'isolation_forest_only'}
    
    def get_performance_metrics(self):
        """Get performance metrics from monitor."""
        return self.performance_monitor.get_metrics()
    
    def log_performance_summary(self):
        """Log performance summary."""
        self.performance_monitor.log_summary()
    
    def save_performance_metrics(self, conn):
        """Save performance metrics to database."""
        self.performance_monitor.save_metrics(conn)
    
    def check_drift(self, conn, hours=1):
        """
        Check for concept drift in recent data.
        
        Args:
            conn: Database connection
            hours: Hours of recent data to check
            
        Returns:
            Drift detection results
        """
        try:
            # Get recent data for each feature
            current_data = {}
            cutoff = datetime.now() - timedelta(hours=hours)
            
            for feature in self.feature_columns:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT metric_value 
                    FROM quality_metrics 
                    WHERE metric_name = %s 
                      AND timestamp > %s
                """, (feature, cutoff))
                
                values = [row[0] for row in cursor.fetchall()]
                if len(values) >= 30:
                    current_data[feature] = values
                
                cursor.close()
            
            # Detect drift
            drift_results = self.drift_detector.detect_drift(current_data)
            self.last_drift_check = datetime.now()
            
            return drift_results
            
        except Exception as e:
            logger.error(f"Drift check failed: {e}")
            return {'error': str(e)}
    
    def save_anomaly(self, conn, data, anomaly_score):
        """
        Save detected anomaly to database.
        
        Args:
            conn: Database connection
            data: Quality metrics data
            anomaly_score: Anomaly score from model
        """
        try:
            cursor = conn.cursor()
            
            # Create anomalies table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quality_anomalies (
                    id SERIAL PRIMARY KEY,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    anomaly_score FLOAT,
                    completeness_score FLOAT,
                    timeliness_score FLOAT,
                    accuracy_score FLOAT,
                    consistency_score FLOAT,
                    uniqueness_score FLOAT,
                    validity_score FLOAT,
                    issue_rate FLOAT,
                    description TEXT
                )
            """)
            
            # Insert anomaly
            description = f"Anomaly detected with score {anomaly_score:.3f}"
            
            cursor.execute("""
                INSERT INTO quality_anomalies (
                    anomaly_score, completeness_score, timeliness_score,
                    accuracy_score, consistency_score, uniqueness_score,
                    validity_score, issue_rate, description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                anomaly_score,
                data.get('completeness_score', 0),
                data.get('timeliness_score', 0),
                data.get('accuracy_score', 0),
                data.get('consistency_score', 0),
                data.get('uniqueness_score', 0),
                data.get('validity_score', 0),
                data.get('issue_rate', 0),
                description
            ))
            
            conn.commit()
            cursor.close()
            
            logger.info(f"Anomaly saved to database with score {anomaly_score:.3f}")
            
        except Exception as e:
            logger.error(f"Error saving anomaly: {e}")
            conn.rollback()


def retrain_model(detector, conn, hours=24):
    """
    Periodically retrain the model with fresh data.
    
    Args:
        detector: AnomalyDetector instance
        conn: Database connection
        hours: Hours of data to use
    """
    try:
        logger.info("Starting model retraining...")
        success = detector.train(conn, hours=hours)
        
        if success:
            logger.info("Model retrained successfully")
        else:
            logger.warning("Model retraining failed")
            
        return success
        
    except Exception as e:
        logger.error(f"Error retraining model: {e}")
        return False