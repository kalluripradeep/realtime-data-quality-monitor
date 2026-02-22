"""
LSTM-based Anomaly Detection for Temporal Patterns
Uses sequence prediction to detect anomalies
"""
import numpy as np
import pandas as pd
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta
import logging
import pickle

logger = logging.getLogger(__name__)


class LSTMAnomalyDetector:
    """
    LSTM-based anomaly detector for temporal patterns.
    Predicts next quality scores based on historical sequences.
    """
    
    def __init__(self, sequence_length=60, threshold_percentile=95):
        """
        Initialize LSTM detector.
        
        Args:
            sequence_length: Number of time steps to look back
            threshold_percentile: Percentile for anomaly threshold
        """
        self.sequence_length = sequence_length
        self.threshold_percentile = threshold_percentile
        self.model = None
        self.scaler = MinMaxScaler()
        self.is_trained = False
        self.threshold = None
        self.feature_columns = [
            'completeness_score',
            'timeliness_score',
            'accuracy_score',
            'consistency_score',
            'uniqueness_score',
            'validity_score',
            'issue_rate'
        ]
        
        logger.info(f"LSTMAnomalyDetector initialized (seq_len={sequence_length})")
    
    def build_model(self, n_features):
        """Build LSTM model architecture."""
        model = Sequential([
            LSTM(50, activation='relu', return_sequences=True, 
                 input_shape=(self.sequence_length, n_features)),
            Dropout(0.2),
            LSTM(50, activation='relu'),
            Dropout(0.2),
            Dense(n_features)
        ])
        
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model
    
    def prepare_sequences(self, data):
        """
        Prepare sequences for LSTM training.
        
        Args:
            data: DataFrame with features
            
        Returns:
            X, y arrays for training
        """
        # Scale data
        scaled_data = self.scaler.fit_transform(data)
        
        X, y = [], []
        for i in range(len(scaled_data) - self.sequence_length):
            X.append(scaled_data[i:i + self.sequence_length])
            y.append(scaled_data[i + self.sequence_length])
        
        return np.array(X), np.array(y)
    
    def train(self, conn, hours=1, min_samples=50):
        """
        Train LSTM model on historical data.
        
        Args:
            conn: Database connection
            hours: Hours of historical data
            min_samples: Minimum samples needed
            
        Returns:
            True if training successful
        """
        try:
            # Get historical data
            query = """
                SELECT 
                    timestamp,
                    metric_name,
                    metric_value
                FROM quality_metrics
                WHERE timestamp > NOW() - INTERVAL '%s hours'
                ORDER BY timestamp ASC
            """
            
            df = pd.read_sql_query(query, conn, params=(hours,))
            
            # Pivot to get features as columns
            df_pivot = df.pivot(index='timestamp', columns='metric_name', values='metric_value')
            
            # Ensure all features present
            for col in self.feature_columns:
                if col not in df_pivot.columns:
                    df_pivot[col] = 100.0
            
            # Select only needed columns
            df_pivot = df_pivot[self.feature_columns]
            
            # Fill missing values
            df_pivot = df_pivot.ffill().fillna(100.0)

            # Limit to last 1000 samples for faster training
            if len(df_pivot) > 1000:
                df_pivot = df_pivot.tail(1000)
            if len(df_pivot) < min_samples:
                logger.warning(f"Not enough data for LSTM training: {len(df_pivot)} < {min_samples}")
                return False
            
            # Prepare sequences
            X, y = self.prepare_sequences(df_pivot.values)
            
            if len(X) < 50:
                logger.warning(f"Not enough sequences for training: {len(X)} < 50")
                return False
            
            # Build and train model
            self.model = self.build_model(len(self.feature_columns))
            
            # Train with early stopping
            early_stop = keras.callbacks.EarlyStopping(
                monitor='loss',
                patience=5,
                restore_best_weights=True
            )
            
            self.model.fit(
                X, y,
                epochs=50,
                batch_size=32,
                validation_split=0.2,
                callbacks=[early_stop],
                verbose=0
            )
            
            # Calculate threshold from training errors
            predictions = self.model.predict(X, verbose=0)
            errors = np.mean(np.abs(predictions - y), axis=1)
            self.threshold = np.percentile(errors, self.threshold_percentile)
            
            self.is_trained = True
            
            logger.info(f"LSTM model trained on {len(X)} sequences, threshold={self.threshold:.4f}")
            return True
            
        except Exception as e:
            logger.error(f"LSTM training failed: {e}")
            return False
    
    def predict(self, sequence_data):
        """
        Predict if sequence is anomalous.
        
        Args:
            sequence_data: List of recent quality metric dictionaries
            
        Returns:
            Tuple of (is_anomaly, error_score)
        """
        if not self.is_trained:
            return False, 0.0
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(sequence_data)
            
            # Ensure all features present
            for col in self.feature_columns:
                if col not in df.columns:
                    df[col] = 100.0
            
            # Select and order columns
            df = df[self.feature_columns]
            
            # Need at least sequence_length samples
            if len(df) < self.sequence_length:
                return False, 0.0
            
            # Take last sequence_length samples
            recent_data = df.tail(self.sequence_length).values
            
            # Scale
            scaled_data = self.scaler.transform(recent_data)
            
            # Reshape for LSTM
            X = scaled_data.reshape(1, self.sequence_length, len(self.feature_columns))
            
            # Predict
            prediction = self.model.predict(X, verbose=0)[0]
            
            # Calculate error
            actual = scaled_data[-1]
            error = np.mean(np.abs(prediction - actual))
            
            is_anomaly = error > self.threshold
            
            if is_anomaly:
                logger.warning(f"LSTM anomaly detected! Error: {error:.4f} > {self.threshold:.4f}")
            
            return is_anomaly, float(error)
            
        except Exception as e:
            logger.error(f"LSTM prediction failed: {e}")
            return False, 0.0
    
    def save_model(self, path):
        """Save trained model."""
        if self.model:
            self.model.save(f"{path}/lstm_model.h5")
            with open(f"{path}/lstm_scaler.pkl", 'wb') as f:
                pickle.dump(self.scaler, f)
            logger.info(f"LSTM model saved to {path}")
    
    def load_model(self, path):
        """Load trained model."""
        try:
            self.model = keras.models.load_model(f"{path}/lstm_model.h5")
            with open(f"{path}/lstm_scaler.pkl", 'rb') as f:
                self.scaler = pickle.load(f)
            self.is_trained = True
            logger.info(f"LSTM model loaded from {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load LSTM model: {e}")
            return False