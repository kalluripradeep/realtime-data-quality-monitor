"""
Autoencoder-based Anomaly Detection
Uses reconstruction error to detect anomalies
"""
import numpy as np
import pandas as pd
from tensorflow import keras
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, Dropout
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import logging
import pickle

logger = logging.getLogger(__name__)


class AutoencoderDetector:
    """
    Autoencoder-based anomaly detector.
    Learns to reconstruct normal patterns, high error = anomaly.
    """
    
    def __init__(self, encoding_dim=4, threshold_percentile=95):
        """
        Initialize autoencoder.
        
        Args:
            encoding_dim: Dimension of encoded representation
            threshold_percentile: Percentile for anomaly threshold
        """
        self.encoding_dim = encoding_dim
        self.threshold_percentile = threshold_percentile
        self.model = None
        self.encoder = None
        self.scaler = StandardScaler()
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
        
        logger.info(f"AutoencoderDetector initialized (encoding_dim={encoding_dim})")
    
    def build_model(self, n_features):
        """Build autoencoder architecture."""
        # Input layer
        input_layer = Input(shape=(n_features,))
        
        # Encoder
        encoded = Dense(16, activation='relu')(input_layer)
        encoded = Dropout(0.2)(encoded)
        encoded = Dense(8, activation='relu')(encoded)
        encoded = Dropout(0.2)(encoded)
        encoded = Dense(self.encoding_dim, activation='relu')(encoded)
        
        # Decoder
        decoded = Dense(8, activation='relu')(encoded)
        decoded = Dropout(0.2)(decoded)
        decoded = Dense(16, activation='relu')(decoded)
        decoded = Dropout(0.2)(decoded)
        decoded = Dense(n_features, activation='linear')(decoded)
        
        # Models
        autoencoder = Model(input_layer, decoded)
        encoder = Model(input_layer, encoded)
        
        autoencoder.compile(optimizer='adam', loss='mse', metrics=['mae'])
        
        return autoencoder, encoder
    
    def train(self, conn, hours=1, min_samples=50):
        """
        Train autoencoder on normal data.
        
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
            df_pivot = df_pivot.fillna(method='ffill').fillna(100.0)

            # Limit to last 500 samples for faster training
            if len(df_pivot) > 500:
                df_pivot = df_pivot.tail(500)
            if len(df_pivot) < min_samples:
                logger.warning(f"Not enough data for autoencoder training: {len(df_pivot)} < {min_samples}")
                return False
            
            # Scale data
            X = self.scaler.fit_transform(df_pivot.values)
            
            # Build and train model
            self.model, self.encoder = self.build_model(len(self.feature_columns))
            
            # Train with early stopping
            early_stop = keras.callbacks.EarlyStopping(
                monitor='loss',
                patience=10,
                restore_best_weights=True
            )
            
            self.model.fit(
                X, X,  # Autoencoder reconstructs input
                epochs=100,
                batch_size=32,
                validation_split=0.2,
                callbacks=[early_stop],
                verbose=0
            )
            
            # Calculate threshold from reconstruction errors
            reconstructed = self.model.predict(X, verbose=0)
            errors = np.mean(np.abs(X - reconstructed), axis=1)
            self.threshold = np.percentile(errors, self.threshold_percentile)
            
            self.is_trained = True
            
            logger.info(f"Autoencoder trained on {len(X)} samples, threshold={self.threshold:.4f}")
            return True
            
        except Exception as e:
            logger.error(f"Autoencoder training failed: {e}")
            return False
    
    def predict(self, data):
        """
        Predict if data point is anomalous.
        
        Args:
            data: Dictionary with quality metrics
            
        Returns:
            Tuple of (is_anomaly, reconstruction_error)
        """
        if not self.is_trained:
            return False, 0.0
        
        try:
            # Extract features
            features = [data.get(col, 100.0) for col in self.feature_columns]
            X = np.array(features).reshape(1, -1)
            
            # Scale
            X_scaled = self.scaler.transform(X)
            
            # Reconstruct
            reconstructed = self.model.predict(X_scaled, verbose=0)
            
            # Calculate reconstruction error
            error = np.mean(np.abs(X_scaled - reconstructed))
            
            is_anomaly = error > self.threshold
            
            if is_anomaly:
                logger.warning(f"Autoencoder anomaly detected! Error: {error:.4f} > {self.threshold:.4f}")
            
            return is_anomaly, float(error)
            
        except Exception as e:
            logger.error(f"Autoencoder prediction failed: {e}")
            return False, 0.0
    
    def get_reconstruction(self, data):
        """Get reconstructed values for analysis."""
        if not self.is_trained:
            return None
        
        try:
            features = [data.get(col, 100.0) for col in self.feature_columns]
            X = np.array(features).reshape(1, -1)
            X_scaled = self.scaler.transform(X)
            reconstructed_scaled = self.model.predict(X_scaled, verbose=0)
            reconstructed = self.scaler.inverse_transform(reconstructed_scaled)
            
            return {col: float(val) for col, val in zip(self.feature_columns, reconstructed[0])}
            
        except Exception as e:
            logger.error(f"Reconstruction failed: {e}")
            return None
    
    def save_model(self, path):
        """Save trained model."""
        if self.model:
            self.model.save(f"{path}/autoencoder_model.h5")
            with open(f"{path}/autoencoder_scaler.pkl", 'wb') as f:
                pickle.dump(self.scaler, f)
            logger.info(f"Autoencoder saved to {path}")
    
    def load_model(self, path):
        """Load trained model."""
        try:
            self.model = keras.models.load_model(f"{path}/autoencoder_model.h5")
            with open(f"{path}/autoencoder_scaler.pkl", 'rb') as f:
                self.scaler = pickle.load(f)
            self.is_trained = True
            logger.info(f"Autoencoder loaded from {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load autoencoder: {e}")
            return False