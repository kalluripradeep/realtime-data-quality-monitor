"""
Ensemble Anomaly Detector
Combines Isolation Forest, LSTM, and Autoencoder predictions
"""
import numpy as np
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class EnsembleDetector:
    """
    Ensemble anomaly detector combining multiple models.
    Uses weighted voting to make final prediction.
    """
    
    def __init__(self, isolation_forest, lstm_detector, autoencoder):
        """
        Initialize ensemble detector.
        
        Args:
            isolation_forest: IsolationForest detector
            lstm_detector: LSTM detector
            autoencoder: Autoencoder detector
        """
        self.isolation_forest = isolation_forest
        self.lstm_detector = lstm_detector
        self.autoencoder = autoencoder
        
        # Weights for each model (can be tuned)
        self.weights = {
            'isolation_forest': 0.4,
            'lstm': 0.3,
            'autoencoder': 0.3
        }
        
        logger.info("EnsembleDetector initialized with 3 models")
    
    def predict(self, data, sequence_data=None):
        """
        Predict using ensemble of models.
        
        Args:
            data: Current data point (dict)
            sequence_data: Recent sequence for LSTM (list of dicts)
            
        Returns:
            Tuple of (is_anomaly, ensemble_score, individual_results)
        """
        results = {}
        scores = {}
        
        # 1. Isolation Forest
        if self.isolation_forest.is_trained:
            is_anomaly_if, score_if = self.isolation_forest.predict(data)
            results['isolation_forest'] = is_anomaly_if
            # Normalize score to 0-1 range (IF scores are typically -1 to 0)
            scores['isolation_forest'] = abs(score_if) if score_if < 0 else 0
        else:
            results['isolation_forest'] = False
            scores['isolation_forest'] = 0
        
        # 2. LSTM
        if self.lstm_detector.is_trained and sequence_data:
            is_anomaly_lstm, error_lstm = self.lstm_detector.predict(sequence_data)
            results['lstm'] = is_anomaly_lstm
            # Normalize error
            scores['lstm'] = min(error_lstm / self.lstm_detector.threshold if self.lstm_detector.threshold else 0, 1.0)
        else:
            results['lstm'] = False
            scores['lstm'] = 0
        
        # 3. Autoencoder
        if self.autoencoder.is_trained:
            is_anomaly_ae, error_ae = self.autoencoder.predict(data)
            results['autoencoder'] = is_anomaly_ae
            # Normalize error
            scores['autoencoder'] = min(error_ae / self.autoencoder.threshold if self.autoencoder.threshold else 0, 1.0)
        else:
            results['autoencoder'] = False
            scores['autoencoder'] = 0
        
        # Calculate weighted ensemble score
        ensemble_score = sum(
            scores[model] * self.weights[model] 
            for model in scores.keys()
        )
        
        # Voting: anomaly if weighted score > 0.5 OR 2+ models agree
        votes = sum(1 for v in results.values() if v)
        is_anomaly = ensemble_score > 0.5 or votes >= 2
        
        if is_anomaly:
            logger.warning(
                f"🎯 ENSEMBLE ANOMALY! Score: {ensemble_score:.3f}, "
                f"Votes: {votes}/3 (IF:{results['isolation_forest']}, "
                f"LSTM:{results['lstm']}, AE:{results['autoencoder']})"
            )
        
        return is_anomaly, ensemble_score, {
            'individual_results': results,
            'individual_scores': scores,
            'votes': votes,
            'ensemble_score': ensemble_score
        }
    
    def get_model_status(self):
        """Get training status of all models."""
        return {
            'isolation_forest': self.isolation_forest.is_trained,
            'lstm': self.lstm_detector.is_trained,
            'autoencoder': self.autoencoder.is_trained,
            'ensemble_ready': all([
                self.isolation_forest.is_trained,
                self.lstm_detector.is_trained,
                self.autoencoder.is_trained
            ])
        }