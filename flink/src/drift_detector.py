"""
Concept Drift Detection Module
Detects distribution shifts in streaming data
"""
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
from datetime import datetime, timedelta
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class DriftDetector:
    """
    Detects concept drift in data quality metrics.
    Uses Kolmogorov-Smirnov test to compare distributions.
    """
    
    def __init__(self, reference_window_hours=24):
        """
        Initialize drift detector.
        
        Args:
            reference_window_hours: Hours of historical data for reference
        """
        self.reference_window_hours = reference_window_hours
        self.reference_data = {}
        self.drift_history = []
        
        logger.info(f"DriftDetector initialized with {reference_window_hours}h reference window")
    
    def set_reference_data(self, conn, feature_names: List[str]):
        """
        Set reference data from database.
        
        Args:
            conn: Database connection
            feature_names: List of features to monitor
        """
        try:
            cutoff = datetime.now() - timedelta(hours=self.reference_window_hours)
            
            for feature in feature_names:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT metric_value 
                    FROM quality_metrics 
                    WHERE metric_name = %s 
                      AND timestamp > %s
                    ORDER BY timestamp
                """, (feature, cutoff))
                
                values = [row[0] for row in cursor.fetchall()]
                
                if len(values) > 30:  # Minimum samples
                    self.reference_data[feature] = np.array(values)
                    logger.info(f"Reference data set for {feature}: {len(values)} samples")
                else:
                    logger.warning(f"Insufficient reference data for {feature}")
                
                cursor.close()
            
            return len(self.reference_data) > 0
            
        except Exception as e:
            logger.error(f"Failed to set reference data: {e}")
            return False
    
    def detect_drift(self, current_data: Dict[str, List[float]]) -> Dict:
        """
        Detect drift between reference and current data.
        
        Args:
            current_data: Dictionary of feature_name -> list of recent values
            
        Returns:
            Dictionary with drift detection results
        """
        if not self.reference_data:
            return {'error': 'Reference data not set'}
        
        drift_results = {
            'timestamp': datetime.now().isoformat(),
            'features_checked': 0,
            'drifts_detected': 0,
            'details': []
        }
        
        for feature, current_values in current_data.items():
            if feature not in self.reference_data:
                continue
            
            if len(current_values) < 30:
                continue
            
            drift_results['features_checked'] += 1
            
            # Perform KS test
            statistic, p_value = ks_2samp(
                self.reference_data[feature],
                current_values
            )
            
            # Determine drift severity
            drift_detected = p_value < 0.05
            if drift_detected:
                drift_results['drifts_detected'] += 1
                severity = 'critical' if p_value < 0.01 else 'warning'
            else:
                severity = 'none'
            
            detail = {
                'feature': feature,
                'drift_detected': drift_detected,
                'severity': severity,
                'p_value': float(p_value),
                'ks_statistic': float(statistic),
                'recommendation': self._get_recommendation(p_value)
            }
            
            drift_results['details'].append(detail)
            
            # Log critical drifts
            if severity == 'critical':
                logger.warning(
                    f"⚠️ CRITICAL DRIFT DETECTED in {feature}: "
                    f"p-value={p_value:.6f}, KS={statistic:.4f}"
                )
        
        # Store in history
        self.drift_history.append(drift_results)
        
        return drift_results
    
    def _get_recommendation(self, p_value: float) -> str:
        """Get recommendation based on drift severity."""
        if p_value < 0.01:
            return "Retrain model immediately - significant distribution shift"
        elif p_value < 0.05:
            return "Schedule model retraining - moderate drift detected"
        else:
            return "No action needed - distributions similar"
    
    def get_drift_summary(self) -> Dict:
        """Get summary of drift detection history."""
        if not self.drift_history:
            return {'total_checks': 0}
        
        total_checks = len(self.drift_history)
        total_drifts = sum([h['drifts_detected'] for h in self.drift_history])
        
        return {
            'total_checks': total_checks,
            'total_drifts_detected': total_drifts,
            'drift_rate': total_drifts / total_checks if total_checks > 0 else 0,
            'last_check': self.drift_history[-1]['timestamp']
        }