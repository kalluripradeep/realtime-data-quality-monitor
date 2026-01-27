"""
Explainable AI Module for Anomaly Detection
Provides SHAP-based explanations for anomaly predictions
"""
import shap
import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class AnomalyExplainer:
    """
    Provides explainable AI capabilities for anomaly detection.
    Uses SHAP (SHapley Additive exPlanations) to explain why
    data points were flagged as anomalies.
    """
    
    def __init__(self, model, feature_names):
        """
        Initialize explainer.
        
        Args:
            model: Trained Isolation Forest model
            feature_names: List of feature names
        """
        self.model = model
        self.feature_names = feature_names
        self.explainer = None
        
        logger.info("AnomalyExplainer initialized")
    
    def create_explainer(self, background_data):
        """
        Create SHAP explainer with background data.
        
        Args:
            background_data: Sample of normal data for baseline
        """
        try:
            # TreeExplainer for tree-based models
            self.explainer = shap.TreeExplainer(
                self.model,
                background_data
            )
            logger.info(f"SHAP explainer created with {len(background_data)} background samples")
            return True
        except Exception as e:
            logger.error(f"Failed to create explainer: {e}")
            return False
    
    def explain_anomaly(self, data_point: Dict) -> Dict:
        """
        Explain why a data point was flagged as anomaly.
        
        Args:
            data_point: Dictionary with feature values
            
        Returns:
            Dictionary with explanation details
        """
        if self.explainer is None:
            return {'error': 'Explainer not initialized'}
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame([data_point])
            
            # Get SHAP values
            shap_values = self.explainer.shap_values(df)
            
            # Get feature contributions
            contributions = []
            for i, feature in enumerate(self.feature_names):
                contributions.append({
                    'feature': feature,
                    'value': data_point[feature],
                    'shap_value': float(shap_values[0][i]),
                    'impact': 'increases' if shap_values[0][i] > 0 else 'decreases'
                })
            
            # Sort by absolute SHAP value
            contributions.sort(key=lambda x: abs(x['shap_value']), reverse=True)
            
            # Get top contributors
            top_3 = contributions[:3]
            
            return {
                'all_contributions': contributions,
                'top_contributors': top_3,
                'explanation_summary': self._generate_summary(top_3),
                'anomaly_confidence': abs(sum([c['shap_value'] for c in contributions]))
            }
            
        except Exception as e:
            logger.error(f"Explanation failed: {e}")
            return {'error': str(e)}
    
    def _generate_summary(self, top_contributors: List[Dict]) -> str:
        """Generate human-readable explanation."""
        if not top_contributors:
            return "Unable to generate explanation"
        
        top = top_contributors[0]
        summary = f"Primary anomaly driver: {top['feature']} = {top['value']:.2f} "
        summary += f"({top['impact']} anomaly score by {abs(top['shap_value']):.4f})"
        
        if len(top_contributors) > 1:
            second = top_contributors[1]
            summary += f"\nSecondary factor: {second['feature']} = {second['value']:.2f}"
        
        return summary