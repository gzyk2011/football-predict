"""
Ensemble Model
Combines multiple models for better predictions
"""
import numpy as np
from .base_model import BaseModel
import config


class EnsembleModel(BaseModel):
    """Ensemble of multiple models using weighted averaging"""
    
    def __init__(self, models, weights=None, name="ensemble_model"):
        """
        Args:
            models: List of trained BaseModel instances
            weights: List of weights for each model (must sum to 1)
            name: Model name
        """
        super().__init__(name)
        self.models = models
        
        if weights is None:
            # Use config weights if available
            if hasattr(config, 'ENSEMBLE_WEIGHTS') and len(models) == 2:
                self.weights = [
                    config.ENSEMBLE_WEIGHTS.get("xgboost", 0.5),
                    config.ENSEMBLE_WEIGHTS.get("lightgbm", 0.5)
                ]
            else:
                # Equal weights
                self.weights = [1.0 / len(models)] * len(models)
        else:
            if len(weights) != len(models):
                raise ValueError("Number of weights must match number of models")
            if not np.isclose(sum(weights), 1.0):
                raise ValueError("Weights must sum to 1")
            self.weights = weights
        
        print(f"Ensemble weights: {dict(zip([m.name for m in models], self.weights))}")
        
        # Check all models are trained
        for model in self.models:
            if not model.is_trained:
                raise ValueError(f"Model {model.name} must be trained before ensemble")
        
        self.is_trained = True
        self.feature_columns = self.models[0].feature_columns
    
    def build_model(self):
        """Not needed for ensemble"""
        pass
    
    def train(self, X_train, y_train, X_val=None, y_val=None):
        """Not needed - ensemble uses pre-trained models"""
        raise NotImplementedError("Ensemble uses pre-trained models")
    
    def predict_proba(self, X):
        """
        Predict probabilities using weighted ensemble
        
        Returns:
            Array of shape (n_samples, 3) with probabilities for [Home, Draw, Away]
        """
        # Get predictions from all models
        predictions = []
        for model in self.models:
            pred = model.predict_proba(X)
            predictions.append(pred)
        
        # Weighted average
        ensemble_pred = np.zeros_like(predictions[0])
        for pred, weight in zip(predictions, self.weights):
            ensemble_pred += weight * pred
        
        return ensemble_pred
    
    def get_individual_predictions(self, X):
        """Get predictions from each individual model"""
        predictions = {}
        for model in self.models:
            predictions[model.name] = model.predict_proba(X)
        return predictions
