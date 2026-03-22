"""
Base Model Class
Abstract base class for all prediction models
"""
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from sklearn.calibration import CalibratedClassifierCV
import config


class BaseModel(ABC):
    """Abstract base class for football prediction models"""
    
    def __init__(self, name="base_model", use_calibration=None):
        self.name = name
        self.model = None
        self.calibrators = None
        self.is_trained = False
        self.feature_columns = None
        self.models_dir = config.MODELS_DIR
        self.use_calibration = use_calibration if use_calibration is not None else config.USE_CALIBRATION
    
    @abstractmethod
    def build_model(self):
        """Build the model architecture"""
        pass
    
    @abstractmethod
    def train(self, X_train, y_train, X_val=None, y_val=None):
        """Train the model"""
        pass
    
    @abstractmethod
    def predict_proba(self, X):
        """Predict probabilities"""
        pass
    
    def calibrate_model(self, X_val, y_val, method=None):
        """
        Calibrate model probabilities using validation set
        
        Args:
            X_val: Validation features
            y_val: Validation labels
            method: 'isotonic' or 'sigmoid' (default from config)
        """
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
        
        if method is None:
            method = config.CALIBRATION_METHOD
        
        print(f"Calibrating {self.name} using {method} regression...")
        
        # Get uncalibrated probabilities
        uncal_probs = self.model.predict_proba(X_val)
        
        # Train calibrators for each class
        self.calibrators = []
        for class_idx in range(uncal_probs.shape[1]):
            if method == 'isotonic':
                calibrator = IsotonicRegression(out_of_bounds='clip')
            else:  # sigmoid
                calibrator = LogisticRegression()
            
            # Binary indicator for this class
            y_binary = (y_val == class_idx).astype(int)
            calibrator.fit(uncal_probs[:, class_idx].reshape(-1, 1), y_binary)
            self.calibrators.append(calibrator)
        
        print(f"{self.name} calibration complete")
    
    def _get_model_for_prediction(self):
        """Get the appropriate model for prediction"""
        return self.model
    
    def _apply_calibration(self, probs):
        """Apply calibration to probabilities"""
        if self.calibrators is None:
            return probs
        
        calibrated_probs = np.zeros_like(probs)
        for class_idx, calibrator in enumerate(self.calibrators):
            calibrated_probs[:, class_idx] = calibrator.predict(probs[:, class_idx].reshape(-1, 1)).ravel()
        
        # Normalize to sum to 1
        row_sums = calibrated_probs.sum(axis=1, keepdims=True)
        calibrated_probs = calibrated_probs / row_sums
        return calibrated_probs
    
    def predict(self, X):
        """Predict class labels"""
        proba = self.predict_proba(X)
        return np.argmax(proba, axis=1)
    
    def evaluate(self, X_test, y_test):
        """
        Evaluate model performance
        
        Returns:
            Dictionary with evaluation metrics
        """
        from sklearn.metrics import accuracy_score, log_loss, classification_report
        
        y_pred = self.predict(X_test)
        y_proba = self.predict_proba(X_test)
        
        accuracy = accuracy_score(y_test, y_pred)
        logloss = log_loss(y_test, y_proba)
        
        # Convert predictions to result labels
        result_map = {0: "Home Win", 1: "Draw", 2: "Away Win"}
        y_test_labels = [result_map[y] for y in y_test]
        y_pred_labels = [result_map[y] for y in y_pred]
        
        report = classification_report(y_test_labels, y_pred_labels)
        
        return {
            "accuracy": accuracy,
            "log_loss": logloss,
            "report": report
        }
    
    def save_model(self, filename=None):
        """Save trained model to disk"""
        if not self.is_trained:
            raise ValueError("Model must be trained before saving")
        
        if filename is None:
            filename = f"{self.name}.joblib"
        
        filepath = self.models_dir / filename
        
        model_data = {
            "model": self.model,
            "feature_columns": self.feature_columns,
            "name": self.name
        }
        
        joblib.dump(model_data, filepath)
        print(f"Model saved to {filepath}")
        return filepath
    
    def load_model(self, filename=None):
        """Load trained model from disk"""
        if filename is None:
            filename = f"{self.name}.joblib"
        
        filepath = self.models_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Model file not found: {filepath}")
        
        model_data = joblib.load(filepath)
        self.model = model_data["model"]
        self.feature_columns = model_data["feature_columns"]
        self.name = model_data["name"]
        self.is_trained = True
        
        print(f"Model loaded from {filepath}")
        return self
    
    def get_feature_importance(self, top_n=10):
        """
        Get feature importance (for tree-based models)
        
        Args:
            top_n: Number of top features to return
        
        Returns:
            DataFrame with feature importance
        """
        if not hasattr(self.model, "feature_importances_"):
            print("Model does not support feature importance")
            return None
        
        importance = self.model.feature_importances_
        feature_importance = pd.DataFrame({
            "feature": self.feature_columns,
            "importance": importance
        }).sort_values("importance", ascending=False)
        
        return feature_importance.head(top_n)
