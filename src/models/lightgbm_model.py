"""
LightGBM Model Implementation
"""
import lightgbm as lgb
import numpy as np
from .base_model import BaseModel
import config


class LightGBMModel(BaseModel):
    """LightGBM classifier for match prediction"""
    
    def __init__(self, name="lightgbm_model", params=None):
        super().__init__(name)
        self.params = params or config.MODEL_PARAMS["lightgbm"]
    
    def build_model(self):
        """Build LightGBM classifier"""
        self.model = lgb.LGBMClassifier(
            objective='multiclass',
            num_class=3,
            **self.params
        )
        return self.model
    
    def train(self, X_train, y_train, X_val=None, y_val=None):
        """
        Train LightGBM model
        
        Args:
            X_train: Training features
            y_train: Training labels (0=Home, 1=Draw, 2=Away)
            X_val: Validation features (optional, for calibration)
            y_val: Validation labels (optional, for calibration)
        """
        if self.model is None:
            self.build_model()
        
        self.feature_columns = list(X_train.columns)
        
        eval_set = [(X_train, y_train)]
        if X_val is not None and y_val is not None:
            eval_set.append((X_val, y_val))
        
        print(f"Training {self.name}...")
        self.model.fit(
            X_train,
            y_train,
            eval_set=eval_set,
            callbacks=[lgb.log_evaluation(0)]  # Silent training
        )
        
        self.is_trained = True
        print(f"{self.name} training complete")
        
        # Calibrate if validation data provided
        if X_val is not None and y_val is not None and self.use_calibration:
            self.calibrate_model(X_val, y_val)
        
        # Print feature importance
        print("\nTop 10 most important features:")
        importance = self.get_feature_importance(10)
        if importance is not None:
            print(importance.to_string(index=False))
        
        return self
    
    def predict_proba(self, X):
        """
        Predict probabilities for each outcome
        
        Returns:
            Array of shape (n_samples, 3) with probabilities for [Home, Draw, Away]
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")
        
        probs = self.model.predict_proba(X)
        
        # Apply calibration if available
        if self.use_calibration and self.calibrators is not None:
            probs = self._apply_calibration(probs)
        
        return probs
