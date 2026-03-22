"""
Hyperparameter Tuning Module
Uses RandomizedSearchCV to find optimal model parameters
"""
import numpy as np
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold
from scipy.stats import randint, uniform
import joblib
from pathlib import Path
import config


class ModelTuner:
    """Hyperparameter tuning for prediction models"""
    
    def __init__(self, n_iter=50, cv=5, random_state=42):
        """
        Initialize tuner
        
        Args:
            n_iter: Number of parameter settings sampled
            cv: Number of cross-validation folds
            random_state: Random state for reproducibility
        """
        self.n_iter = n_iter
        self.cv = cv
        self.random_state = random_state
    
    def tune_xgboost(self, X_train, y_train):
        """
        Tune XGBoost hyperparameters
        
        Args:
            X_train: Training features
            y_train: Training labels
        
        Returns:
            Best parameters dictionary
        """
        from xgboost import XGBClassifier
        
        print("\nTuning XGBoost hyperparameters...")
        
        # Parameter distribution
        param_dist = {
            'n_estimators': randint(200, 600),
            'max_depth': randint(4, 12),
            'learning_rate': uniform(0.01, 0.1),
            'subsample': uniform(0.6, 0.4),
            'colsample_bytree': uniform(0.6, 0.4),
            'min_child_weight': randint(1, 7),
            'gamma': uniform(0, 0.5),
            'reg_alpha': uniform(0, 1),
            'reg_lambda': uniform(0, 2)
        }
        
        # Base model
        base_model = XGBClassifier(
            objective='multi:softprob',
            num_class=3,
            random_state=self.random_state,
            n_jobs=-1
        )
        
        # Randomized search
        search = RandomizedSearchCV(
            base_model,
            param_distributions=param_dist,
            n_iter=self.n_iter,
            cv=StratifiedKFold(n_splits=self.cv, shuffle=True, random_state=self.random_state),
            scoring='neg_log_loss',
            n_jobs=-1,
            verbose=2,
            random_state=self.random_state
        )
        
        search.fit(X_train, y_train)
        
        print(f"\nBest XGBoost score: {-search.best_score_:.4f}")
        print("Best parameters:")
        for param, value in search.best_params_.items():
            print(f"  {param}: {value}")
        
        return search.best_params_
    
    def tune_lightgbm(self, X_train, y_train):
        """
        Tune LightGBM hyperparameters
        
        Args:
            X_train: Training features
            y_train: Training labels
        
        Returns:
            Best parameters dictionary
        """
        from lightgbm import LGBMClassifier
        
        print("\nTuning LightGBM hyperparameters...")
        
        # Parameter distribution
        param_dist = {
            'n_estimators': randint(200, 600),
            'max_depth': randint(4, 12),
            'learning_rate': uniform(0.01, 0.1),
            'num_leaves': randint(20, 100),
            'min_child_samples': randint(10, 50),
            'subsample': uniform(0.6, 0.4),
            'colsample_bytree': uniform(0.6, 0.4),
            'reg_alpha': uniform(0, 1),
            'reg_lambda': uniform(0, 2)
        }
        
        # Base model
        base_model = LGBMClassifier(
            objective='multiclass',
            num_class=3,
            random_state=self.random_state,
            n_jobs=-1,
            verbose=-1
        )
        
        # Randomized search
        search = RandomizedSearchCV(
            base_model,
            param_distributions=param_dist,
            n_iter=self.n_iter,
            cv=StratifiedKFold(n_splits=self.cv, shuffle=True, random_state=self.random_state),
            scoring='neg_log_loss',
            n_jobs=-1,
            verbose=2,
            random_state=self.random_state
        )
        
        search.fit(X_train, y_train)
        
        print(f"\nBest LightGBM score: {-search.best_score_:.4f}")
        print("Best parameters:")
        for param, value in search.best_params_.items():
            print(f"  {param}: {value}")
        
        return search.best_params_
    
    def save_best_params(self, xgb_params, lgb_params, filepath=None):
        """
        Save best parameters to file
        
        Args:
            xgb_params: XGBoost parameters
            lgb_params: LightGBM parameters
            filepath: Path to save parameters
        """
        if filepath is None:
            filepath = config.MODELS_DIR / "best_params.joblib"
        
        params = {
            'xgboost': xgb_params,
            'lightgbm': lgb_params
        }
        
        joblib.dump(params, filepath)
        print(f"\nBest parameters saved to {filepath}")
    
    @staticmethod
    def load_best_params(filepath=None):
        """
        Load best parameters from file
        
        Args:
            filepath: Path to parameter file
        
        Returns:
            Dictionary with xgboost and lightgbm parameters
        """
        if filepath is None:
            filepath = config.MODELS_DIR / "best_params.joblib"
        
        if Path(filepath).exists():
            params = joblib.load(filepath)
            print(f"Loaded best parameters from {filepath}")
            return params
        else:
            print(f"No saved parameters found at {filepath}")
            return None


if __name__ == "__main__":
    import sys
    sys.path.append("../..")
    from src.data.collector import FootballDataCollector
    from src.data.preprocessor import DataPreprocessor
    from src.features.engineer import FeatureEngineer
    from sklearn.model_selection import train_test_split
    import pandas as pd
    
    # Load and prepare data
    print("Loading data...")
    csv_file = config.DATA_DIR / "football_matches_historical.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file)
        df["date"] = pd.to_datetime(df["date"])
    else:
        collector = FootballDataCollector()
        df = collector.load_data()
    
    if not df.empty:
        # Preprocess
        preprocessor = DataPreprocessor()
        df_clean = preprocessor.clean_data(df)
        df_encoded = preprocessor.encode_results(df_clean)
        
        # Feature engineering
        engineer = FeatureEngineer()
        df_features = engineer.create_all_features(df_encoded)
        
        # Prepare features
        feature_columns = engineer.get_feature_columns()
        X = df_features[feature_columns]
        y = df_features["result"].map({"H": 0, "D": 1, "A": 2})
        
        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Tune models
        tuner = ModelTuner(n_iter=30, cv=3)  # Reduced for faster testing
        
        xgb_params = tuner.tune_xgboost(X_train, y_train)
        lgb_params = tuner.tune_lightgbm(X_train, y_train)
        
        # Save best parameters
        tuner.save_best_params(xgb_params, lgb_params)
        
        print("\n" + "="*60)
        print("HYPERPARAMETER TUNING COMPLETE")
        print("="*60)
