"""
Configuration settings for the Football Prediction System
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# API Configuration
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")

# API Endpoints
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"

# Leagues to track (Football-Data.org codes)
LEAGUES = {
    "PL": {"name": "Premier League", "code": "PL", "country": "England"},
    "PD": {"name": "La Liga", "code": "PD", "country": "Spain"},
    "SA": {"name": "Serie A", "code": "SA", "country": "Italy"},
    "BL1": {"name": "Bundesliga", "code": "BL1", "country": "Germany"},
    "FL1": {"name": "Ligue 1", "code": "FL1", "country": "France"},
}

# Model Configuration
MODEL_PARAMS = {
    "xgboost": {
        "n_estimators": 400,
        "max_depth": 8,
        "learning_rate": 0.03,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "min_child_weight": 3,
        "gamma": 0.1,
        "reg_alpha": 0.05,
        "reg_lambda": 1.0,
        "random_state": 42
    },
    "lightgbm": {
        "n_estimators": 400,
        "max_depth": 8,
        "learning_rate": 0.03,
        "num_leaves": 50,
        "min_child_samples": 20,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "reg_alpha": 0.05,
        "reg_lambda": 1.0,
        "random_state": 42
    }
}

# Ensemble Configuration
ENSEMBLE_WEIGHTS = {
    "xgboost": 0.55,
    "lightgbm": 0.45
}

# Hyperparameter Tuning
TUNING_CONFIG = {
    "n_iter": 50,  # Number of parameter combinations to try
    "cv_folds": 5,  # Cross-validation folds
    "random_state": 42
}

# Probability Calibration
USE_CALIBRATION = True
CALIBRATION_METHOD = "isotonic"  # "isotonic" or "sigmoid"

# Feature Engineering
FORM_MATCHES = 5  # Number of recent matches for form calculation
HEAD_TO_HEAD_MATCHES = 10  # Number of H2H matches to consider

# Prediction thresholds
MIN_CONFIDENCE = 0.58  # Minimum probability to consider a prediction (58% = reasonable edge)
VALUE_BET_THRESHOLD = 0.05  # Minimum edge over bookmaker odds (5%)
HIGH_CONFIDENCE_THRESHOLD = 0.70  # High confidence predictions

# Training/Testing split
TEST_SIZE = 0.2
VALIDATION_SIZE = 0.1

# Target variables
TARGETS = ["home_win", "draw", "away_win"]

# Odds types for comparison
ODDS_TYPES = ["1X2", "over_under_2.5", "both_teams_score"]
