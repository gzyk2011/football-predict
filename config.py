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
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")

# ==========================================
# API-Football 接口与完整联赛配置库
# ==========================================
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

API_FOOTBALL_LEAGUES = {
    # ==========================================
    # 1. 欧洲顶级联赛
    # ==========================================
    "PL": {"name": "Premier League", "id": 39, "country": "England"},         # 英超
    "SA": {"name": "Serie A", "id": 135, "country": "Italy"},                 # 意甲
    "PD": {"name": "La Liga", "id": 140, "country": "Spain"},                 # 西甲
    "BL1": {"name": "Bundesliga", "id": 78, "country": "Germany"},            # 德甲
    "FL1": {"name": "Ligue 1", "id": 61, "country": "France"},                # 法甲
    "NED": {"name": "Eredivisie", "id": 88, "country": "Netherlands"},        # 荷甲
    "POR": {"name": "Primeira Liga", "id": 94, "country": "Portugal"},        # 葡超
    "TUR": {"name": "Süper Lig", "id": 203, "country": "Turkey"},             # 土超
    "BEL": {"name": "Jupiler Pro League", "id": 144, "country": "Belgium"},   # 比甲
    "NOR": {"name": "Eliteserien", "id": 103, "country": "Norway"},           # 挪超
    "SWE": {"name": "Allsvenskan", "id": 113, "country": "Sweden"},           # 瑞典超
    "SUI": {"name": "Super League", "id": 207, "country": "Switzerland"},     # 瑞士超
    "DEN": {"name": "Superliga", "id": 119, "country": "Denmark"},            # 丹麦超
    "POL": {"name": "Ekstraklasa", "id": 106, "country": "Poland"},           # 波兰甲
    "CRO": {"name": "HNL", "id": 210, "country": "Croatia"},                  # 克罗地亚甲
    "SRB": {"name": "Super Liga", "id": 286, "country": "Serbia"},            # 塞尔维亚超

    # ==========================================
    # 2. 美洲及亚洲联赛
    # ==========================================
    "MLS": {"name": "Major League Soccer", "id": 253, "country": "USA"},      # 美职联
    "BRA": {"name": "Serie A", "id": 71, "country": "Brazil"},                # 巴甲
    "ARG": {"name": "Liga Profesional Argentina", "id": 128, "country": "Argentina"}, # 阿甲
    "MEX": {"name": "Liga MX", "id": 262, "country": "Mexico"},               # 墨超
    "J1": {"name": "J1 League", "id": 98, "country": "Japan"},                # 日职联(J1)
    "K1": {"name": "K League 1", "id": 292, "country": "South-Korea"},        # 韩K联(K1)
    "AUS": {"name": "A-League", "id": 188, "country": "Australia"},           # 澳超
    "SAU": {"name": "Pro League", "id": 307, "country": "Saudi-Arabia"},      # 沙特联
    "QAT": {"name": "Stars League", "id": 305, "country": "Qatar"},           # 卡塔尔星联赛
    "UAE": {"name": "Pro League", "id": 301, "country": "United-Arab-Emirates"}, # 阿联酋超
    "IDN": {"name": "Liga 1", "id": 274, "country": "Indonesia"},             # 印尼超

    # ==========================================
    # 3. 次级联赛
    # ==========================================
    "ECH": {"name": "Championship", "id": 40, "country": "England"},          # 英冠
    "EL1": {"name": "League One", "id": 41, "country": "England"},            # 英甲
    "EL2": {"name": "League Two", "id": 42, "country": "England"},            # 英乙
    "FL2": {"name": "Ligue 2", "id": 62, "country": "France"},                # 法乙
    "SB": {"name": "Serie B", "id": 136, "country": "Italy"},                 # 意乙
    "BL2": {"name": "2. Bundesliga", "id": 79, "country": "Germany"},         # 德乙
    "SD": {"name": "Segunda División", "id": 141, "country": "Spain"},        # 西乙
    "TUR2": {"name": "1. Lig", "id": 204, "country": "Turkey"},               # 土甲
    "NED2": {"name": "Eerste Divisie", "id": 89, "country": "Netherlands"},   # 荷乙
    "BEL2": {"name": "Challenger Pro League", "id": 145, "country": "Belgium"},# 比乙
    "SWE2": {"name": "Superettan", "id": 114, "country": "Sweden"},           # 瑞典甲
    "SUI2": {"name": "Challenge League", "id": 208, "country": "Switzerland"},# 瑞士甲

    # ==========================================
    # 4. 洲际杯赛与国际赛事
    # ==========================================
    "USC": {"name": "UEFA Super Cup", "id": 531, "country": "World"},         # 欧洲超级杯
    "AFC": {"name": "AFC Champions League", "id": 17, "country": "World"},    # 亚冠精英联赛
    "UCL": {"name": "UEFA Champions League", "id": 2, "country": "World"},    # 欧冠联赛
}

# 兼容旧代码的别名
LEAGUES = API_FOOTBALL_LEAGUES

# ==========================================
# 模型与系统配置 (保持原样，无需修改)
# ==========================================
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

ENSEMBLE_WEIGHTS = {
    "xgboost": 0.55,
    "lightgbm": 0.45
}

TUNING_CONFIG = {
    "n_iter": 50,  
    "cv_folds": 5, 
    "random_state": 42
}

USE_CALIBRATION = True
CALIBRATION_METHOD = "isotonic" 

FORM_MATCHES = 5  
HEAD_TO_HEAD_MATCHES = 10  

MIN_CONFIDENCE = 0.58  
VALUE_BET_THRESHOLD = 0.05  
HIGH_CONFIDENCE_THRESHOLD = 0.70  

TEST_SIZE = 0.2
VALIDATION_SIZE = 0.1

TARGETS = ["home_win", "draw", "away_win"]
ODDS_TYPES = ["1X2", "over_under_2.5", "both_teams_score"]
