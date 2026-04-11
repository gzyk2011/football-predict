"""
Main Execution Script
Orchestrates data collection, model training, prediction, 和 backtesting
"""
import argparse
import sys
import os
from pathlib import Path
import pandas as pd
import requests  # 用于发送网络请求到 PushPlus

# Add src to path - try multiple approaches for compatibility
script_dir = Path(__file__).parent.resolve()
sys.path.insert(0, str(script_dir))

# Also set PYTHONPATH environment variable
os.environ['PYTHONPATH'] = str(script_dir) + os.pathsep + os.environ.get('PYTHONPATH', '')

from src.data.collector import FootballDataCollector
from src.data.csv_collector import FootballDataCSVCollector
from src.data.odds_collector import OddsCollector
from src.data.preprocessor import DataPreprocessor
from src.features.engineer import FeatureEngineer
from src.models.xgboost_model import XGBoostModel
from src.models.lightgbm_model import LightGBMModel
from src.models.ensemble import EnsembleModel
from src.prediction.predictor import MatchPredictor
from src.prediction.odds_analyzer import OddsAnalyzer
from src.evaluation.backtester import Backtester
import config


def collect_csv_data(args):
    """Collect historical match data from football-data.co.uk CSV files (FREE)"""
    print("\n" + "="*80)
    print("COLLECTING HISTORICAL DATA FROM CSV FILES")
    print("="*80 + "\n")
    
    collector = FootballDataCSVCollector()
    
    # Parse leagues
    league_map = {
        "PL": "E0", "Championship": "E1",
        "LaLiga": "SP1", "SerieA": "I1",
        "Bundesliga": "D1", "Ligue1": "F1"
    }
    
    leagues_input = args.leagues.split(",") if args.leagues else ["PL"]
    league_codes = [league_map.get(l, l) for l in leagues_input]
    
    # Parse seasons (e.g., 1819 for 2018/19)
    start_season = int(args.start_season) if args.start_season else 1819
    end_season = int(args.end_season) if args.end_season else 2324
    
    print(f"Leagues: {', '.join(leagues_input)}")
    print(f"Seasons: {start_season} to {end_season}\n")
    
    # Collect data
    df = collector.collect_multiple_seasons(league_codes, start_season, end_season)
    
    if df.empty:
        print("No data collected!")
        return
    
    # Save data
    collector.save_data(df)
    
    print(f"\nSuccessfully collected {len(df)} matches!")


def collect_data(args):
    """Collect historical match data"""
    print("\n" + "="*80)
    print("COLLECTING DATA")
    print("="*80 + "\n")
    
    collector = FootballDataCollector()
    
    # Parse leagues and seasons
    leagues = args.leagues.split(",") if args.leagues else ["PL"]
    seasons = [int(s) for s in args.seasons.split(",")] if args.seasons else [2021, 2022, 2023]
    
    print(f"Leagues: {', '.join(leagues)}")
    print(f"Seasons: {', '.join(map(str, seasons))}\n")
    
    # Collect data
    df = collector.collect_multiple_seasons(leagues, seasons)
    
    if df.empty:
        print("No data collected!")
        return
    
    # Save data
    collector.save_data(df)
    
    print(f"\nSuccessfully collected {len(df)} matches!")


def train_models(args):
    """Train prediction models - Skip if models already exist (Permanent Memory)"""
    import joblib
    
    # 1. 定义核心模型文件路径
    xgb_path = config.MODELS_DIR / "xgboost_model.joblib"
    lgb_path = config.MODELS_DIR / "lightgbm_model.joblib"
    engineer_path = config.MODELS_DIR / "feature_engineer.joblib"
    
    # 2. 检查模型是否已经存在
    if xgb_path.exists() and lgb_path.exists() and engineer_path.exists():
        print("\n" + "="*80)
        print("✅ 永久记忆模式：检测到现有模型文件，直接跳过训练。")
        print("🚀 系统将直接进入预测环节。")
        print("💡 提示：若需强制重练模型，请手动删除仓库 models/ 文件夹下的 .joblib 文件。")
        print("="*80 + "\n")
        return

    # 3. 如果模型不存在，则执行大规模训练
    print("\n" + "="*80)
    print("首次运行或模型缺失，正在开始全联赛数据采集与模型训练...")
    print("这可能需要较长时间（约 20 分钟），请耐心等待。")
    print("="*80 + "\n")
    
    # 获取历史数据（抓取最近 3 年的数据以保证准确度）
    collector = FootballDataCollector()
    leagues = args.leagues.split(",") if args.leagues else ["PL"]
    current_year = pd.Timestamp.now().year
    seasons = [current_year - 2, current_year - 1, current_year]
    
    df = collector.collect_multiple_seasons(leagues, seasons)
    
    if df.empty:
        print("❌ 错误：从 API 未采集到训练数据，尝试加载本地数据...")
        df = collector.load_data()
        
    if df.empty:
        print("❌ 错误：无可用训练数据，训练终止。")
        return
        
    collector.save_data(df)
    
    # 执行标准预处理
    preprocessor = DataPreprocessor()
    df_clean = preprocessor.clean_data(df)
    df_encoded = preprocessor.encode_results(df_clean)
    
    # 特征工程
    engineer = FeatureEngineer()
    df_features = engineer.create_all_features(df_encoded)
    
    print(f"\nDataset: {len(df_features)} matches with {len(engineer.get_feature_columns())} features")
    
    # 分割数据集
    train_df, test_df = preprocessor.split_train_test(df_features, test_size=0.2, by_date=True)
    val_split_idx = int(len(train_df) * 0.85)
    
    feature_cols = engineer.get_feature_columns()
    X_train = train_df.iloc[:val_split_idx][feature_cols]
    y_train = train_df.iloc[:val_split_idx]["result"].map({"H": 0, "D": 1, "A": 2}).values
    X_val = train_df.iloc[val_split_idx:][feature_cols]
    y_val = train_df.iloc[val_split_idx:]["result"].map({"H": 0, "D": 1, "A": 2}).values
    X_test = test_df[feature_cols]
    y_test = test_df["result"].map({"H": 0, "D": 1, "A": 2}).values
    
    # 训练 XGBoost
    print("\n训练 XGBoost 模型...")
    xgb_model = XGBoostModel()
    xgb_model.train(X_train, y_train, X_val, y_val)
    xgb_model.save_model()
    
    # 训练 LightGBM
    print("\n训练 LightGBM 模型...")
    lgb_model = LightGBMModel()
    lgb_model.train(X_train, y_train, X_val, y_val)
    lgb_model.save_model()
    
    # 保存特征工程对象
    joblib.dump(engineer, engineer_path)
    
    print("\n🎉 首次训练完成！模型已持久化保存。")
    print("="*80 + "\n")


def make_predictions(args):
    """Make predictions for upcoming matches"""
    print("\n" + "="*80)
    print("MAKING PREDICTIONS")
    print("="*80 + "\n")
    
    # Load models
    import joblib
    
    try:
        xgb_model = XGBoostModel().load_model()
        lgb_model = LightGBMModel().load_model()
        engineer = joblib.load(config.MODELS_DIR / "feature_engineer.joblib")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run --train first to train the models.")
        return
    
    # Create ensemble
    ensemble = EnsembleModel(
        models=[xgb_model, lgb_model]
    )
    
    # Load historical data - try CSV first for better feature engineering
    csv_collector = FootballDataCSVCollector()
    historical_data = csv_collector.load_data()
    
    if historical_data.empty:
        print("No CSV historical data found, trying API data...")
        collector = FootballDataCollector()
        historical_data = collector.load_data()
    
    if historical_data.empty:
        print("No historical data found!")
        return
    
    # Preprocess historical data
    preprocessor = DataPreprocessor()
    historical_data = preprocessor.clean_data(historical_data)
    historical_data = preprocessor.encode_results(historical_data)
    
    # Get upcoming matches
    leagues = args.leagues.split(",") if args.leagues else ["PL"]
    days_ahead = args.days or 7
    
    # Use API collector for upcoming matches
    api_collector = FootballDataCollector()
    all_upcoming = []
    for league in leagues:
        upcoming = api_collector.get_upcoming_matches(league, days_ahead=days_ahead)
        if not upcoming.empty:
            all_upcoming.append(upcoming)
    
    if not all_upcoming:
        print(f"No upcoming matches found in the next {days_ahead} days.")
        return
    
    upcoming_matches = pd.concat(all_upcoming, ignore_index=True)
    print(f"Found {len(upcoming_matches)} upcoming matches\n")
    
    # Make predictions
    predictor = MatchPredictor(ensemble, engineer)
    predictions = predictor.predict_matches(upcoming_matches, historical_data)
    
    # Filter high confidence predictions
    high_conf = predictor.filter_high_confidence(predictions)
    
    # Display predictions
    print("\n" + "="*80)
    print("HIGH CONFIDENCE PREDICTIONS")
    print("="*80 + "\n")
    
    pred_df = predictor.to_dataframe(high_conf)
    if not pred_df.empty:
        print(pred_df.to_string(index=False))
    else:
        print("No high confidence predictions found.")
    
    # Fetch bookmaker odds and identify value bets
    value_df = pd.DataFrame() # 初始化
    if args.odds:
        print("\n" + "="*80)
        print("FETCHING BOOKMAKER ODDS & IDENTIFYING VALUE BETS")
        print("="*80 + "\n")
        
        odds_collector = OddsCollector()
        odds_data = odds_collector.get_multiple_leagues(leagues)
        
        if odds_data:
            analyzer = OddsAnalyzer()
            value_bets = analyzer.find_value_bets(predictions, odds_data)
            
            if value_bets:
                # 转为DataFrame处理
                raw_value_df = analyzer.to_dataframe(value_bets)
                
                # ==========================================
                # 【新增功能】：严格门槛过滤 (网页和微信双重生效)
                # 过滤条件: 信心(confidence) >= 65% (0.65) 且 优势(edge) >= 15% (0.15)
                # ==========================================
                value_df = raw_value_df[(raw_value_df['confidence'] >= 0.65) & (raw_value_df['edge'] >= 0.15)].copy()
                
                # 无论是否为空，都必须保存CSV给网页读取（空数据会让网页显示今日0场）
                output_file = config.DATA_DIR / f"value_bets_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
                value_df.to_csv(output_file, index=False)
                
                if not value_df.empty:
                    print(f"\n✓ 过滤完成：筛选出 {len(value_df)} 场高标准赛事，已保存至 {output_file}")
                else:
                    print("\n✓ 过滤完成：今日暂无满足高标准 (信心≥65%且优势≥15%) 的赛事。")
            else:
                print("No value bets found with current criteria.")
        else:
            print("Could not fetch odds data. Check your API settings.")
    else:
        print("\n" + "="*80)
        print("NOTE: Add --odds flag to fetch bookmaker odds and identify value bets")
        print("="*80 + "\n")

    # ==========================================
    # PushPlus 微信自动推送逻辑 (极简精炼直观版)
    # ==========================================
    push_token = os.getenv("PUSHPLUS_TOKEN", "")
    if push_token:
        print("\n" + "="*80)
        print("正在生成报告并推送到微信 (PushPlus)...")
        print("="*80)
        
        push_html = ""
        
        # 只推送满足严格条件的赛事
        if args.odds and not value_df.empty:
            push_html += "<h3 style='color: #2c3e50; border-bottom: 2px solid #e74c3c; padding-bottom: 8px;'>🔥 严选高价值赛事推荐</h3>"
            
            for _, row in value_df.iterrows():
                match_name = f"{row.get('home_team', '')} vs {row.get('away_team', '')}"
                conf = float(row.get('confidence', 0)) * 100
                edge = float(row.get('edge', 0)) * 100
                outcome = row.get('outcome', '')
                
                # 时间转换：UTC转北京时间 (UTC+8)
                try:
                    utc_time = pd.to_datetime(row.get('date'))
                    if utc_time.tzinfo is None:
                        utc_time = utc_time.tz_localize('UTC')
                    bj_time = utc_time.tz_convert('Asia/Shanghai').strftime('%m-%d %H:%M')
                except Exception:
                    bj_time = "时间格式未知"
                
                # 全新极简排版：双方队名 + 时间 + 推荐 + 信心价值
                push_html += f"""
                <div style="margin-bottom: 15px; border-radius: 6px; background-color: #f8f9fa; padding: 12px; border: 1px solid #e9ecef;">
                    <div style="font-size: 16px; font-weight: bold; color: #34495e; margin-bottom: 8px;">⚽ {match_name}</div>
                    <div style="font-size: 14px; color: #555; line-height: 1.8;">
                        ⏰ 比赛时间: <span style="color: #333; font-weight: bold;">{bj_time}</span><br>
                        🎯 推荐方向: <span style="color: #e74c3c; font-weight: bold; font-size: 15px;">{outcome}</span><br>
                        💎 信心: <span style="font-weight: bold;">{conf:.1f}%</span> &nbsp;|&nbsp; 📈 价值: <span style="color: #27ae60; font-weight: bold;">+{edge:.1f}%</span>
                    </div>
                </div>
                """
        else:
            # 如果没通过筛选，则下发这句提示
            push_html = "<div style='text-align: center; padding: 20px; color: #7f8c8d; font-size: 15px;'>💡 今日暂无符合高标准（信心≥65% 且 价值≥15%）的赛事。</div>"
            
        # 请求参数
        push_url = "http://www.pushplus.plus/send"
        push_data = {
            "token": push_token,
            "title": f"价值投注65%+edge15%以上场次推荐 ({pd.Timestamp.now().strftime('%m-%d')})",
            "content": push_html,
            "template": "html"
        }
        
        try:
            res = requests.post(push_url, json=push_data)
            if res.status_code == 200 and res.json().get("code") == 200:
                print("✓ 成功：格式化预测报告已推送到您的微信！\n")
            else:
                print(f"⚠️ PushPlus 推送失败，返回信息: {res.text}\n")
        except Exception as e:
            print(f"⚠️ PushPlus 网络请求报错: {e}\n")
    else:
        print("\n📝 未在环境变量中检测到 PUSHPLUS_TOKEN，跳过微信推送。\n")


def run_backtest(args):
    """Run backtest on historical data"""
    print("\n" + "="*80)
    print("RUNNING BACKTEST")
    print("="*80 + "\n")
    
    # Load models
    import joblib
    
    try:
        xgb_model = XGBoostModel().load_model()
        lgb_model = LightGBMModel().load_model()
        engineer = joblib.load(config.MODELS_DIR / "feature_engineer.joblib")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run --train first to train the models.")
        return
    
    # Create ensemble
    ensemble = EnsembleModel(
        models=[xgb_model, lgb_model],
        weights=[0.5, 0.5]
    )
    
    # Load and prepare data
    collector = FootballDataCollector()
    df = collector.load_data()
    
    preprocessor = DataPreprocessor()
    df_clean = preprocessor.clean_data(df)
    df_encoded = preprocessor.encode_results(df_clean)
    df_features = engineer.create_all_features(df_encoded)
    
    # Run backtest
    backtester = Backtester(ensemble, engineer)
    
    start_date = args.start_date
    end_date = args.end_date
    
    backtest_results = backtester.backtest(
        df_features,
        start_date=start_date,
        end_date=end_date
    )
    
    # Simulate betting
    betting_results = backtester.simulate_betting(
        backtest_results,
        min_confidence=0.55,
        bankroll=1000,
        stake_per_bet=10
    )
    
    # Print summary
    backtester.print_summary(backtest_results, betting_results)


def main():
    parser = argparse.ArgumentParser(
        description="Football Match Prediction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Train models (Run once to save permanent models)
  python main.py --train --leagues PL,LaLiga,SerieA
  
  # Make predictions (Will skip training if models exist)
  python main.py --train --predict --odds --leagues PL --days 3
        """
    )
    
    # Main actions
    parser.add_argument("--collect-csv", action="store_true", 
                       help="Collect historical CSV data from football-data.co.uk")
    parser.add_argument("--collect", action="store_true", 
                       help="Collect data from Football-Data.org API")
    parser.add_argument("--train", action="store_true", help="Train prediction models")
    parser.add_argument("--predict", action="store_true", help="Predict upcoming matches")
    parser.add_argument("--backtest", action="store_true", help="Run backtest on historical data")
    
    # Options
    parser.add_argument("--leagues", type=str, 
                       help="Comma-separated league codes")
    parser.add_argument("--seasons", type=str, 
                       help="Comma-separated season years")
    parser.add_argument("--start-season", type=str, 
                       help="Start season for CSV collection")
    parser.add_argument("--end-season", type=str, 
                       help="End season for CSV collection")
    parser.add_argument("--days", type=int, help="Days ahead for upcoming matches")
    parser.add_argument("--start-date", type=str, help="Backtest start date")
    parser.add_argument("--end-date", type=str, help="Backtest end date")
    parser.add_argument("--odds", action="store_true", 
                       help="Fetch bookmaker odds and identify value bets")
    
    args = parser.parse_args()
    
    # Execute actions
    if args.collect_csv:
        collect_csv_data(args)
    
    if args.collect:
        collect_data(args)
    
    if args.train:
        train_models(args)
    
    if args.predict:
        make_predictions(args)
    
    if args.backtest:
        run_backtest(args)
    
    if not any([args.collect_csv, args.collect, args.train, args.predict, args.backtest]):
        parser.print_help()


if __name__ == "__main__":
    main()
