"""
Main Execution Script
Orchestrates data collection, model training, prediction, and backtesting
"""
import argparse
import sys
import os
from pathlib import Path
import pandas as pd

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
    """Train prediction models"""
    print("\n" + "="*80)
    print("TRAINING MODELS")
    print("="*80 + "\n")
    
    # Load data - try historical CSV first, then API data
    csv_collector = FootballDataCSVCollector()
    df = csv_collector.load_data()
    
    if df.empty:
        print("No historical CSV data found, trying API data...")
        api_collector = FootballDataCollector()
        df = api_collector.load_data()
    
    if df.empty:
        print("No data found! Please run --collect-csv or --collect first.")
        return
    
    print(f"Loaded {len(df)} matches")
    data_source = "CSV historical data" if not df.empty else "API data"
    print(f"Using: {data_source}\n")
    
    if df.empty:
        print("No data found! Please run --collect first.")
        return
    
    # Preprocess
    preprocessor = DataPreprocessor()
    df_clean = preprocessor.clean_data(df)
    df_encoded = preprocessor.encode_results(df_clean)
    
    # Create features
    engineer = FeatureEngineer()
    df_features = engineer.create_all_features(df_encoded)
    
    print(f"\nDataset: {len(df_features)} matches with {len(engineer.get_feature_columns())} features")
    
    # Split data into train, validation, and test
    train_df, test_df = preprocessor.split_train_test(df_features, test_size=0.2, by_date=True)
    
    # Further split training into train and validation for calibration
    val_split_idx = int(len(train_df) * 0.85)
    train_subset = train_df.iloc[:val_split_idx]
    val_subset = train_df.iloc[val_split_idx:]
    
    # Prepare training data
    feature_cols = engineer.get_feature_columns()
    X_train = train_subset[feature_cols]
    y_train = train_subset["result"].map({"H": 0, "D": 1, "A": 2}).values
    
    X_val = val_subset[feature_cols]
    y_val = val_subset["result"].map({"H": 0, "D": 1, "A": 2}).values
    
    X_test = test_df[feature_cols]
    y_test = test_df["result"].map({"H": 0, "D": 1, "A": 2}).values
    
    print(f"Training set: {len(train_subset)} matches ({train_subset['date'].min()} to {train_subset['date'].max()})")
    print(f"Validation set: {len(val_subset)} matches ({val_subset['date'].min()} to {val_subset['date'].max()})")
    print(f"Testing set: {len(test_df)} matches ({test_df['date'].min()} to {test_df['date'].max()})")
    
    # Train XGBoost with calibration
    print("\n" + "-"*80)
    xgb_model = XGBoostModel()
    xgb_model.train(X_train, y_train, X_val, y_val)
    xgb_results = xgb_model.evaluate(X_test, y_test)
    print(f"\nXGBoost Test Accuracy: {xgb_results['accuracy']:.2%}")
    print(f"XGBoost Log Loss: {xgb_results['log_loss']:.3f}")
    
    # Train LightGBM with calibration
    print("\n" + "-"*80)
    lgb_model = LightGBMModel()
    lgb_model.train(X_train, y_train, X_val, y_val)
    lgb_results = lgb_model.evaluate(X_test, y_test)
    print(f"\nLightGBM Test Accuracy: {lgb_results['accuracy']:.2%}")
    print(f"LightGBM Log Loss: {lgb_results['log_loss']:.3f}")
    
    # Create ensemble with optimized weights
    print("\n" + "-"*80)
    print("\nCreating ensemble model...")
    ensemble = EnsembleModel(
        models=[xgb_model, lgb_model],
        name="ensemble_model"
    )
    ensemble_results = ensemble.evaluate(X_test, y_test)
    print(f"Ensemble Test Accuracy: {ensemble_results['accuracy']:.2%}")
    print(f"Ensemble Log Loss: {ensemble_results['log_loss']:.3f}")
    
    # Save models
    print("\nSaving models...")
    xgb_model.save_model()
    lgb_model.save_model()
    
    # Save feature engineer settings
    import joblib
    joblib.dump(engineer, config.MODELS_DIR / "feature_engineer.joblib")
    print("Feature engineer saved")
    
    print("\n" + "="*80)
    print("TRAINING COMPLETE")
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
    if args.odds:
        print("\n" + "="*80)
        print("FETCHING BOOKMAKER ODDS & IDENTIFYING VALUE BETS")
        print("="*80 + "\n")
        
        odds_collector = OddsCollector()
        odds_data = odds_collector.get_multiple_leagues(leagues)
        
        if odds_data:
            odds_collector.display_odds(odds_data)
            
            # Analyze value bets
            analyzer = OddsAnalyzer()
            value_bets = analyzer.find_value_bets(predictions, odds_data)
            
            if value_bets:
                analyzer.display_value_bets(value_bets, top_n=10)
                
                # Save to CSV
                value_df = analyzer.to_dataframe(value_bets)
                output_file = config.DATA_DIR / f"value_bets_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
                value_df.to_csv(output_file, index=False)
                print(f"\n✓ Value bets saved to {output_file}")
            else:
                print("No value bets found with current criteria.")
        else:
            print("Could not fetch odds data. Check your ODDS_API_KEY in .env")
    else:
        print("\n" + "="*80)
        print("NOTE: Add --odds flag to fetch bookmaker odds and identify value bets")
        print("Get free API key from: https://the-odds-api.com/")
        print("="*80 + "\n")


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
  # Collect historical CSV data (RECOMMENDED - more data, free)
  python main.py --collect-csv --leagues PL --start-season 1819 --end-season 2324
  
  # Collect data from API (limited to current season with free tier)
  python main.py --collect --leagues PL --seasons 2023
  
  # Train models
  python main.py --train
  
  # Make predictions for upcoming matches
  python main.py --predict --leagues PL --days 7
  
  # Run backtest
  python main.py --backtest --start-date 2023-01-01 --end-date 2024-01-01
        """
    )
    
    # Main actions
    parser.add_argument("--collect-csv", action="store_true", 
                       help="Collect historical CSV data from football-data.co.uk (FREE, RECOMMENDED)")
    parser.add_argument("--collect", action="store_true", 
                       help="Collect data from Football-Data.org API (requires API key)")
    parser.add_argument("--train", action="store_true", help="Train prediction models")
    parser.add_argument("--predict", action="store_true", help="Predict upcoming matches")
    parser.add_argument("--backtest", action="store_true", help="Run backtest on historical data")
    
    # Options
    parser.add_argument("--leagues", type=str, 
                       help="Comma-separated league codes (e.g., PL,LaLiga,SerieA)")
    parser.add_argument("--seasons", type=str, 
                       help="Comma-separated season years for API (e.g., 2021,2022,2023)")
    parser.add_argument("--start-season", type=str, 
                       help="Start season for CSV collection (e.g., 1819 for 2018/19)")
    parser.add_argument("--end-season", type=str, 
                       help="End season for CSV collection (e.g., 2324 for 2023/24)")
    parser.add_argument("--days", type=int, help="Days ahead for upcoming matches (default: 7)")
    parser.add_argument("--start-date", type=str, help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--odds", action="store_true", 
                       help="Fetch bookmaker odds and identify value bets (requires ODDS_API_KEY)")
    
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
