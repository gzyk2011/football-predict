"""
Backtesting Framework
Evaluates model performance on historical data
"""
import pandas as pd
import numpy as np
from datetime import datetime
import config


class Backtester:
    """Backtests prediction models on historical data"""
    
    def __init__(self, model, feature_engineer):
        """
        Args:
            model: Trained prediction model
            feature_engineer: FeatureEngineer instance
        """
        self.model = model
        self.feature_engineer = feature_engineer
    
    def backtest(self, data, start_date=None, end_date=None):
        """
        Run backtest on historical data
        
        Args:
            data: DataFrame with historical matches (with features)
            start_date: Start date for backtest period
            end_date: End date for backtest period
        
        Returns:
            Dictionary with backtest results
        """
        df = data.copy()
        
        # Filter by date range if specified
        if start_date:
            df = df[df["date"] >= pd.to_datetime(start_date, utc=True)]
        if end_date:
            df = df[df["date"] <= pd.to_datetime(end_date, utc=True)]
        
        print(f"Backtesting on {len(df)} matches from {df['date'].min()} to {df['date'].max()}")
        
        # Get features and targets
        X = df[self.model.feature_columns]
        
        # Create target labels (0=Home, 1=Draw, 2=Away)
        y_true = df["result"].map({"H": 0, "D": 1, "A": 2}).values
        
        # Make predictions
        y_pred_proba = self.model.predict_proba(X)
        y_pred = np.argmax(y_pred_proba, axis=1)
        
        # Calculate metrics
        results = self._calculate_metrics(y_true, y_pred, y_pred_proba)
        
        # Add prediction details to dataframe
        df["pred_home_prob"] = y_pred_proba[:, 0]
        df["pred_draw_prob"] = y_pred_proba[:, 1]
        df["pred_away_prob"] = y_pred_proba[:, 2]
        df["predicted_outcome"] = pd.Series(y_pred).map({0: "H", 1: "D", 2: "A"}).values
        df["prediction_correct"] = (y_true == y_pred)
        df["prediction_confidence"] = np.max(y_pred_proba, axis=1)
        
        results["detailed_predictions"] = df
        
        return results
    
    def _calculate_metrics(self, y_true, y_pred, y_pred_proba):
        """Calculate performance metrics"""
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score, f1_score,
            log_loss, confusion_matrix, classification_report
        )
        import numpy as np
        
        # Basic metrics
        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, average="macro", zero_division=0)
        recall = recall_score(y_true, y_pred, average="macro", zero_division=0)
        f1 = f1_score(y_true, y_pred, average="macro", zero_division=0)
        logloss = log_loss(y_true, y_pred_proba, labels=np.array([0, 1, 2]))
        
        # Confusion matrix (ensure 3x3 even if some classes missing)
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1, 2])
        
        # Class-wise accuracy
        result_map = {0: "Home Win", 1: "Draw", 2: "Away Win"}
        y_true_labels = [result_map[y] for y in y_true]
        y_pred_labels = [result_map[y] for y in y_pred]
        
        report = classification_report(y_true_labels, y_pred_labels, output_dict=True)
        
        # Calculate accuracy by confidence level
        confidence_analysis = self._analyze_by_confidence(y_true, y_pred, y_pred_proba)
        
        results = {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "log_loss": logloss,
            "confusion_matrix": cm,
            "classification_report": report,
            "confidence_analysis": confidence_analysis,
            "total_predictions": len(y_true),
            "correct_predictions": int(np.sum(y_true == y_pred))
        }
        
        return results
    
    def _analyze_by_confidence(self, y_true, y_pred, y_pred_proba):
        """Analyze prediction accuracy by confidence level"""
        confidence = np.max(y_pred_proba, axis=1)
        correct = (y_true == y_pred)
        
        # Define confidence buckets
        buckets = [
            (0.33, 0.40, "Very Low (33-40%)"),
            (0.40, 0.50, "Low (40-50%)"),
            (0.50, 0.60, "Medium (50-60%)"),
            (0.60, 0.70, "High (60-70%)"),
            (0.70, 1.00, "Very High (70%+)")
        ]
        
        analysis = []
        
        for min_conf, max_conf, label in buckets:
            mask = (confidence >= min_conf) & (confidence < max_conf)
            n_predictions = np.sum(mask)
            
            if n_predictions > 0:
                accuracy = np.mean(correct[mask])
                analysis.append({
                    "confidence_range": label,
                    "n_predictions": int(n_predictions),
                    "accuracy": accuracy,
                    "avg_confidence": np.mean(confidence[mask])
                })
        
        return analysis
    
    def simulate_betting(self, backtest_results, odds_data=None, 
                        min_confidence=None, bankroll=1000, stake_per_bet=10):
        """
        Simulate betting strategy on backtest results
        
        Args:
            backtest_results: Results from backtest() method
            odds_data: DataFrame with actual odds (if available)
            min_confidence: Minimum confidence threshold for placing bets
            bankroll: Initial bankroll
            stake_per_bet: Fixed stake per bet (or percentage if < 1)
        
        Returns:
            Dictionary with betting simulation results
        """
        min_conf = min_confidence or config.MIN_CONFIDENCE
        df = backtest_results["detailed_predictions"].copy()
        
        # Filter by confidence
        df_bets = df[df["prediction_confidence"] >= min_conf].copy()
        
        print(f"\nSimulating betting strategy:")
        print(f"  Minimum confidence: {min_conf:.0%}")
        print(f"  Initial bankroll: ${bankroll}")
        print(f"  Stake per bet: ${stake_per_bet}")
        print(f"  Total bets: {len(df_bets)}")
        
        # Simple simulation assuming average odds
        # In reality, you'd use actual bookmaker odds
        average_odds = {
            "H": 2.20,  # Home win
            "D": 3.30,  # Draw
            "A": 3.00   # Away win
        }
        
        current_bankroll = bankroll
        total_wagered = 0
        total_won = 0
        wins = 0
        losses = 0
        
        bankroll_history = [bankroll]
        
        for _, bet in df_bets.iterrows():
            # Bet on predicted outcome
            predicted = bet["predicted_outcome"]
            actual = bet["result"]
            
            # Determine stake (fixed amount or percentage)
            stake = stake_per_bet if stake_per_bet >= 1 else current_bankroll * stake_per_bet
            stake = min(stake, current_bankroll)  # Can't bet more than we have
            
            if stake <= 0:
                break  # Bankrupt
            
            total_wagered += stake
            
            # Check if bet won
            if predicted == actual:
                # Win: get back stake + profit
                payout = stake * average_odds[predicted]
                profit = payout - stake
                current_bankroll += profit
                total_won += payout
                wins += 1
            else:
                # Loss: lose stake
                current_bankroll -= stake
                losses += 1
            
            bankroll_history.append(current_bankroll)
        
        # Calculate metrics
        roi = ((current_bankroll - bankroll) / bankroll) * 100
        profit = current_bankroll - bankroll
        win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0
        
        results = {
            "initial_bankroll": bankroll,
            "final_bankroll": current_bankroll,
            "profit": profit,
            "roi_percent": roi,
            "total_bets": wins + losses,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total_wagered": total_wagered,
            "total_won": total_won,
            "bankroll_history": bankroll_history
        }
        
        return results
    
    def print_summary(self, backtest_results, betting_results=None):
        """Print formatted summary of backtest results"""
        print("\n" + "="*80)
        print("BACKTEST RESULTS SUMMARY")
        print("="*80)
        
        print(f"\nPrediction Performance:")
        print(f"  Total Predictions: {backtest_results['total_predictions']}")
        print(f"  Correct Predictions: {backtest_results['correct_predictions']}")
        print(f"  Accuracy: {backtest_results['accuracy']:.2%}")
        print(f"  Precision: {backtest_results['precision']:.2%}")
        print(f"  Recall: {backtest_results['recall']:.2%}")
        print(f"  F1 Score: {backtest_results['f1_score']:.3f}")
        print(f"  Log Loss: {backtest_results['log_loss']:.3f}")
        
        print(f"\nAccuracy by Confidence Level:")
        for conf in backtest_results["confidence_analysis"]:
            print(f"  {conf['confidence_range']}: "
                  f"{conf['accuracy']:.2%} ({conf['n_predictions']} predictions)")
        
        print(f"\nConfusion Matrix:")
        cm = backtest_results["confusion_matrix"]
        print(f"                Predicted")
        print(f"              H     D     A")
        print(f"Actual  H   {cm[0, 0]:4d}  {cm[0, 1]:4d}  {cm[0, 2]:4d}")
        print(f"        D   {cm[1, 0]:4d}  {cm[1, 1]:4d}  {cm[1, 2]:4d}")
        print(f"        A   {cm[2, 0]:4d}  {cm[2, 1]:4d}  {cm[2, 2]:4d}")
        
        if betting_results:
            print(f"\nBetting Simulation:")
            print(f"  Initial Bankroll: ${betting_results['initial_bankroll']:.2f}")
            print(f"  Final Bankroll: ${betting_results['final_bankroll']:.2f}")
            print(f"  Profit/Loss: ${betting_results['profit']:.2f}")
            print(f"  ROI: {betting_results['roi_percent']:.2f}%")
            print(f"  Win Rate: {betting_results['win_rate']:.2%}")
            print(f"  Total Bets: {betting_results['total_bets']} "
                  f"(W: {betting_results['wins']}, L: {betting_results['losses']})")
        
        print("="*80 + "\n")
