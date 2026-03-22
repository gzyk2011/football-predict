"""
Match Predictor
Makes predictions for upcoming matches
"""
import pandas as pd
import numpy as np
from datetime import datetime
import config


# Team name mapping between API (full names) and CSV (short names)
TEAM_NAME_MAPPING = {
    # Premier League
    "Arsenal FC": "Arsenal",
    "Aston Villa FC": "Aston Villa",
    "AFC Bournemouth": "Bournemouth",
    "Brentford FC": "Brentford",
    "Brighton & Hove Albion FC": "Brighton",
    "Burnley FC": "Burnley",
    "Chelsea FC": "Chelsea",
    "Crystal Palace FC": "Crystal Palace",
    "Everton FC": "Everton",
    "Fulham FC": "Fulham",
    "Liverpool FC": "Liverpool",
    "Luton Town FC": "Luton",
    "Manchester City FC": "Man City",
    "Manchester United FC": "Man United",
    "Newcastle United FC": "Newcastle",
    "Nottingham Forest FC": "Nott'm Forest",
    "Sheffield United FC": "Sheffield United",
    "Tottenham Hotspur FC": "Tottenham",
    "West Ham United FC": "West Ham",
    "Wolverhampton Wanderers FC": "Wolves",
    "Leicester City FC": "Leicester",
    "Leeds United FC": "Leeds",
    "Southampton FC": "Southampton",
    "Watford FC": "Watford",
    "Norwich City FC": "Norwich",
    "Sunderland AFC": "Sunderland",
    "Cardiff City FC": "Cardiff",
    "Huddersfield Town AFC": "Huddersfield",
    "Hull City AFC": "Hull",
    "Middlesbrough FC": "Middlesbrough",
    "Queens Park Rangers FC": "QPR",
    "Stoke City FC": "Stoke",
    "Swansea City AFC": "Swansea",
    "West Bromwich Albion FC": "West Brom",
    
    # La Liga
    "Athletic Club": "Ath Bilbao",
    "Club Atlético de Madrid": "Ath Madrid",
    "FC Barcelona": "Barcelona",
    "Real Madrid CF": "Real Madrid",
    "Sevilla FC": "Sevilla",
    "Real Betis Balompié": "Betis",
    "Valencia CF": "Valencia",
    "Villarreal CF": "Villarreal",
    "Real Sociedad de Fútbol": "Sociedad",
    
    # Serie A
    "AC Milan": "Milan",
    "FC Internazionale Milano": "Inter",
    "Juventus FC": "Juventus",
    "AS Roma": "Roma",
    "SSC Napoli": "Napoli",
    "Atalanta BC": "Atalanta",
    "SS Lazio": "Lazio",
    "ACF Fiorentina": "Fiorentina",
    
    # Bundesliga
    "FC Bayern München": "Bayern Munich",
    "Borussia Dortmund": "Dortmund",
    "RB Leipzig": "RB Leipzig",
    "Bayer 04 Leverkusen": "Leverkusen",
    "Borussia Mönchengladbach": "M'gladbach",
    "VfL Wolfsburg": "Wolfsburg",
    "Eintracht Frankfurt": "Ein Frankfurt",
    "FC Schalke 04": "Schalke 04",
    
    # Ligue 1
    "Paris Saint-Germain FC": "Paris SG",
    "Olympique de Marseille": "Marseille",
    "Olympique Lyonnais": "Lyon",
    "AS Monaco FC": "Monaco",
    "OGC Nice": "Nice",
    "Stade Rennais FC 1901": "Rennes",
    "Lille OSC": "Lille",
}


def normalize_team_name(team_name):
    """
    Normalize team name to match CSV format
    
    Args:
        team_name: Team name from API or CSV
    
    Returns:
        Normalized team name
    """
    # Direct mapping
    if team_name in TEAM_NAME_MAPPING:
        return TEAM_NAME_MAPPING[team_name]
    
    # Already normalized or unknown team
    return team_name


class MatchPredictor:
    """Predicts outcomes for football matches"""
    
    def __init__(self, model, feature_engineer):
        """
        Args:
            model: Trained prediction model
            feature_engineer: FeatureEngineer instance for creating features
        """
        self.model = model
        self.feature_engineer = feature_engineer
        
        if not model.is_trained:
            raise ValueError("Model must be trained before making predictions")
    
    def predict_match(self, match_data, historical_data):
        """
        Predict outcome for a single match
        
        Args:
            match_data: Dict with match info (home_team, away_team, date)
            historical_data: DataFrame with historical matches for feature creation
        
        Returns:
            Dictionary with predictions
        """
        # Normalize team names to match CSV format
        home_team = normalize_team_name(match_data["home_team"])
        away_team = normalize_team_name(match_data["away_team"])
        match_date = pd.to_datetime(match_data.get("date", datetime.now()))
        if match_date.tzinfo is not None:
            match_date = match_date.tz_localize(None)
        
        # Get historical matches before this date (already cleaned from predict_matches)
        hist_before = historical_data[historical_data['date'] < match_date].copy()
        
        if len(hist_before) < 10:
            return {
                "home_team": home_team,
                "away_team": away_team,
                "error": "Insufficient historical data (need at least 10 matches)"
            }
        
        # Create a row with the match and extract features manually
        # Get form for both teams
        home_recent = hist_before[
            (hist_before['home_team'] == home_team) | 
            (hist_before['away_team'] == home_team)
        ].tail(5)
        
        away_recent = hist_before[
            (hist_before['home_team'] == away_team) | 
            (hist_before['away_team'] == away_team)
        ].tail(5)
        
        if len(home_recent) < 5 or len(away_recent) < 5:
            return {
                "home_team": match_data["home_team"],  # Original name for display
                "away_team": match_data["away_team"],  # Original name for display
                "error": "Insufficient recent matches for form calculation"
            }
        
        # Build a minimal DataFrame with just this match for feature engineering
        match_df = pd.DataFrame([{
            'date': match_date,
            'home_team': home_team,
            'away_team': away_team,
            'home_score': 0,  # Dummy
            'away_score': 0,  # Dummy
            'result': 'H',  # Dummy
            'competition': match_data.get('competition', 'Unknown')
        }])
        
        # Combine with ALL historical data, then feature engineer
        combined_df = pd.concat([hist_before, match_df], ignore_index=True)
        
        # Create features (suppress verbose output for cleaner prediction flow)
        featured_df = self.feature_engineer.create_all_features(combined_df, verbose=False)
        
        # Find our prediction match in the featured dataframe using normalized names
        prediction_row = featured_df[
            (featured_df['home_team'] == home_team) & 
            (featured_df['away_team'] == away_team)
        ].tail(1)
        
        # Get the last row (our match)
        if len(prediction_row) == 0:
            return {
                "home_team": match_data["home_team"],
                "away_team": match_data["away_team"],
                "error": "Insufficient historical data to make prediction"
            }
        
        match_features = prediction_row[self.model.feature_columns]
        
        # Make prediction
        probabilities = self.model.predict_proba(match_features)[0]
        
        prediction = {
            "home_team": match_data["home_team"],
            "away_team": match_data["away_team"],
            "date": match_data.get("date", "Unknown"),
            "probabilities": {
                "home_win": float(probabilities[0]),
                "draw": float(probabilities[1]),
                "away_win": float(probabilities[2])
            },
            "predicted_outcome": self._get_predicted_outcome(probabilities),
            "confidence": float(np.max(probabilities))
        }
        
        return prediction
    
    def predict_matches(self, matches_df, historical_data):
        """
        Predict outcomes for multiple matches
        
        Args:
            matches_df: DataFrame with upcoming matches
            historical_data: DataFrame with historical matches
        
        Returns:
            List of prediction dictionaries
        """
        print("Preparing historical data...")
        # Ensure datetime consistency once for all matches
        historical_data = historical_data.copy()
        if 'date' in historical_data.columns:
            historical_data['date'] = pd.to_datetime(historical_data['date']).dt.tz_localize(None)
        
        # Pre-compute features on historical data ONCE (this takes time but only done once)
        print("Computing historical features (one-time process)...")
        hist_with_features = self.feature_engineer.create_all_features(historical_data, verbose=True)
        print(f"✓ Historical features ready ({len(hist_with_features)} matches)\n")
        
        predictions = []
        total_matches = len(matches_df)
        
        for idx, match in matches_df.iterrows():
            print(f"Match {idx+1}/{total_matches}: ", end='')
            match_data = {
                "home_team": match["home_team"],
                "away_team": match["away_team"],
                "date": match.get("date", datetime.now()),
                "competition": match.get("competition", "Unknown"),
            }
            
            # Fast prediction using pre-computed features
            prediction = self._predict_match_fast(match_data, hist_with_features)
            
            if "match_id" in match:
                prediction["match_id"] = match["match_id"]
            
            predictions.append(prediction)
            
            # Display result
            print(f"{prediction['home_team']} vs {prediction['away_team']}", end=' - ')
            if 'error' in prediction:
                print(f"⚠️  {prediction['error']}")
            else:
                print(f"✓ {prediction['predicted_outcome']} ({prediction['confidence']:.1%})")
        
        return predictions
    
    def _predict_match_fast(self, match_data, hist_with_features):
        """
        Fast prediction using pre-computed historical features
        
        Args:
            match_data: Dict with match info
            hist_with_features: Historical data with features already computed
        
        Returns:
            Prediction dictionary
        """
        # Normalize team names
        home_team = normalize_team_name(match_data["home_team"])
        away_team = normalize_team_name(match_data["away_team"])
        match_date = pd.to_datetime(match_data.get("date", datetime.now()))
        if match_date.tzinfo is not None:
            match_date = match_date.tz_localize(None)
        
        # Filter historical data before match date
        hist_before = hist_with_features[hist_with_features['date'] < match_date]
        
        if len(hist_before) < 10:
            return {
                "home_team": match_data["home_team"],
                "away_team": match_data["away_team"],
                "error": "Insufficient data"
            }
        
        # Get most recent match for each team to extract their latest features
        home_recent = hist_before[
            (hist_before['home_team'] == home_team) | 
            (hist_before['away_team'] == home_team)
        ].tail(1)
        
        away_recent = hist_before[
            (hist_before['home_team'] == away_team) | 
            (hist_before['away_team'] == away_team)
        ].tail(1)
        
        if len(home_recent) == 0 or len(away_recent) == 0:
            return {
                "home_team": match_data["home_team"],
                "away_team": match_data["away_team"],
                "error": "No recent data"
            }
        
        # Build prediction features by combining team stats
        # This is a simplified approach - we extract features from recent matches
        try:
            # Get feature columns that exist
            feature_cols = [col for col in self.model.feature_columns if col in hist_before.columns]
            
            # Calculate match features based on team histories
            # Use averages from recent team performances
            home_feats = home_recent[feature_cols].iloc[0] if len(home_recent) > 0 else pd.Series(0, index=feature_cols)
            away_feats = away_recent[feature_cols].iloc[0] if len(away_recent) > 0 else pd.Series(0, index=feature_cols)
            
            # Create match features (simplified - use home team's perspective)
            match_features = pd.DataFrame([home_feats], columns=feature_cols)
            
            # Make prediction
            probabilities = self.model.predict_proba(match_features)[0]
            
            return {
                "home_team": match_data["home_team"],
                "away_team": match_data["away_team"],
                "date": match_data.get("date", "Unknown"),
                "probabilities": {
                    "home_win": float(probabilities[0]),
                    "draw": float(probabilities[1]),
                    "away_win": float(probabilities[2])
                },
                "predicted_outcome": self._get_predicted_outcome(probabilities),
                "confidence": float(np.max(probabilities))
            }
        except Exception as e:
            return {
                "home_team": match_data["home_team"],
                "away_team": match_data["away_team"],
                "error": f"Prediction failed: {str(e)}"
            }
    
    def _get_predicted_outcome(self, probabilities):
        """Convert probabilities to readable outcome"""
        outcomes = ["Home Win", "Draw", "Away Win"]
        return outcomes[np.argmax(probabilities)]
    
    def filter_high_confidence(self, predictions, min_confidence=None):
        """
        Filter predictions by confidence threshold
        
        Args:
            predictions: List of prediction dictionaries
            min_confidence: Minimum confidence threshold (uses config default if None)
        
        Returns:
            Filtered list of predictions
        """
        threshold = min_confidence or config.MIN_CONFIDENCE
        
        filtered = [
            pred for pred in predictions
            if 'confidence' in pred and pred["confidence"] >= threshold
        ]
        
        print(f"High confidence predictions ({threshold:.0%}+): {len(filtered)}/{len(predictions)}")
        return filtered
    
    def to_dataframe(self, predictions):
        """Convert predictions to DataFrame for easy viewing"""
        rows = []
        
        for pred in predictions:
            row = {
                "home_team": pred["home_team"],
                "away_team": pred["away_team"],
                "date": pred.get("date", ""),
                "predicted_outcome": pred["predicted_outcome"],
                "confidence": pred["confidence"],
                "home_win_prob": pred["probabilities"]["home_win"],
                "draw_prob": pred["probabilities"]["draw"],
                "away_win_prob": pred["probabilities"]["away_win"]
            }
            rows.append(row)
        
        return pd.DataFrame(rows)
