"""
Data Preprocessing Module
Cleans and prepares data for feature engineering
"""
import pandas as pd
import numpy as np


class DataPreprocessor:
    """Preprocesses football match data"""
    
    def __init__(self):
        pass
    
    def clean_data(self, df):
        """
        Clean and validate match data
        
        Args:
            df: Raw match DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        df = df.copy()
        
        # Remove duplicates
        df = df.drop_duplicates(subset=["match_id"], keep="first")
        
        # Remove matches with missing scores
        df = df.dropna(subset=["home_score", "away_score"])
        
        # Ensure scores are integers
        df["home_score"] = df["home_score"].astype(int)
        df["away_score"] = df["away_score"].astype(int)
        
        # Ensure date is datetime
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"])
        
        # Sort by date
        df = df.sort_values("date").reset_index(drop=True)
        
        # Validate result labels
        if "result" not in df.columns:
            df["result"] = df.apply(
                lambda row: "H" if row["home_score"] > row["away_score"]
                else "A" if row["away_score"] > row["home_score"]
                else "D",
                axis=1
            )
        
        # Add derived metrics
        if "goal_diff" not in df.columns:
            df["goal_diff"] = df["home_score"] - df["away_score"]
        
        if "total_goals" not in df.columns:
            df["total_goals"] = df["home_score"] + df["away_score"]
        
        return df
    
    def encode_results(self, df):
        """
        Create binary target variables for classification
        
        Args:
            df: Match DataFrame
        
        Returns:
            DataFrame with encoded target variables
        """
        df = df.copy()
        
        # Create binary targets
        df["home_win"] = (df["result"] == "H").astype(int)
        df["draw"] = (df["result"] == "D").astype(int)
        df["away_win"] = (df["result"] == "A").astype(int)
        
        # Over/under 2.5 goals
        df["over_2.5"] = (df["total_goals"] > 2.5).astype(int)
        
        # Both teams to score
        df["btts"] = ((df["home_score"] > 0) & (df["away_score"] > 0)).astype(int)
        
        return df
    
    def split_train_test(self, df, test_size=0.2, by_date=True):
        """
        Split data into training and testing sets
        
        Args:
            df: Match DataFrame
            test_size: Proportion of data for testing
            by_date: If True, split by date (more realistic); if False, random split
        
        Returns:
            train_df, test_df
        """
        df = df.copy()
        
        if by_date:
            # Split by date to avoid data leakage
            split_date = df["date"].quantile(1 - test_size)
            train_df = df[df["date"] < split_date].copy()
            test_df = df[df["date"] >= split_date].copy()
        else:
            # Random split
            train_df = df.sample(frac=1-test_size, random_state=42)
            test_df = df.drop(train_df.index)
        
        print(f"Training set: {len(train_df)} matches ({train_df['date'].min()} to {train_df['date'].max()})")
        print(f"Testing set: {len(test_df)} matches ({test_df['date'].min()} to {test_df['date'].max()})")
        
        return train_df, test_df
    
    def get_team_stats(self, df):
        """
        Calculate overall team statistics
        
        Args:
            df: Match DataFrame
        
        Returns:
            DataFrame with team statistics
        """
        stats = []
        
        # Get unique teams
        teams = pd.concat([
            df[["home_team", "home_team_id"]].rename(columns={"home_team": "team", "home_team_id": "team_id"}),
            df[["away_team", "away_team_id"]].rename(columns={"away_team": "team", "away_team_id": "team_id"})
        ]).drop_duplicates()
        
        for _, team_row in teams.iterrows():
            team = team_row["team"]
            team_id = team_row["team_id"]
            
            # Home matches
            home_matches = df[df["home_team"] == team]
            # Away matches
            away_matches = df[df["away_team"] == team]
            
            # Overall stats
            total_matches = len(home_matches) + len(away_matches)
            
            if total_matches == 0:
                continue
            
            # Wins, draws, losses
            home_wins = (home_matches["result"] == "H").sum()
            away_wins = (away_matches["result"] == "A").sum()
            home_draws = (home_matches["result"] == "D").sum()
            away_draws = (away_matches["result"] == "D").sum()
            
            total_wins = home_wins + away_wins
            total_draws = home_draws + away_draws
            total_losses = total_matches - total_wins - total_draws
            
            # Goals
            goals_scored = home_matches["home_score"].sum() + away_matches["away_score"].sum()
            goals_conceded = home_matches["away_score"].sum() + away_matches["home_score"].sum()
            
            stats.append({
                "team": team,
                "team_id": team_id,
                "matches": total_matches,
                "wins": total_wins,
                "draws": total_draws,
                "losses": total_losses,
                "win_rate": total_wins / total_matches,
                "goals_scored": goals_scored,
                "goals_conceded": goals_conceded,
                "goals_per_game": goals_scored / total_matches,
                "goals_conceded_per_game": goals_conceded / total_matches,
                "goal_diff": goals_scored - goals_conceded
            })
        
        return pd.DataFrame(stats)


if __name__ == "__main__":
    # Example usage
    import sys
    sys.path.append("../..")
    from src.data.collector import FootballDataCollector
    
    collector = FootballDataCollector()
    df = collector.load_data()
    
    if not df.empty:
        preprocessor = DataPreprocessor()
        
        # Clean data
        df_clean = preprocessor.clean_data(df)
        print(f"Cleaned data: {len(df_clean)} matches")
        
        # Encode targets
        df_encoded = preprocessor.encode_results(df_clean)
        print("\nTarget distribution:")
        print(f"Home wins: {df_encoded['home_win'].sum()}")
        print(f"Draws: {df_encoded['draw'].sum()}")
        print(f"Away wins: {df_encoded['away_win'].sum()}")
        
        # Get team stats
        team_stats = preprocessor.get_team_stats(df_clean)
        print("\nTop 5 teams by win rate:")
        print(team_stats.nlargest(5, "win_rate")[["team", "matches", "win_rate", "goals_per_game"]])
