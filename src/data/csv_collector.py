"""
CSV Data Collector
Downloads historical football data from football-data.co.uk (FREE)
Provides years of historical match data with results and statistics
"""
import requests
import pandas as pd
from pathlib import Path
import config
import time


class FootballDataCSVCollector:
    """Collects historical football data from football-data.co.uk CSV files"""
    
    def __init__(self):
        self.base_url = "https://www.football-data.co.uk"
        self.data_dir = config.DATA_DIR
        
        # Available leagues and their codes
        self.leagues = {
            "E0": {"name": "Premier League", "country": "England", "path": "englandm.php"},
            "E1": {"name": "Championship", "country": "England", "path": "englandm.php"},
            "SP1": {"name": "La Liga", "country": "Spain", "path": "spainm.php"},
            "I1": {"name": "Serie A", "country": "Italy", "path": "italym.php"},
            "D1": {"name": "Bundesliga", "country": "Germany", "path": "germanym.php"},
            "F1": {"name": "Ligue 1", "country": "France", "path": "francem.php"},
        }
    
    def download_season_data(self, league_code, season):
        """
        Download data for a specific league and season
        
        Args:
            league_code: League code (e.g., 'E0' for Premier League)
            season: Season in format 'YYZZ' (e.g., '2122' for 2021/22)
        
        Returns:
            DataFrame with match data
        """
        # Construct URL
        # Format: https://www.football-data.co.uk/mmz4281/2122/E0.csv
        url = f"{self.base_url}/mmz4281/{season}/{league_code}.csv"
        
        print(f"Downloading {self.leagues[league_code]['name']} {season[:2]}/{season[2:]}...")
        
        try:
            # Download CSV
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse CSV
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Standardize column names
            df = self._standardize_columns(df, league_code, season)
            
            print(f"  ✓ Downloaded {len(df)} matches")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Failed to download: {e}")
            return pd.DataFrame()
    
    def _standardize_columns(self, df, league_code, season):
        """Standardize column names to match our format"""
        
        # Convert season to year (e.g., '2122' -> '2021')
        season_year = f"20{season[:2]}"
        
        # Map football-data.co.uk columns to our format
        column_mapping = {
            "Date": "date",
            "HomeTeam": "home_team",
            "AwayTeam": "away_team",
            "FTHG": "home_score",  # Full Time Home Goals
            "FTAG": "away_score",  # Full Time Away Goals
            "FTR": "result",       # Full Time Result (H/D/A)
            "HTHG": "half_time_home",
            "HTAG": "half_time_away",
            "HS": "home_shots",
            "AS": "away_shots",
            "HST": "home_shots_target",
            "AST": "away_shots_target",
            "HC": "home_corners",
            "AC": "away_corners",
            "HF": "home_fouls",
            "AF": "away_fouls",
            "HY": "home_yellow_cards",
            "AY": "away_yellow_cards",
            "HR": "home_red_cards",
            "AR": "away_red_cards",
        }
        
        # Rename columns that exist
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        
        # Add metadata
        df["competition_code"] = league_code
        df["competition"] = self.leagues[league_code]["name"]
        df["season"] = season_year
        
        # Parse dates
        if "date" in df.columns:
            # Try different date formats
            for date_format in ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"]:
                try:
                    df["date"] = pd.to_datetime(df["date"], format=date_format)
                    break
                except:
                    continue
            
            # If still not parsed, try auto-parsing
            if not pd.api.types.is_datetime64_any_dtype(df["date"]):
                df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        
        # Add derived columns
        if "home_score" in df.columns and "away_score" in df.columns:
            df["goal_diff"] = df["home_score"] - df["away_score"]
            df["total_goals"] = df["home_score"] + df["away_score"]
        
        # Create unique match IDs
        df["match_id"] = df.apply(
            lambda row: f"{league_code}_{season}_{row.name}", axis=1
        )
        
        # Keep only relevant columns
        keep_columns = [
            "match_id", "date", "home_team", "away_team", 
            "home_score", "away_score", "result",
            "half_time_home", "half_time_away",
            "goal_diff", "total_goals",
            "competition", "competition_code", "season"
        ]
        
        # Add optional columns if they exist
        optional_columns = [
            "home_shots", "away_shots", "home_shots_target", "away_shots_target",
            "home_corners", "away_corners", "home_fouls", "away_fouls",
            "home_yellow_cards", "away_yellow_cards", "home_red_cards", "away_red_cards"
        ]
        
        for col in optional_columns:
            if col in df.columns:
                keep_columns.append(col)
        
        existing_keep_columns = [col for col in keep_columns if col in df.columns]
        df = df[existing_keep_columns]
        
        # Remove rows with missing essential data
        df = df.dropna(subset=["home_score", "away_score"])
        
        return df
    
    def collect_multiple_seasons(self, league_codes, start_season, end_season):
        """
        Collect data for multiple leagues and seasons
        
        Args:
            league_codes: List of league codes (e.g., ['E0', 'SP1'])
            start_season: Start season (e.g., 1718 for 2017/18)
            end_season: End season (e.g., 2324 for 2023/24)
        
        Returns:
            Combined DataFrame with all matches
        """
        all_data = []
        
        # Generate season codes
        start_year = int(str(start_season)[:2])
        end_year = int(str(end_season)[:2])
        
        seasons = []
        for year in range(start_year, end_year + 1):
            next_year = (year + 1) % 100
            season_code = f"{year:02d}{next_year:02d}"
            seasons.append(season_code)
        
        print(f"\nCollecting data for {len(seasons)} seasons from {len(league_codes)} leagues...")
        print(f"Seasons: {seasons[0][:2]}/{seasons[0][2:]} to {seasons[-1][:2]}/{seasons[-1][2:]}\n")
        
        total_matches = 0
        for league_code in league_codes:
            print(f"\n{self.leagues[league_code]['name']}:")
            
            for season in seasons:
                df = self.download_season_data(league_code, season)
                
                if not df.empty:
                    all_data.append(df)
                    total_matches += len(df)
                
                # Be nice to the server
                time.sleep(0.5)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values("date").reset_index(drop=True)
            
            print(f"\n{'='*60}")
            print(f"Total matches collected: {total_matches}")
            print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
            print(f"{'='*60}\n")
            
            return combined_df
        
        return pd.DataFrame()
    
    def save_data(self, df, filename="football_matches_historical.csv"):
        """Save collected data to CSV"""
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} matches to {filepath}")
        return filepath
    
    def load_data(self, filename="football_matches_historical.csv"):
        """Load data from CSV"""
        filepath = self.data_dir / filename
        if filepath.exists():
            df = pd.read_csv(filepath)
            df["date"] = pd.to_datetime(df["date"])
            print(f"Loaded {len(df)} matches from {filepath}")
            return df
        else:
            print(f"File not found: {filepath}")
            return pd.DataFrame()


if __name__ == "__main__":
    # Example usage
    collector = FootballDataCSVCollector()
    
    # Collect Premier League data for last 5 seasons
    df = collector.collect_multiple_seasons(
        league_codes=["E0"],  # Premier League
        start_season=1819,     # 2018/19
        end_season=2324        # 2023/24
    )
    
    print(f"\nCollected {len(df)} matches")
    print(f"\nSample data:")
    print(df.head())
    
    # Save data
    if not df.empty:
        collector.save_data(df)
