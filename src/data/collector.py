"""
Data Collection Module
Fetches historical and live football match data from APIs
"""
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import json
import config


class FootballDataCollector:
    """Collects football data from Football-Data.org API"""
    
    def __init__(self, api_key=None):
        self.api_key = api_key or config.FOOTBALL_DATA_API_KEY
        if not self.api_key or self.api_key == "your_api_key_here":
            raise ValueError(
                "API key not configured. Please:\n"
                "1. Get a free API key from https://www.football-data.org/client/register\n"
                "2. Copy .env.example to .env\n"
                "3. Add your API key to the .env file"
            )
        
        self.base_url = config.FOOTBALL_DATA_BASE_URL
        self.headers = {"X-Auth-Token": self.api_key}
        self.data_dir = config.DATA_DIR
        
    def _make_request(self, endpoint, params=None):
        """Make API request with rate limiting"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            # Rate limiting: Free tier allows 10 requests per minute
            if response.status_code == 429:
                print("Rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None
    
    def collect_league_matches(self, league_code, season_start_year=None):
        """
        Collect all matches for a specific league and season
        
        Args:
            league_code: League code (e.g., 'PL' for Premier League)
            season_start_year: Season start year (e.g., 2023 for 2023/24 season)
        
        Returns:
            DataFrame with match data
        """
        if season_start_year:
            # Format: 2023 for 2023/24 season
            endpoint = f"competitions/{league_code}/matches"
            params = {"season": season_start_year}
        else:
            # Get current season
            endpoint = f"competitions/{league_code}/matches"
            params = {}
        
        print(f"Fetching {league_code} matches for season {season_start_year or 'current'}...")
        data = self._make_request(endpoint, params)
        
        if not data or "matches" not in data:
            print(f"No data returned for {league_code}")
            return pd.DataFrame()
        
        matches = []
        for match in data["matches"]:
            # Only include finished matches
            if match["status"] not in ["FINISHED"]:
                continue
            
            match_data = {
                "match_id": match["id"],
                "date": match["utcDate"],
                "matchday": match.get("matchday"),
                "home_team": match["homeTeam"]["name"],
                "away_team": match["awayTeam"]["name"],
                "home_team_id": match["homeTeam"]["id"],
                "away_team_id": match["awayTeam"]["id"],
                "home_score": match["score"]["fullTime"]["home"],
                "away_score": match["score"]["fullTime"]["away"],
                "half_time_home": match["score"]["halfTime"]["home"],
                "half_time_away": match["score"]["halfTime"]["away"],
                "competition": match["competition"]["name"],
                "competition_code": match["competition"]["code"],
                "season": match["season"]["startDate"][:4]
            }
            matches.append(match_data)
        
        df = pd.DataFrame(matches)
        
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            
            # Add result labels
            df["result"] = df.apply(
                lambda row: "H" if row["home_score"] > row["away_score"]
                else "A" if row["away_score"] > row["home_score"]
                else "D",
                axis=1
            )
            
            # Add goal difference
            df["goal_diff"] = df["home_score"] - df["away_score"]
            df["total_goals"] = df["home_score"] + df["away_score"]
        
        return df
    
    def collect_multiple_seasons(self, league_codes, seasons):
        """
        Collect data for multiple leagues and seasons
        
        Args:
            league_codes: List of league codes (e.g., ['PL', 'PD'])
            seasons: List of season start years (e.g., [2020, 2021, 2022])
        
        Returns:
            Combined DataFrame
        """
        all_matches = []
        
        for league_code in league_codes:
            for season in seasons:
                df = self.collect_league_matches(league_code, season)
                if not df.empty:
                    all_matches.append(df)
                # Be nice to the API
                time.sleep(6)  # 10 requests per minute = 6 seconds between requests
        
        if all_matches:
            combined_df = pd.concat(all_matches, ignore_index=True)
            combined_df = combined_df.sort_values("date").reset_index(drop=True)
            return combined_df
        
        return pd.DataFrame()
    
    def save_data(self, df, filename="football_matches.csv"):
        """Save collected data to CSV"""
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} matches to {filepath}")
        return filepath
    
    def load_data(self, filename="football_matches.csv"):
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
    
    def get_upcoming_matches(self, league_code, days_ahead=7):
        """Get upcoming matches for predictions"""
        endpoint = f"competitions/{league_code}/matches"
        
        date_from = datetime.now().strftime("%Y-%m-%d")
        date_to = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "status": "SCHEDULED"
        }
        
        print(f"Fetching upcoming {league_code} matches...")
        data = self._make_request(endpoint, params)
        
        if not data or "matches" not in data:
            return pd.DataFrame()
        
        matches = []
        for match in data["matches"]:
            match_data = {
                "match_id": match["id"],
                "date": match["utcDate"],
                "matchday": match.get("matchday"),
                "home_team": match["homeTeam"]["name"],
                "away_team": match["awayTeam"]["name"],
                "home_team_id": match["homeTeam"]["id"],
                "away_team_id": match["awayTeam"]["id"],
                "competition": match["competition"]["name"],
                "competition_code": match["competition"]["code"]
            }
            matches.append(match_data)
        
        df = pd.DataFrame(matches)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        
        return df


if __name__ == "__main__":
    # Example usage
    collector = FootballDataCollector()
    
    # Collect Premier League data for last 3 seasons
    df = collector.collect_multiple_seasons(
        league_codes=["PL"],
        seasons=[2021, 2022, 2023]
    )
    
    print(f"\nCollected {len(df)} matches")
    print(df.head())
    
    # Save data
    collector.save_data(df)
