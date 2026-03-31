"""
Data Collection Module
Fetches historical and live football match data from API-Football (v3)
"""
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import config

class FootballDataCollector:
    """Collects football data from API-Football"""
    
    def __init__(self, api_key=None):
        # 读取我们刚刚在 config.py 里配置好的新密钥
        self.api_key = api_key or config.API_FOOTBALL_KEY
        if not self.api_key or self.api_key == "your_api_key_here":
            raise ValueError(
                "API key not configured. Please check your .env file or GitHub Secrets."
            )
        
        self.base_url = config.API_FOOTBALL_BASE_URL
        # API-Football 要求的专属鉴权 Header
        self.headers = {
            "x-apisports-key": self.api_key,
        }
        self.data_dir = config.DATA_DIR
        
    def _make_request(self, endpoint, params=None):
        """发送 API 请求并处理基础错误"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            # API-Football 速率限制处理
            if response.status_code == 429:
                print("Rate limit reached. Waiting 2 seconds...")
                time.sleep(2)
                return self._make_request(endpoint, params)
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("errors") and len(data["errors"]) > 0:
                print(f"API Error: {data['errors']}")
                return None
                
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None
    
    def collect_league_matches(self, league_code, season_start_year=None):
        """收集特定联赛和赛季的历史完赛数据"""
        league_id = config.API_FOOTBALL_LEAGUES.get(league_code, {}).get("id")
        if not league_id:
            print(f"Unsupported league code: {league_code}")
            return pd.DataFrame()

        season = season_start_year if season_start_year else datetime.now().year
        endpoint = "fixtures"
        params = {
            "league": league_id,
            "season": season
        }
        
        print(f"Fetching {league_code} matches for season {season}...")
        data = self._make_request(endpoint, params)
        
        if not data or "response" not in data or len(data["response"]) == 0:
            print(f"No data returned for {league_code} season {season}")
            return pd.DataFrame()
        
        matches = []
        for item in data["response"]:
            fixture = item["fixture"]
            teams = item["teams"]
            goals = item["goals"]
            score = item["score"]
            league_info = item["league"]
            
            # 只收录已完赛的比赛
            if fixture["status"]["short"] not in ["FT", "AET", "PEN"]:
                continue
            
            match_data = {
                "match_id": fixture["id"],
                "date": fixture["date"],
                "matchday": league_info.get("round", ""),
                "home_team": teams["home"]["name"],
                "away_team": teams["away"]["name"],
                "home_team_id": teams["home"]["id"],
                "away_team_id": teams["away"]["id"],
                "home_score": goals["home"],
                "away_score": goals["away"],
                "half_time_home": score["halftime"]["home"] if score["halftime"]["home"] is not None else 0,
                "half_time_away": score["halftime"]["away"] if score["halftime"]["away"] is not None else 0,
                "competition": league_info["name"],
                "competition_code": league_code,
                "season": str(season)
            }
            matches.append(match_data)
        
        df = pd.DataFrame(matches)
        
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            
            # 计算比赛结果标签
            df["result"] = df.apply(
                lambda row: "H" if row["home_score"] > row["away_score"]
                else "A" if row["away_score"] > row["home_score"]
                else "D",
                axis=1
            )
            df["goal_diff"] = df["home_score"] - df["away_score"]
            df["total_goals"] = df["home_score"] + df["away_score"]
        
        return df
    
    def collect_multiple_seasons(self, league_codes, seasons):
        """收集多个联赛、多个赛季的数据"""
        all_matches = []
        for league_code in league_codes:
            for season in seasons:
                df = self.collect_league_matches(league_code, season)
                if not df.empty:
                    all_matches.append(df)
                time.sleep(1)  
        
        if all_matches:
            combined_df = pd.concat(all_matches, ignore_index=True)
            combined_df = combined_df.sort_values("date").reset_index(drop=True)
            return combined_df
        
        return pd.DataFrame()
    
    def save_data(self, df, filename="football_matches.csv"):
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} matches to {filepath}")
        return filepath
    
    def load_data(self, filename="football_matches.csv"):
        filepath = self.data_dir / filename
        if filepath.exists():
            df = pd.read_csv(filepath)
            df["date"] = pd.to_datetime(df["date"])
            print(f"Loaded {len(df)} matches from {filepath}")
            return df
        else:
            return pd.DataFrame()
    
    def get_upcoming_matches(self, league_code, days_ahead=7):
        """获取未来即将进行的比赛以供预测"""
        league_id = config.API_FOOTBALL_LEAGUES.get(league_code, {}).get("id")
        if not league_id:
            return pd.DataFrame()

        endpoint = "fixtures"
        
        date_from = datetime.now().strftime("%Y-%m-%d")
        date_to = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        season = datetime.now().year if datetime.now().month > 6 else datetime.now().year - 1
        
        params = {
            "league": league_id,
            "season": season,
            "from": date_from,
            "to": date_to
        }
        
        print(f"Fetching upcoming {league_code} matches from {date_from} to {date_to}...")
        data = self._make_request(endpoint, params)
        
        if not data or "response" not in data:
            return pd.DataFrame()
        
        matches = []
        for item in data["response"]:
            fixture = item["fixture"]
            teams = item["teams"]
            league_info = item["league"]
            
            # 只收录尚未开始的比赛
            if fixture["status"]["short"] not in ["NS", "TBD"]:
                continue
                
            match_data = {
                "match_id": fixture["id"],
                "date": fixture["date"],
                "matchday": league_info.get("round", ""),
                "home_team": teams["home"]["name"],
                "away_team": teams["away"]["name"],
                "home_team_id": teams["home"]["id"],
                "away_team_id": teams["away"]["id"],
                "competition": league_info["name"],
                "competition_code": league_code
            }
            matches.append(match_data)
        
        df = pd.DataFrame(matches)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        
        return df
