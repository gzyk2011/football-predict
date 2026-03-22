"""
Odds Collector
Fetches live bookmaker odds from The Odds API
API Documentation: https://the-odds-api.com/
Free tier: 500 requests/month
"""
import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


class OddsCollector:
    """Fetches betting odds from The Odds API"""
    
    def __init__(self, api_key=None):
        """
        Args:
            api_key: The Odds API key (get free key from https://the-odds-api.com/)
        """
        self.api_key = api_key or os.getenv("ODDS_API_KEY", "")
        self.base_url = "https://api.the-odds-api.com/v4"
        
        # Sport keys for football leagues
        self.sport_keys = {
            "PL": "soccer_epl",              # Premier League
            "PD": "soccer_spain_la_liga",    # La Liga
            "SA": "soccer_italy_serie_a",    # Serie A
            "BL1": "soccer_germany_bundesliga",  # Bundesliga
            "FL1": "soccer_france_ligue_one"     # Ligue 1
        }
    
    def get_odds(self, league="PL", markets="h2h", regions="uk", bookmakers=None):
        """
        Fetch odds for a specific league
        
        Args:
            league: League code (PL, PD, SA, BL1, FL1)
            markets: Comma-separated markets (h2h, spreads, totals)
            regions: Comma-separated regions (uk, us, eu, au)
            bookmakers: Specific bookmakers (optional)
        
        Returns:
            Dictionary with match odds
        """
        if not self.api_key:
            print("⚠️  ODDS_API_KEY not found in .env file")
            print("   Get your free API key from: https://the-odds-api.com/")
            return {}
        
        sport_key = self.sport_keys.get(league)
        if not sport_key:
            print(f"⚠️  League {league} not supported for odds")
            return {}
        
        url = f"{self.base_url}/sports/{sport_key}/odds"
        
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": "decimal",
            "dateFormat": "iso"
        }
        
        if bookmakers:
            params["bookmakers"] = bookmakers
        
        try:
            print(f"Fetching odds for {league} from The Odds API...")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            # Check remaining requests
            remaining = response.headers.get('x-requests-remaining', 'Unknown')
            print(f"✓ Odds fetched successfully (Requests remaining: {remaining})")
            
            data = response.json()
            return self._parse_odds_response(data)
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                print("⚠️  Invalid API key. Please check your ODDS_API_KEY in .env")
            elif response.status_code == 429:
                print("⚠️  API rate limit exceeded. Wait before making more requests.")
            else:
                print(f"⚠️  HTTP Error: {e}")
            return {}
        except Exception as e:
            print(f"⚠️  Error fetching odds: {e}")
            return {}
    
    def _parse_odds_response(self, data):
        """
        Parse API response into usable format
        
        Returns:
            Dictionary: {match_key: {"home": odds, "draw": odds, "away": odds}}
        """
        odds_dict = {}
        
        for match in data:
            home_team = match.get("home_team", "")
            away_team = match.get("away_team", "")
            match_key = f"{home_team} vs {away_team}"
            
            # Get best odds across bookmakers
            bookmakers = match.get("bookmakers", [])
            if not bookmakers:
                continue
            
            # Average odds across all bookmakers (or pick best)
            home_odds_list = []
            draw_odds_list = []
            away_odds_list = []
            
            for bookmaker in bookmakers:
                markets = bookmaker.get("markets", [])
                for market in markets:
                    if market.get("key") == "h2h":
                        outcomes = market.get("outcomes", [])
                        for outcome in outcomes:
                            name = outcome.get("name", "")
                            price = outcome.get("price", 0)
                            
                            if name == home_team:
                                home_odds_list.append(price)
                            elif name == away_team:
                                away_odds_list.append(price)
                            elif name == "Draw":
                                draw_odds_list.append(price)
            
            # Use best (highest) odds for betting
            if home_odds_list and away_odds_list:
                odds_dict[match_key] = {
                    "home": max(home_odds_list),
                    "draw": max(draw_odds_list) if draw_odds_list else None,
                    "away": max(away_odds_list),
                    "home_team": home_team,
                    "away_team": away_team,
                    "commence_time": match.get("commence_time", "")
                }
        
        return odds_dict
    
    def get_multiple_leagues(self, leagues=None):
        """
        Fetch odds for multiple leagues
        
        Args:
            leagues: List of league codes (default: all supported leagues)
        
        Returns:
            Combined odds dictionary
        """
        if leagues is None:
            leagues = list(self.sport_keys.keys())
        
        all_odds = {}
        for league in leagues:
            league_odds = self.get_odds(league)
            all_odds.update(league_odds)
        
        return all_odds
    
    def display_odds(self, odds_dict):
        """Display fetched odds in readable format"""
        if not odds_dict:
            print("No odds data available")
            return
        
        print(f"\n{'='*80}")
        print(f"BOOKMAKER ODDS ({len(odds_dict)} matches)")
        print(f"{'='*80}\n")
        
        for match, odds in odds_dict.items():
            print(f"{match}")
            print(f"  Home: {odds['home']:.2f}", end="")
            if odds['draw']:
                print(f" | Draw: {odds['draw']:.2f}", end="")
            print(f" | Away: {odds['away']:.2f}")
            print(f"  Time: {odds['commence_time']}")
            print()


# Example usage
if __name__ == "__main__":
    collector = OddsCollector()
    
    # Get Premier League odds
    odds = collector.get_odds("PL")
    collector.display_odds(odds)
    
    # Or get multiple leagues
    # all_odds = collector.get_multiple_leagues(["PL", "PD", "SA"])
    # collector.display_odds(all_odds)
