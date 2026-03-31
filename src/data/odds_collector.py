"""
Odds Collector
Fetches live bookmaker odds from API-Football (v3)
Unified data source to replace external odds APIs.
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import config

class OddsCollector:
    """Fetches betting odds from API-Football"""
    
    def __init__(self, api_key=None):
        # 统一使用配置好的 API-Football 密钥
        self.api_key = api_key or config.API_FOOTBALL_KEY
        if not self.api_key or self.api_key == "your_api_key_here":
            raise ValueError("API_FOOTBALL_KEY not found in config or .env")
            
        self.base_url = config.API_FOOTBALL_BASE_URL
        self.headers = {
            "x-apisports-key": self.api_key
        }
        
    def _make_request(self, endpoint, params=None):
        """发送 API 请求并处理基础错误"""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            # API-Football 频率限制处理 (免费/基础版通常有每秒/每分钟请求限制)
            if response.status_code == 429:
                print("Rate limit reached. Waiting 2 seconds...")
                time.sleep(2)
                return self._make_request(endpoint, params)
                
            response.raise_for_status()
            data = response.json()
            
            if data.get("errors") and len(data["errors"]) > 0:
                print(f"API Error ({endpoint}): {data['errors']}")
                return None
                
            return data
        except Exception as e:
            print(f"⚠️ Error fetching from {endpoint}: {e}")
            return None

    def get_odds(self, league="PL", markets="h2h", regions="uk", bookmakers=None):
        """
        获取特定联赛的赔率。
        注：这里的参数 (markets, regions, bookmakers) 是为了兼容原系统 main.py 的调用格式而保留的，
        但内部逻辑已完全替换为 API-Football 的处理方式。
        """
        league_id = config.API_FOOTBALL_LEAGUES.get(league, {}).get("id")
        if not league_id:
            print(f"⚠️ League {league} not supported for odds mapping")
            return {}
            
        # 确定当前赛季年份 (通常 7/8 月后算新赛季)
        current_month = datetime.now().month
        season = datetime.now().year if current_month >= 7 else datetime.now().year - 1
        
        # 获取未来 7 天的比赛
        days_ahead = 7
        date_from = datetime.now().strftime("%Y-%m-%d")
        date_to = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        print(f"Fetching fixtures and odds for {league} (API-Football)...")
        
        # 步骤 1：先获取赛程，建立 Fixture ID -> (主队, 客队) 的映射
        # 原因是 API-Football 的 odds 接口本身不返回球队名字，只返回比赛 ID
        fixtures_params = {
            "league": league_id,
            "season": season,
            "from": date_from,
            "to": date_to
        }
        fixtures_data = self._make_request("fixtures", fixtures_params)
        
        fixture_map = {}
        if fixtures_data and "response" in fixtures_data:
            for item in fixtures_data["response"]:
                fix_id = item["fixture"]["id"]
                home = item["teams"]["home"]["name"]
                away = item["teams"]["away"]["name"]
                fixture_map[fix_id] = (home, away)
                
        if not fixture_map:
            print("No upcoming fixtures found to map odds.")
            return {}

        # 步骤 2：针对未来每一天，获取当天的赔率数据
        odds_dict = {}
        for i in range(days_ahead + 1):
            target_date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
            odds_params = {
                "league": league_id,
                "season": season,
                "date": target_date
            }
            odds_data = self._make_request("odds", odds_params)
            
            if odds_data and "response" in odds_data:
                self._parse_odds_response(odds_data["response"], odds_dict, fixture_map)
                
            time.sleep(0.5) # 稍微延迟避免触发 API 并发限制
            
        print(f"✓ Odds fetched successfully ({len(odds_dict)} matches)")
        return odds_dict
        
    def _parse_odds_response(self, response_data, odds_dict, fixture_map):
        """解析 API-Football 赔率数据，并转换为系统兼容的格式"""
        for item in response_data:
            fix_id = item["fixture"]["id"]
            
            # 如果这场比赛不在我们刚刚获取的近期赛程中，则跳过
            if fix_id not in fixture_map:
                continue
                
            home_team, away_team = fixture_map[fix_id]
            match_key = f"{home_team} vs {away_team}"
            
            bookmakers_list = item.get("bookmakers", [])
            if not bookmakers_list:
                continue
                
            home_odds_list = []
            draw_odds_list = []
            away_odds_list = []
            
            # 遍历该场比赛所有博彩公司，提取 "Match Winner" (胜平负，ID为1) 玩法的赔率
            for bookmaker in bookmakers_list:
                for bet in bookmaker.get("bets", []):
                    if bet["id"] == 1 or bet["name"] == "Match Winner":
                        for value in bet["values"]:
                            val_name = str(value["value"])
                            try:
                                odd_val = float(value["odd"])
                                if val_name == "Home":
                                    home_odds_list.append(odd_val)
                                elif val_name == "Draw":
                                    draw_odds_list.append(odd_val)
                                elif val_name == "Away":
                                    away_odds_list.append(odd_val)
                            except ValueError:
                                continue
                                
            # 取各大博彩公司开出的最高赔率 (模拟最优赔率对冲)
            if home_odds_list and away_odds_list:
                odds_dict[match_key] = {
                    "home": max(home_odds_list),
                    "draw": max(draw_odds_list) if draw_odds_list else None,
                    "away": max(away_odds_list),
                    "home_team": home_team,
                    "away_team": away_team,
                    "commence_time": item["fixture"]["date"] # 保持和旧系统一样的字段名
                }
                
    def get_multiple_leagues(self, leagues=None):
        """获取多个联赛的赔率"""
        if leagues is None:
            leagues = list(config.API_FOOTBALL_LEAGUES.keys())
            
        all_odds = {}
        for league in leagues:
            league_odds = self.get_odds(league)
            all_odds.update(league_odds)
            
        return all_odds
        
    def display_odds(self, odds_dict):
        """在终端格式化打印赔率"""
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

if __name__ == "__main__":
    # 简单的本地测试入口
    collector = OddsCollector()
    odds = collector.get_odds("PL")
    collector.display_odds(odds)
