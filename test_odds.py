"""
Test script for odds fetching and value bet identification
"""
from src.data.odds_collector import OddsCollector
from src.prediction.odds_analyzer import OddsAnalyzer

# Test odds collector
print("="*80)
print("TESTING ODDS COLLECTOR")
print("="*80 + "\n")

collector = OddsCollector()
odds = collector.get_odds("PL")

if odds:
    print(f"\n✓ Successfully fetched odds for {len(odds)} matches!")
    collector.display_odds(odds)
    
    # Test value bet analysis
    print("\n" + "="*80)
    print("TESTING VALUE BET ANALYZER")
    print("="*80 + "\n")
    
    # Create example prediction
    if odds:
        first_match = list(odds.keys())[0]
        first_odds = odds[first_match]
        
        example_prediction = {
            "home_team": first_odds['home_team'],
            "away_team": first_odds['away_team'],
            "date": first_odds['commence_time'],
            "probabilities": {
                "home_win": 0.55,  # Example: Model thinks home team has 55% chance
                "draw": 0.25,
                "away_win": 0.20
            },
            "predicted_outcome": "Home Win",
            "confidence": 0.55
        }
        
        analyzer = OddsAnalyzer()
        value_bets = analyzer.find_value_bets([example_prediction], odds)
        
        if value_bets:
            print("✓ Value bet analysis working!")
            analyzer.display_value_bets(value_bets)
        else:
            print("⚠️  No value bets found (this is normal if model and bookmaker agree)")
else:
    print("\n⚠️  No odds fetched. Please check:")
    print("   1. ODDS_API_KEY is set in .env file")
    print("   2. Get free key from: https://the-odds-api.com/")
    print("   3. Check internet connection")
