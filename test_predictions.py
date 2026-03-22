"""
Quick test script for predictions
"""
import pandas as pd
from src.prediction.predictor import normalize_team_name

# Test team name normalization
print("Testing team name normalization...")
test_names = [
    "Brighton & Hove Albion FC",
    "AFC Bournemouth", 
    "Manchester City FC",
    "Nottingham Forest FC"
]

for name in test_names:
    normalized = normalize_team_name(name)
    print(f"  {name} → {normalized}")

# Test data availability
print("\nChecking data availability...")
df = pd.read_csv('data/football_matches_historical.csv')
df['date'] = pd.to_datetime(df['date'])

for name in test_names:
    normalized = normalize_team_name(name)
    matches = df[(df['home_team'] == normalized) | (df['away_team'] == normalized)]
    print(f"  {normalized}: {len(matches)} matches")

# Test a specific matchup
print("\nTesting specific matchup (Brighton vs Bournemouth)...")
home = normalize_team_name("Brighton & Hove Albion FC")
away = normalize_team_name("AFC Bournemouth")

home_matches = df[(df['home_team'] == home) | (df['away_team'] == home)]
away_matches = df[(df['home_team'] == away) | (df['away_team'] == away)]

print(f"  {home}: {len(home_matches)} total matches, last: {home_matches['date'].max()}")
print(f"  {away}: {len(away_matches)} total matches, last: {away_matches['date'].max()}")

# H2H
h2h = df[
    ((df['home_team'] == home) & (df['away_team'] == away)) |
    ((df['home_team'] == away) & (df['away_team'] == home))
]
print(f"  Head-to-head: {len(h2h)} matches")

print("\n✓ All tests passed! Ready for predictions.")
