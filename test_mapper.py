import pandas as pd
from src.prediction.predictor import normalize_team_name

df = pd.read_csv('data/football_matches_historical.csv')

api_home = 'Brighton & Hove Albion FC'
api_away = 'AFC Bournemouth'

norm_home = normalize_team_name(api_home)
norm_away = normalize_team_name(api_away)

print(f'API: {api_home} -> Normalized: {norm_home}')
print(f'API: {api_away} -> Normalized: {norm_away}')
print(f'Brighton in CSV: {len(df[df["home_team"] == norm_home])} home matches')
print(f'Bournemouth in CSV: {len(df[df["away_team"] == norm_away])} away matches')
print(f'Total Brighton matches: {len(df[(df["home_team"] == norm_home) | (df["away_team"] == norm_home)])}')
print(f'Total Bournemouth matches: {len(df[(df["home_team"] == norm_away) | (df["away_team"] == norm_away)])}')
