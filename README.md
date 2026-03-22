# ⚽ Football Match Prediction System

A comprehensive machine learning system for predicting football match outcomes and identifying value bets with live bookmaker odds.

## 🌐 Live Predictions Page

**Visit daily predictions:** [https://thedatadoktor.github.io/prediict/](https://thedatadoktor.github.io/prediict/)

Updates automatically every day at 8:00 AM UTC with fresh predictions!

## ✨ Features

- **🤖 ML Predictions**: XGBoost & LightGBM ensemble (51.54% accuracy)
- **💰 Value Betting**: Live odds from 50+ bookmakers via The Odds API
- **📊 Smart Staking**: Kelly Criterion for optimal bet sizing
- **⚡ Fast Performance**: Predictions in seconds (pre-computed features)
- **🌐 GitHub Pages**: Beautiful daily predictions website
- **🔄 Auto-Updates**: GitHub Actions automation (hands-free!)
- **📈 Expected Value**: Clear profit estimates for each bet

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Get a free API key from [Football-Data.org](https://www.football-data.org/client/register)

4. Create `.env` file:
   ```bash
   cp .env.example .env
   ```
   Add your API key to `.env`

## Usage

### Collect Historical Data
```bash
python main.py --collect --leagues PL,PD,SA --seasons 2020,2021,2022,2023
```

### Train Models
```bash
python main.py --train
```

### Make Predictions
### Make Predictions with Value Bets
```bash
python main.py --predict --odds --leagues PL --days 7
```

### Generate GitHub Pages Site
```bash
python generate_html.py
```

### Backtest Performance
```bash
python main.py --backtest --start-date 2023-01-01 --end-date 2024-01-01
```

## 🤖 Automated Daily Updates

### Setup GitHub Actions (One-Time)

1. **Add API Keys to GitHub Secrets:**
   - Go to: `https://github.com/YOUR_USERNAME/prediict/settings/secrets/actions`
   - Click "New repository secret"
   - Add these secrets:
     ```
     ODDS_API_KEY = your_odds_api_key_here
     FOOTBALL_DATA_API_KEY = your_football_data_key_here
     ```

2. **Enable GitHub Pages:**
   - Go to: `https://github.com/YOUR_USERNAME/prediict/settings/pages`
   - Source: Branch `main`, Folder `/ (root)`
   - Click "Save"

3. **Done!** 
   - Predictions update automatically every day at 8:00 AM UTC
   - Site rebuilds automatically: `https://YOUR_USERNAME.github.io/prediict/`
   - Manual trigger: Go to "Actions" tab → "Daily Predictions Update" → "Run workflow"

### Manual Update
```bash
python main.py --predict --odds --leagues PL --days 7
python generate_html.py
git add index.html data/
git commit -m "Update predictions"
git push
```

## Project Structure

```
prediict/
├── src/
│   ├── data/
│   │   ├── collector.py       # Data collection from APIs
│   │   └── preprocessor.py    # Data cleaning and preparation
│   ├── features/
│   │   └── engineer.py        # Feature engineering
│   ├── models/
│   │   ├── base_model.py      # Base model class
│   │   ├── xgboost_model.py   # XGBoost implementation
│   │   ├── lightgbm_model.py  # LightGBM implementation
│   │   └── ensemble.py        # Model ensemble
│   ├── prediction/
│   │   ├── predictor.py       # Prediction system
│   │   └── odds_analyzer.py   # Value bet identification
│   └── evaluation/
│       └── backtester.py      # Backtesting framework
├── config.py                   # Configuration settings
├── main.py                     # Main execution script
└── requirements.txt
```

## Disclaimer

⚠️ **Important**: This system is for educational and research purposes. Betting involves risk and there are no guarantees of profit. Always gamble responsibly and never bet more than you can afford to lose.

## License

MIT License
