# Getting Started Guide

## Quick Start

### 1. Setup API Key

First, get a free API key from [Football-Data.org](https://www.football-data.org/client/register).

Copy the example environment file and add your API key:
```bash
cp .env.example .env
```

Edit `.env` and add your API key:
```
FOOTBALL_DATA_API_KEY=your_actual_api_key_here
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Collect Data

Collect historical match data (this may take a few minutes):

```bash
# Collect Premier League data for last 3 seasons
python main.py --collect --leagues PL --seasons 2021,2022,2023
```

Available league codes:
- `PL` - Premier League (England)
- `PD` - La Liga (Spain)
- `SA` - Serie A (Italy)
- `BL1` - Bundesliga (Germany)
- `FL1` - Ligue 1 (France)

### 4. Train Models

Train the machine learning models on the collected data:

```bash
python main.py --train
```

This will:
- Clean and preprocess the data
- Create advanced features (form, head-to-head, team strength)
- Train XGBoost and LightGBM models
- Create an ensemble model
- Evaluate performance on test data

### 5. Make Predictions

Predict outcomes for upcoming matches:

```bash
# Predict Premier League matches in the next 7 days
python main.py --predict --leagues PL --days 7
```

The system will show:
- Match predictions with probabilities
- Confidence levels
- High-confidence predictions

### 6. Backtest Performance

Evaluate how the model would have performed historically:

```bash
# Backtest on 2023 data
python main.py --backtest --start-date 2023-01-01 --end-date 2024-01-01
```

This will show:
- Prediction accuracy
- Performance by confidence level
- Simulated betting results

## Understanding the Output

### Prediction Format

```
Predicted: Arsenal vs Manchester City
  Home Win (confidence: 45%)
  Probabilities: H=45%, D=30%, A=25%
```

- **H (Home Win)**: Home team wins
- **D (Draw)**: Match ends in a draw
- **A (Away Win)**: Away team wins
- **Confidence**: How confident the model is (higher is better)

### Value Betting

A prediction is NOT the same as a good bet. You want to find **value bets** where:
- Your model's probability > Bookmaker's implied probability
- Example: Model says 60% chance of home win, bookmaker odds imply only 45%

The `odds_analyzer.py` module handles this calculation.

## Tips for Better Results

### 1. Use More Data
- Collect multiple seasons (at least 3)
- Include multiple leagues for better model training
- More data = better predictions

### 2. Focus on High Confidence
- Only bet on predictions with >55% confidence
- The model is better at identifying strong favorites vs underdogs
- Avoid betting on predicted draws (hardest to predict)

### 3. Track Performance
- Run backtests regularly
- Keep a log of your predictions vs actual results
- Adjust your strategy based on what works

### 4. Understand Limitations
- Football is inherently unpredictable
- Even 60% accuracy is good (random guessing = 33%)
- Injuries, form changes, and luck affect outcomes
- No model can predict red cards, referee decisions, etc.

### 5. Bankroll Management
- Never bet more than 1-5% of your bankroll on a single match
- Use Kelly Criterion for optimal stake sizing (built into `odds_analyzer.py`)
- Set a budget and stick to it

## Advanced Usage

### Collect Multiple Leagues

```bash
python main.py --collect --leagues PL,PD,SA,BL1,FL1 --seasons 2020,2021,2022,2023
```

### Custom Model Parameters

Edit `config.py` to adjust:
- Feature engineering parameters (form matches, H2H matches)
- Model hyperparameters
- Confidence thresholds
- Value bet thresholds

### Using the Odds Analyzer

```python
from src.prediction.odds_analyzer import OddsAnalyzer

analyzer = OddsAnalyzer(value_threshold=0.05)

# Your bookmaker odds
odds = {
    "Arsenal vs Man City": {
        "home": 2.10,
        "draw": 3.40,
        "away": 3.50
    }
}

# Find value bets
value_bets = analyzer.find_value_bets(predictions, odds)
analyzer.display_value_bets(value_bets)
```

## Troubleshooting

### "API key not configured"
Make sure you:
1. Created a `.env` file (copy from `.env.example`)
2. Added your actual API key
3. API key is valid (test at football-data.org)

### "Rate limit reached"
The free API tier allows 10 requests per minute. The system automatically waits, but if collecting lots of data, expect delays.

### "No data found"
Run `--collect` first before `--train` or `--predict`.

### Low Accuracy
- Collect more data (more seasons, more leagues)
- Check if test data is from recent seasons (recent form matters)
- Football is inherently random - 50-60% accuracy is realistic

## Next Steps

1. **Paper Trade**: Track predictions without real money for a month
2. **Compare Odds**: Find the best odds across multiple bookmakers
3. **Specialize**: Focus on specific leagues you know well
4. **Refine Features**: Add new features based on domain knowledge
5. **Track Everything**: Keep detailed records of all predictions and bets

## Responsible Gambling

⚠️ **Important Reminders**:
- Gambling should be entertainment, not income
- Never bet money you can't afford to lose
- Set strict limits and stick to them
- Take breaks if you're on a losing streak
- Seek help if gambling becomes a problem

## Support

For issues or questions:
1. Check the README.md for documentation
2. Review the code comments in each module
3. Test with small data sets first
4. Verify your API key and internet connection

Good luck and bet responsibly! 🍀
