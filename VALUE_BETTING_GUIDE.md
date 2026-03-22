# 📊 Value Betting Setup Guide

## Overview
This system identifies **value bets** by comparing ML predictions with bookmaker odds to find profitable betting opportunities.

---

## 🔑 Step 1: Get The Odds API Key (FREE)

**The Odds API** provides live bookmaker odds from 50+ bookmakers worldwide.

### Get Your Free API Key:
1. Visit: **https://the-odds-api.com/**
2. Click **"Get a Free API Key"**
3. Sign up with your email
4. Copy your API key

### Free Tier Includes:
- ✅ **500 requests per month**
- ✅ Live odds from 50+ bookmakers (Bet365, William Hill, Unibet, etc.)
- ✅ Premier League, La Liga, Serie A, Bundesliga, Ligue 1
- ✅ H2H (match winner), spreads, totals markets
- ✅ Best odds across bookmakers

---

## ⚙️ Step 2: Add API Key to .env

1. Open your `.env` file (or create from `.env.example`)
2. Add your Odds API key:

```env
# Odds API - Get free key from: https://the-odds-api.com/
ODDS_API_KEY=your_actual_api_key_here
```

3. Save the file

---

## 🚀 Step 3: Run Predictions with Odds

### Basic Usage:
```bash
python main.py --predict --odds --leagues PL --days 7
```

### What This Does:
1. ✓ Fetches upcoming Premier League matches
2. ✓ Makes ML predictions (home win, draw, away win probabilities)
3. ✓ Fetches live bookmaker odds from The Odds API
4. ✓ Calculates **expected value (EV)** for each betting opportunity
5. ✓ Identifies **value bets** (positive EV with edge over bookmaker)
6. ✓ Recommends **Kelly Criterion stakes** (optimal bet sizing)
7. ✓ Saves results to CSV file

### Multiple Leagues:
```bash
python main.py --predict --odds --leagues PL,LaLiga,SerieA --days 3
```

### Supported Leagues:
- `PL` - Premier League (England)
- `PD` - La Liga (Spain)
- `SA` - Serie A (Italy)
- `BL1` - Bundesliga (Germany)
- `FL1` - Ligue 1 (France)

---

## 📈 Understanding Value Bets

### What is a Value Bet?
A value bet occurs when your model estimates a **higher probability** than the bookmaker's implied odds.

**Example:**
- **Your Model**: Man City has 65% chance to win
- **Bookmaker Odds**: 2.00 (implies 50% chance)
- **Edge**: 15% advantage (65% - 50%)
- **Expected Value**: +0.30 (30% profit per $1 bet)

### Value Bet Output:
```
1. Brighton vs Bournemouth
   Bet: Away Win
   Bookmaker Odds: 2.85 (implied prob: 35.1%)
   Model Probability: 59.9%
   Edge: 24.8%
   Expected Value: +1.109
   Recommended Stake: 18.4% of bankroll
   Confidence: 59.9%
```

### Key Metrics:
- **Edge**: Your advantage over bookmaker (higher = better)
- **Expected Value (EV)**: Profit per $1 bet (positive = profitable)
- **Kelly Stake**: Optimal bet size (% of your bankroll)
- **Confidence**: Model's certainty in prediction

---

## 🎯 Betting Strategy

### Conservative Approach (Recommended):
```python
# In config.py
MIN_CONFIDENCE = 0.58          # Only bet on 58%+ predictions
VALUE_BET_THRESHOLD = 0.05     # Require 5%+ edge
```

### Bankroll Management:
- Use **Quarter Kelly** (0.25x) for safety (default)
- Never bet more than 5% on a single match
- Track all bets in a spreadsheet
- Review performance monthly

### Example Bankroll ($1,000):
```
Value Bet: Away Win @ 2.85 odds
Kelly Stake: 18.4% = $184

Conservative Stake (5% cap): $50
Expected Return: $50 × 2.85 × 59.9% = $85.36
Expected Profit: $35.36
```

---

## 🔍 Supported Bookmakers

The Odds API aggregates odds from **50+ bookmakers**, including:

### UK Bookmakers:
- Bet365
- William Hill
- Ladbrokes
- Coral
- Betfair
- Unibet
- Skybet

### European:
- Bwin
- 1xBet
- Betclic
- Betsson

### Best Odds Selection:
The system automatically picks the **highest odds** across all bookmakers for each outcome, maximizing your potential returns.

---

## 📊 Output Files

Value bets are saved automatically:
```
data/value_bets_20260119_235959.csv
```

CSV includes:
- Match details
- Predicted outcome
- Model probability vs bookmaker probability
- Edge and expected value
- Recommended stake
- Timestamp

---

## 🛡️ Risk Management Tips

1. **Start Small**: Test with small stakes initially
2. **Track Everything**: Record all predictions and bets
3. **Diversify**: Don't put all money on one match
4. **Be Patient**: Value betting is long-term strategy
5. **Review Model**: Retrain with fresh data regularly
6. **Set Limits**: Daily/weekly betting limits
7. **Avoid Chasing**: Don't increase stakes after losses

---

## 🔧 Advanced Configuration

### Adjust Value Bet Threshold:
```python
# config.py
VALUE_BET_THRESHOLD = 0.10  # Require 10% edge (more selective)
```

### Change Kelly Fraction:
```python
# src/data/odds_collector.py
kelly_stake = self.calculate_kelly_stake(
    model_prob, 
    bookmaker_odds,
    kelly_fraction=0.125  # Eighth Kelly (very conservative)
)
```

### Filter Specific Bookmakers:
```python
odds = collector.get_odds(
    league="PL",
    bookmakers="bet365,williamhill"  # Only these bookmakers
)
```

---

## 📞 API Usage Monitoring

The system shows remaining API requests:
```
✓ Odds fetched successfully (Requests remaining: 487)
```

### Usage Tips:
- **500 requests/month** = ~16 per day
- Fetch odds for **multiple leagues at once** to save requests
- Run predictions **2-3 times per week** (before match days)
- Upgrade to paid plan ($10/month) for unlimited requests if needed

---

## 🐛 Troubleshooting

### "ODDS_API_KEY not found"
- Check `.env` file exists in project root
- Verify `ODDS_API_KEY=your_key` is set correctly
- Restart terminal/venv after adding key

### "No value bets found"
- Model may not have sufficient edge over bookmakers
- Lower `VALUE_BET_THRESHOLD` in config.py
- Check `MIN_CONFIDENCE` isn't too high
- Bookmaker odds may be very accurate (competitive market)

### "API rate limit exceeded"
- Wait until next month for reset
- Upgrade to paid plan ($10/month)
- Reduce prediction frequency

---

## 💡 Best Practices

1. **Pre-Match Only**: Bet at least 2-4 hours before kickoff for stable odds
2. **Compare Odds**: Manually verify odds on bookmaker site before placing bet
3. **Account for Margin**: Bookmakers take 2-5% margin (overround)
4. **Track ROI**: Aim for 5-10% ROI over 100+ bets
5. **Specialize**: Focus on 1-2 leagues you know well
6. **Trust the Model**: Don't override predictions with emotions

---

## 📚 Resources

- **The Odds API Docs**: https://the-odds-api.com/liveapi/guides/v4/
- **Kelly Criterion**: https://en.wikipedia.org/wiki/Kelly_criterion
- **Value Betting**: https://www.pinnacle.com/en/betting-articles/betting-strategy/what-is-value-betting

---

## ⚠️ Disclaimer

**This system is for educational purposes only.**

- Betting involves risk - never bet more than you can afford to lose
- Past performance does not guarantee future results
- Check local gambling laws and regulations
- Gamble responsibly - seek help if needed: https://www.begambleaware.org/

---

## 🎉 Quick Start Example

```bash
# 1. Get API key from https://the-odds-api.com/
# 2. Add to .env file:
echo "ODDS_API_KEY=your_api_key_here" >> .env

# 3. Run predictions with odds:
python main.py --predict --odds --leagues PL --days 3

# 4. Review value_bets_*.csv in data/ folder
# 5. Place bets at recommended stakes
# 6. Track results!
```

**Good luck! 🍀**
