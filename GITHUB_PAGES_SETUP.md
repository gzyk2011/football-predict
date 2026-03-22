# 🚀 Enable GitHub Pages

Your prediction website is ready! Just need to enable GitHub Pages:

## Steps to Enable:

1. **Go to your repository:**
   - https://github.com/thedatadoktor/prediict

2. **Click "Settings" tab** (top right)

3. **Scroll down to "Pages"** (left sidebar)

4. **Under "Source":**
   - Branch: `main`
   - Folder: `/ (root)`
   - Click **"Save"**

5. **Wait 1-2 minutes** for deployment

6. **Your site will be live at:**
   ```
   https://thedatadoktor.github.io/prediict/
   ```

## 📅 Update Predictions Daily

Every day, run these commands:

```bash
# 1. Get new predictions with odds
python main.py --predict --odds --leagues PL --days 7

# 2. Generate HTML from latest predictions
python generate_html.py

# 3. Publish to GitHub Pages
git add index.html
git commit -m "Update predictions $(date +%Y-%m-%d)"
git push
```

**Site updates automatically in 1-2 minutes after push!**

## 🤖 Automate with GitHub Actions (Optional)

Create `.github/workflows/daily-predictions.yml`:

```yaml
name: Daily Predictions

on:
  schedule:
    - cron: '0 8 * * *'  # Run daily at 8 AM UTC
  workflow_dispatch:  # Allow manual trigger

jobs:
  update-predictions:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      
      - name: Run predictions
        env:
          ODDS_API_KEY: ${{ secrets.ODDS_API_KEY }}
          FOOTBALL_DATA_API_KEY: ${{ secrets.FOOTBALL_DATA_API_KEY }}
        run: |
          python main.py --predict --odds --leagues PL --days 7
          python generate_html.py
      
      - name: Commit and push
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add index.html data/
          git commit -m "Auto-update predictions $(date +%Y-%m-%d)" || exit 0
          git push
```

Add your API keys to GitHub Secrets (Settings → Secrets → Actions).

## 📱 Share Your Site

Once live, share:
- **https://thedatadoktor.github.io/prediict/**

Mobile-friendly, updates automatically, looks professional! 🎉
