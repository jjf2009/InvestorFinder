# 🇮🇳 India Startup Funding Tracker

A free, automated weekly scraper that collects Indian startup funding data — investor names, domains, funding rounds, and ticket sizes — and saves everything to a public CSV. No paid subscriptions needed.

## What's tracked

| Column | Description |
|---|---|
| `startup_name` | Name of the startup |
| `domain` | Sector / industry (e.g. EdTech, FinTech, HealthTech) |
| `round` | Funding stage (Seed, Series A, Series B, Angel …) |
| `amount_usd` | Ticket size in USD (null if undisclosed) |
| `investor` | Investor name (one row per investor per deal) |
| `date` | Date of funding announcement |
| `source` | Which site the data came from |
| `scraped_at` | Date this row was collected |

## Data sources

| Source | URL | Method |
|---|---|---|
| StartupTalky 2026 | [link](https://startuptalky.com/indian-startups-funding-investors-data-2026/) | HTML table |
| StartupTalky 2025 | [link](https://startuptalky.com/indian-startups-funding-investors-data-2025/) | HTML table |
| StartupTalky 2024 | [link](https://startuptalky.com/indian-startups-funding-investor-data-2024/) | HTML table |
| Inc42 weekly | [link](https://inc42.com/tag/funding-galore/) | Kimi AI extract |
| Entrackr news | [link](https://entrackr.com/news/) | Kimi AI extract |

## How it works

1. **Every Monday at 6:00 AM IST** a GitHub Actions workflow runs `scraper/run.py`
2. StartupTalky tables are parsed directly (no AI needed)
3. Inc42 and Entrackr articles are fetched and sent to [Kimi API](https://platform.moonshot.cn/) (moonshot-v1-8k) for structured extraction
4. New rows are appended to `data/india_funding.csv`, deduplicated, and committed back to the repo

## Setup (for your own fork)

### 1. Fork this repo

### 2. Add your Kimi API key as a secret
Go to `Settings → Secrets and variables → Actions → New repository secret`
- Name: `KIMI_API_KEY`
- Value: your key from [platform.moonshot.cn](https://platform.moonshot.cn/)

### 3. Enable GitHub Actions
Go to the `Actions` tab and click **Enable workflows**

### 4. Run manually to test
Go to `Actions → Weekly India Funding Scraper → Run workflow`

## Run locally

```bash
git clone https://github.com/YOUR_USERNAME/india-funding-tracker
cd india-funding-tracker
pip install -r requirements.txt
export KIMI_API_KEY=your_key_here
python scraper/run.py
```

## Cost

| Component | Cost |
|---|---|
| GitHub Actions | Free (2000 min/month) |
| Kimi API (~200 articles/week) | ~₹15–20/week |
| Everything else | ₹0 |

## CSV download

The latest data is always at:
```
data/india_funding.csv
```

Direct raw link (once hosted):
```
https://raw.githubusercontent.com/YOUR_USERNAME/india-funding-tracker/main/data/india_funding.csv
```

## License

MIT — free to use, fork, and build on.
