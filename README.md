# 🇮🇳 India EdTech Investor Finder

A free, automated weekly scraper that collects Indian startup funding data from public sources, filters for EdTech deals, and cross-references investors against the SEBI AIF registry to surface contact details — all saved to public CSVs. No paid subscriptions needed.

## What's tracked

### Full funding data (`data/india_funding_<year>.csv`)

| Column | Description |
|---|---|
| `startup_name` | Name of the startup |
| `domain` | Sector / industry (e.g. EdTech, FinTech, HealthTech) |
| `round` | Funding stage (Seed, Series A, Series B, Angel …) |
| `amount_usd` | Ticket size in USD (null if undisclosed) |
| `investor` | Investor name (one row per investor per deal) |
| `headquarters` | City / state where the startup is based |
| `date` | Date of funding announcement |
| `scraped_at` | Date this row was collected |

### EdTech investor contacts (`data/edtech_investors.csv`)

| Column | Description |
|---|---|
| `investor` | Investor name |
| `investor_email` | Contact email from SEBI AIF registry (if matched) |
| `sebi_reg_no` | SEBI AIF registration number (if matched) |
| `aif_category` | AIF category (Category I / II / III) |
| `investor_city` | City from SEBI registry |
| `startup_name` | Name of the EdTech startup funded |
| `domain` | EdTech sub-sector |
| `round` | Funding stage |
| `amount_usd` | Ticket size in USD |
| `headquarters` | Startup headquarters |
| `date` | Date of funding announcement |

## Data sources

| Source | URL | Method |
|---|---|---|
| StartupTalky 2026 | [link](https://startuptalky.com/indian-startups-funding-investors-data-2026/) | HTML table |
| StartupTalky 2025 | [link](https://startuptalky.com/indian-startups-funding-investors-data-2025/) | HTML table |
| StartupTalky 2024 | [link](https://startuptalky.com/indian-startups-funding-investor-data-2024/) | HTML table |
| SEBI AIF Registry | [link](https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRecognisedFpi=yes&intmId=16) | HTML scrape |

## How it works

1. **Every Monday at 6:00 AM IST** a GitHub Actions workflow runs `scraper/run.py`
2. StartupTalky tables (2024, 2025, 2026) are scraped and parsed into structured rows
3. Rows matching EdTech keywords are filtered into a separate dataset
4. The SEBI AIF registry is scraped to collect fund names, registration numbers, categories, contact emails, and cities
5. EdTech investors are fuzzy-matched against SEBI fund names (≥ 50% token overlap) to enrich with contact details
6. All CSVs are deduplicated and committed back to the repo

## Output files

| File | Description |
|---|---|
| `data/india_funding_2024.csv` | All startup funding deals scraped for 2024 |
| `data/india_funding_2025.csv` | All startup funding deals scraped for 2025 |
| `data/india_funding_2026.csv` | All startup funding deals scraped for 2026 |
| `data/edtech_investors.csv` | EdTech-only investors enriched with SEBI AIF contact data |

## Setup (for your own fork)

### 1. Fork this repo

### 2. Enable GitHub Actions
Go to the `Actions` tab and click **Enable workflows**

### 3. Run manually to test
Go to `Actions → Weekly India Funding Scraper → Run workflow`

No API keys or secrets required — all data sources are publicly accessible.

## Run locally

```bash
git clone https://github.com/YOUR_USERNAME/InvestorFinder
cd InvestorFinder
pip install -r requirements.txt
python scraper/run.py
```

## Cost

| Component | Cost |
|---|---|
| GitHub Actions | Free (2000 min/month) |
| Everything else | ₹0 |

## CSV download

The latest data is always available in the `data/` folder. Direct raw links:

```
https://raw.githubusercontent.com/YOUR_USERNAME/InvestorFinder/main/data/india_funding_2026.csv
https://raw.githubusercontent.com/YOUR_USERNAME/InvestorFinder/main/data/edtech_investors.csv
```

## License

MIT — free to use, fork, and build on.
