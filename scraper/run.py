"""
India Startup Funding Scraper
Sources: StartupTalky (2024/2025/2026), Inc42, Entrackr
Output:  data/india_funding.csv
"""

import os
import re
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from openai import OpenAI  # Kimi uses OpenAI-compatible SDK

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Kimi client (OpenAI-compatible) ──────────────────────────────────────────
KIMI_API_KEY = os.environ.get("KIMI_API_KEY", "")
kimi = OpenAI(
    api_key=KIMI_API_KEY,
    base_url="https://api.moonshot.cn/v1",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "india_funding.csv")

# ─── Kimi extraction ──────────────────────────────────────────────────────────

EXTRACT_PROMPT = """
You are extracting Indian startup funding deals from news article text.
Extract EVERY funding deal mentioned. Return ONLY a valid JSON array — no explanation, no markdown, no backticks.

For each deal extract:
- startup_name: company name (string)
- domain: the sector/industry as written (e.g. "EdTech", "FinTech", "HealthTech", "AgriTech")
- round: funding stage exactly as written (e.g. "Seed", "Pre-Seed", "Series A", "Angel", "Debt")
- amount_usd: amount in USD as a number, null if undisclosed or not mentioned
- investors: array of investor name strings (split comma-separated investors into individual strings)
- date: date string if mentioned (YYYY-MM-DD preferred), else null

Rules:
- If amount is in INR, convert to USD at 1 USD = 84 INR
- If amount says "undisclosed" set to null
- If multiple investors listed, split them into the array
- Return [] if no funding deals found

Return format (JSON array only):
[{"startup_name":"...","domain":"...","round":"...","amount_usd":null,"investors":["..."],"date":"..."}]

Article text:
{article_text}
"""

def kimi_extract(article_text: str) -> list[dict]:
    """Use Kimi to extract structured deals from unstructured article text."""
    if not KIMI_API_KEY:
        log.warning("No KIMI_API_KEY set — skipping AI extraction")
        return []
    try:
        resp = kimi.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[{
                "role": "user",
                "content": EXTRACT_PROMPT.format(article_text=article_text[:6000])
            }],
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip any accidental markdown fences
        raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("```").strip()
        return json.loads(raw)
    except Exception as e:
        log.error(f"Kimi extraction failed: {e}")
        return []


# ─── SCRAPER 1: StartupTalky (plain HTML tables) ──────────────────────────────

STARTUPTALKY_URLS = {
    "2024": "https://startuptalky.com/indian-startups-funding-investor-data-2024/",
    "2025": "https://startuptalky.com/indian-startups-funding-investors-data-2025/",
    "2026": "https://startuptalky.com/indian-startups-funding-investors-data-2026/",
}

def scrape_startuptalky() -> list[dict]:
    rows = []
    for year, url in STARTUPTALKY_URLS.items():
        log.info(f"StartupTalky {year}: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            tables = soup.find_all("table")
            log.info(f"  Found {len(tables)} tables")

            for table in tables:
                ths = [th.get_text(strip=True) for th in table.find_all("th")]
                # Normalise column names
                col_map = _map_startuptalky_cols(ths)
                if not col_map:
                    continue

                for tr in table.find_all("tr")[1:]:
                    tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if len(tds) < 3:
                        continue
                    row = _extract_startuptalky_row(tds, ths, col_map)
                    if row:
                        row["source"] = f"startuptalky_{year}"
                        # Explode multi-investor rows
                        for inv in _split_investors(row.get("investors_raw", "")):
                            rows.append({**row, "investor": inv})
        except Exception as e:
            log.error(f"StartupTalky {year} failed: {e}")
        time.sleep(2)
    return rows


def _map_startuptalky_cols(headers: list[str]) -> dict:
    """Return index map for known column names (case-insensitive)."""
    mapping = {}
    for i, h in enumerate(headers):
        h_lower = h.lower()
        if "company" in h_lower or "startup" in h_lower:
            mapping["startup_name"] = i
        elif "sector" in h_lower or "industry" in h_lower or "vertical" in h_lower:
            mapping["domain"] = i
        elif "amount" in h_lower or "raised" in h_lower or "funding" in h_lower and "round" not in h_lower:
            mapping["amount_usd"] = i
        elif "round" in h_lower or "stage" in h_lower or "type" in h_lower:
            mapping["round"] = i
        elif "investor" in h_lower or "lead" in h_lower:
            mapping["investors_raw"] = i
        elif "date" in h_lower:
            mapping["date"] = i
    return mapping if "startup_name" in mapping else {}


def _extract_startuptalky_row(tds, ths, col_map) -> dict | None:
    def g(key):
        idx = col_map.get(key)
        return tds[idx].strip() if idx is not None and idx < len(tds) else ""

    startup = g("startup_name")
    if not startup or startup.lower() in ("company", "startup", "—", "-", ""):
        return None

    raw_amount = g("amount_usd")
    amount = _parse_amount(raw_amount)

    return {
        "startup_name": startup,
        "domain": g("domain"),
        "round": g("round"),
        "amount_usd": amount,
        "investors_raw": g("investors_raw"),
        "date": g("date"),
    }


# ─── SCRAPER 2: Inc42 funding galore articles ─────────────────────────────────

INC42_TAG_URL = "https://inc42.com/tag/funding-galore/"

def scrape_inc42(max_pages: int = 3) -> list[dict]:
    """
    Inc42 is JS-heavy. We use requests + BeautifulSoup to pull article links
    from the tag page, then fetch each article and pass to Kimi.
    Note: If BeautifulSoup can't get article links (JS wall), the function
    logs a warning — install Playwright and switch to _scrape_inc42_playwright().
    """
    rows = []
    article_links = []

    for page in range(1, max_pages + 1):
        url = INC42_TAG_URL if page == 1 else f"{INC42_TAG_URL}page/{page}/"
        log.info(f"Inc42 tag page {page}: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")
            # Article cards usually carry class 'post-card' or similar
            links = [
                a["href"] for a in soup.find_all("a", href=True)
                if "funding-galore" in a["href"] and a["href"] not in article_links
            ]
            article_links.extend(links)
            log.info(f"  Found {len(links)} new article links")
        except Exception as e:
            log.error(f"Inc42 tag page {page} failed: {e}")
        time.sleep(2)

    # Deduplicate
    article_links = list(dict.fromkeys(article_links))[:10]  # cap at 10/run
    log.info(f"Inc42: scraping {len(article_links)} articles via Kimi")

    for link in article_links:
        article_rows = _fetch_and_extract(link, source="inc42")
        rows.extend(article_rows)
        time.sleep(2)

    return rows


# ─── SCRAPER 3: Entrackr ──────────────────────────────────────────────────────

ENTRACKR_NEWS_URL = "https://entrackr.com/news/"

def scrape_entrackr(max_articles: int = 10) -> list[dict]:
    rows = []
    log.info(f"Entrackr news: {ENTRACKR_NEWS_URL}")
    try:
        resp = requests.get(ENTRACKR_NEWS_URL, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        # Entrackr uses standard <a> tags in article cards
        funding_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if any(kw in text for kw in ["funding", "raises", "raised", "investment", "round"]):
                if href.startswith("http") and "entrackr.com" in href:
                    funding_links.append(href)

        funding_links = list(dict.fromkeys(funding_links))[:max_articles]
        log.info(f"Entrackr: found {len(funding_links)} funding articles")

        for link in funding_links:
            article_rows = _fetch_and_extract(link, source="entrackr")
            rows.extend(article_rows)
            time.sleep(2)

    except Exception as e:
        log.error(f"Entrackr failed: {e}")
    return rows


# ─── Shared: fetch article + Kimi extract ─────────────────────────────────────

def _fetch_and_extract(url: str, source: str) -> list[dict]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        # Get main article text
        article_tag = soup.find("article") or soup.find("div", class_=re.compile(r"content|post|article", re.I))
        text = article_tag.get_text(separator="\n", strip=True) if article_tag else soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)[:6000]

        deals = kimi_extract(text)
        rows = []
        for deal in deals:
            for inv in _split_investors(",".join(deal.get("investors") or [])):
                rows.append({
                    "startup_name": deal.get("startup_name", ""),
                    "domain": deal.get("domain", ""),
                    "round": deal.get("round", ""),
                    "amount_usd": deal.get("amount_usd"),
                    "investor": inv,
                    "date": deal.get("date", ""),
                    "source": source,
                })
        log.info(f"  {source}: {url} → {len(rows)} deal-investor rows")
        return rows
    except Exception as e:
        log.error(f"  {source}: fetch/extract failed for {url}: {e}")
        return []


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_amount(raw: str) -> float | None:
    """Convert '$3M', '₹25 Cr', 'Undisclosed' → float USD or None."""
    if not raw or raw.lower() in ("undisclosed", "—", "-", "n/a", ""):
        return None
    raw = raw.replace(",", "").replace(" ", "")
    # INR crore to USD
    m = re.search(r"₹([\d.]+)\s*[Cc]r", raw)
    if m:
        return round(float(m.group(1)) * 1_00_00_000 / 84, 0)
    # USD shorthand
    m = re.search(r"\$?([\d.]+)\s*([BMKbmk]?)", raw)
    if m:
        n, suffix = float(m.group(1)), m.group(2).upper()
        return n * {"B": 1e9, "M": 1e6, "K": 1e3}.get(suffix, 1)
    return None


def _split_investors(raw: str) -> list[str]:
    """Split 'Sequoia, Blume Ventures, Angel' → ['Sequoia','Blume Ventures','Angel']"""
    if not raw or raw.strip() in ("—", "-", "", "Undisclosed"):
        return ["Undisclosed"]
    parts = re.split(r",\s*|\s+and\s+|\s*/\s*", raw)
    return [p.strip() for p in parts if p.strip()]


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column types and clean obvious junk rows."""
    df = df.copy()
    df["startup_name"] = df["startup_name"].str.strip()
    df["domain"] = df["domain"].str.strip()
    df["investor"] = df["investor"].str.strip()
    df["round"] = df["round"].str.strip()
    df["scraped_at"] = datetime.utcnow().strftime("%Y-%m-%d")
    # Drop rows with no startup name
    df = df[df["startup_name"].str.len() > 1]
    return df


def _dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Append new rows, dedup on (startup_name, investor, round)."""
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["startup_name", "investor", "round"],
        keep="last"
    )
    return combined.sort_values(["domain", "startup_name"])


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== India Funding Scraper starting ===")

    all_rows = []

    # 1. StartupTalky (no AI needed)
    log.info("--- Source: StartupTalky ---")
    all_rows.extend(scrape_startuptalky())

    # 2. Inc42 (Kimi extraction)
    log.info("--- Source: Inc42 ---")
    all_rows.extend(scrape_inc42(max_pages=2))

    # 3. Entrackr (Kimi extraction)
    log.info("--- Source: Entrackr ---")
    all_rows.extend(scrape_entrackr(max_articles=8))

    if not all_rows:
        log.warning("No rows scraped — exiting without writing")
        return

    new_df = _normalise(pd.DataFrame(all_rows))

    # Load existing CSV if present
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        log.info(f"Existing CSV: {len(existing_df)} rows")
        final_df = _dedup(existing_df, new_df)
    else:
        final_df = new_df

    final_df.to_csv(OUTPUT_CSV, index=False)
    log.info(f"=== Done. Total rows saved: {len(final_df)} → {OUTPUT_CSV} ===")


if __name__ == "__main__":
    main()
