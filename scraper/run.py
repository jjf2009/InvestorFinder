"""
India Startup Funding Scraper
Sources: StartupTalky (2024/2025/2026), Inc42, Entrackr
AI:      Google Gemini 2.5 Flash (free tier — 10 RPM, 250 RPD)
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
# Free tier model — 10 RPM, 250 RPD, no credit card needed
GEMINI_MODEL   = "gemini-2.5-flash"
GEMINI_URL     = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={{key}}"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "india_funding.csv")

# ─── Gemini API (plain requests, no SDK) ──────────────────────────────────────

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
- If amount is in INR crores, convert to USD at 1 USD = 84 INR (1 Cr = 10,000,000 INR)
- If amount says "undisclosed" set to null
- If multiple investors listed, split them into the array
- Return [] if no funding deals found in the text

Return format — JSON array only, nothing else:
[{"startup_name":"...","domain":"...","round":"...","amount_usd":null,"investors":["..."],"date":"..."}]

Article text:
{article_text}
"""

def gemini_extract(article_text: str) -> list:
    """Call Gemini API directly via requests — no SDK, no version drift."""
    if not GEMINI_API_KEY:
        log.warning("No GEMINI_API_KEY set — skipping AI extraction")
        return []
    try:
        resp = requests.post(
            GEMINI_URL.format(key=GEMINI_API_KEY),
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{
                    "parts": [{
                        "text": EXTRACT_PROMPT.format(article_text=article_text[:6000])
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 2048,
                }
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        # Strip any accidental markdown fences Gemini adds
        raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        return json.loads(raw)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            log.warning("Gemini rate limit hit — sleeping 65s then retrying")
            time.sleep(65)
            return gemini_extract(article_text)  # single retry
        log.error(f"Gemini HTTP error: {e}")
        return []
    except Exception as e:
        log.error(f"Gemini extraction failed: {e}")
        return []


# ─── SCRAPER 1: StartupTalky (plain HTML tables, no AI needed) ────────────────

STARTUPTALKY_URLS = {
    "2024": "https://startuptalky.com/indian-startups-funding-investor-data-2024/",
    "2025": "https://startuptalky.com/indian-startups-funding-investors-data-2025/",
    "2026": "https://startuptalky.com/indian-startups-funding-investors-data-2026/",
}

def scrape_startuptalky() -> list:
    rows = []
    for year, url in STARTUPTALKY_URLS.items():
        log.info(f"StartupTalky {year}: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            tables = soup.find_all("table")
            log.info(f"  Found {len(tables)} tables")

            for table in tables:
                ths = [th.get_text(strip=True) for th in table.find_all("th")]
                col_map = _map_startuptalky_cols(ths)
                if not col_map:
                    continue
                for tr in table.find_all("tr")[1:]:
                    tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                    if len(tds) < 3:
                        continue
                    row = _extract_startuptalky_row(tds, col_map)
                    if row:
                        row["source"] = f"startuptalky_{year}"
                        for inv in _split_investors(row.pop("investors_raw", "")):
                            rows.append({**row, "investor": inv})
        except Exception as e:
            log.error(f"StartupTalky {year} failed: {e}")
        time.sleep(2)
    return rows


def _map_startuptalky_cols(headers: list) -> dict:
    mapping = {}
    for i, h in enumerate(headers):
        hl = h.lower()
        if "company" in hl or "startup" in hl:
            mapping["startup_name"] = i
        elif "sector" in hl or "industry" in hl or "vertical" in hl:
            mapping["domain"] = i
        elif ("amount" in hl or "raised" in hl) and "round" not in hl:
            mapping["amount_usd"] = i
        elif "round" in hl or "stage" in hl or "type" in hl:
            mapping["round"] = i
        elif "investor" in hl or "lead" in hl:
            mapping["investors_raw"] = i
        elif "date" in hl:
            mapping["date"] = i
    return mapping if "startup_name" in mapping else {}


def _extract_startuptalky_row(tds: list, col_map: dict):
    def g(key):
        idx = col_map.get(key)
        return tds[idx].strip() if idx is not None and idx < len(tds) else ""

    startup = g("startup_name")
    if not startup or startup.lower() in ("company", "startup", "—", "-", ""):
        return None
    return {
        "startup_name": startup,
        "domain":        g("domain"),
        "round":         g("round"),
        "amount_usd":    _parse_amount(g("amount_usd")),
        "investors_raw": g("investors_raw"),
        "date":          g("date"),
    }


# ─── SCRAPER 2: Inc42 ─────────────────────────────────────────────────────────

INC42_TAG_URL = "https://inc42.com/tag/funding-galore/"

def scrape_inc42(max_pages: int = 2) -> list:
    rows = []
    article_links = []
    for page in range(1, max_pages + 1):
        url = INC42_TAG_URL if page == 1 else f"{INC42_TAG_URL}page/{page}/"
        log.info(f"Inc42 tag page {page}: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")
            links = list(dict.fromkeys(
                a["href"] for a in soup.find_all("a", href=True)
                if "funding-galore" in a["href"] and a["href"] not in article_links
            ))
            article_links.extend(links)
            log.info(f"  Found {len(links)} new links")
        except Exception as e:
            log.error(f"Inc42 page {page} failed: {e}")
        time.sleep(2)

    for link in list(dict.fromkeys(article_links))[:8]:
        rows.extend(_fetch_and_extract(link, source="inc42"))
        time.sleep(7)   # stay well under 10 RPM Gemini free limit
    return rows


# ─── SCRAPER 3: Entrackr ──────────────────────────────────────────────────────

ENTRACKR_NEWS_URL = "https://entrackr.com/news/"

def scrape_entrackr(max_articles: int = 8) -> list:
    rows = []
    log.info(f"Entrackr: {ENTRACKR_NEWS_URL}")
    try:
        resp = requests.get(ENTRACKR_NEWS_URL, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        funding_links = list(dict.fromkeys(
            a["href"] for a in soup.find_all("a", href=True)
            if "entrackr.com" in a.get("href", "")
            and any(kw in a.get_text(strip=True).lower()
                    for kw in ["funding", "raises", "raised", "investment", "round"])
        ))[:max_articles]
        log.info(f"  Found {len(funding_links)} funding articles")
        for link in funding_links:
            rows.extend(_fetch_and_extract(link, source="entrackr"))
            time.sleep(7)   # stay under Gemini RPM limit
    except Exception as e:
        log.error(f"Entrackr failed: {e}")
    return rows


# ─── Shared: fetch article + Gemini extract ───────────────────────────────────

def _fetch_and_extract(url: str, source: str) -> list:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        article = (soup.find("article")
                   or soup.find("div", class_=re.compile(r"content|post|article", re.I)))
        text = article.get_text("\n", strip=True) if article else soup.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)

        rows = []
        for deal in gemini_extract(text):
            for inv in _split_investors(",".join(deal.get("investors") or [])):
                rows.append({
                    "startup_name": deal.get("startup_name", ""),
                    "domain":       deal.get("domain", ""),
                    "round":        deal.get("round", ""),
                    "amount_usd":   deal.get("amount_usd"),
                    "investor":     inv,
                    "date":         deal.get("date", ""),
                    "source":       source,
                })
        log.info(f"  {source}: {url} → {len(rows)} rows")
        return rows
    except Exception as e:
        log.error(f"  {source}: failed {url}: {e}")
        return []


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_amount(raw: str):
    if not raw or raw.lower() in ("undisclosed", "—", "-", "n/a", ""):
        return None
    raw = raw.replace(",", "").replace(" ", "")
    m = re.search(r"₹([\d.]+)[Cc]r", raw)
    if m:
        return round(float(m.group(1)) * 1_00_00_000 / 84, 0)
    m = re.search(r"\$?([\d.]+)([BMKbmk]?)", raw)
    if m:
        n, s = float(m.group(1)), m.group(2).upper()
        return n * {"B": 1e9, "M": 1e6, "K": 1e3}.get(s, 1)
    return None


def _split_investors(raw: str) -> list:
    if not raw or raw.strip() in ("—", "-", "", "Undisclosed"):
        return ["Undisclosed"]
    return [p.strip() for p in re.split(r",\s*|\s+and\s+|\s*/\s*", raw) if p.strip()]


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["startup_name", "domain", "investor", "round"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    df["scraped_at"] = datetime.utcnow().strftime("%Y-%m-%d")
    return df[df["startup_name"].str.len() > 1]


def _dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, new], ignore_index=True)
    return (combined
            .drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
            .sort_values(["domain", "startup_name"]))


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== India Funding Scraper starting ===")

    all_rows = []

    log.info("--- Source: StartupTalky ---")
    all_rows.extend(scrape_startuptalky())

    log.info("--- Source: Inc42 ---")
    all_rows.extend(scrape_inc42(max_pages=2))

    log.info("--- Source: Entrackr ---")
    all_rows.extend(scrape_entrackr(max_articles=8))

    if not all_rows:
        log.warning("No rows scraped — exiting without writing")
        return

    new_df = _normalise(pd.DataFrame(all_rows))

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        log.info(f"Existing CSV: {len(existing_df)} rows")
        final_df = _dedup(existing_df, new_df)
    else:
        final_df = new_df

    final_df.to_csv(OUTPUT_CSV, index=False)
    log.info(f"=== Done. {len(final_df)} rows → {OUTPUT_CSV} ===")


if __name__ == "__main__":
    main()
