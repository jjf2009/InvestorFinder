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
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def _get_key(): return os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL   = "gemini-2.5-flash"
GEMINI_URL     = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
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

# ─── Gemini API ───────────────────────────────────────────────────────────────

EXTRACT_PROMPT = """
You are extracting Indian startup funding deals from news article text.
Extract EVERY funding deal mentioned. Return ONLY a valid JSON array — no explanation, no markdown, no backticks.

For each deal extract:
- startup_name: company name (string)
- domain: the sector/industry (e.g. "EdTech", "FinTech", "HealthTech", "AgriTech", "SaaS")
- round: funding stage (e.g. "Seed", "Pre-Seed", "Series A", "Series B", "Angel", "Debt")
- amount_usd: amount in USD as a plain number, null if undisclosed
- investors: array of investor name strings — split multiple investors into separate items
- date: date string YYYY-MM-DD if mentioned, else null

Conversion rules:
- INR crores to USD: divide by 84, then multiply by 10,000,000 (e.g. ₹10 Cr = ~$119,048)
- $1 Mn = 1,000,000 | $1 Bn = 1,000,000,000
- "undisclosed" → null

Return ONLY a JSON array. If no deals found return [].
Example: [{{"startup_name":"Acme","domain":"FinTech","round":"Seed","amount_usd":500000,"investors":["Sequoia"],"date":"2026-03-10"}}]

Article text:
{article_text}
"""

def gemini_extract(article_text: str) -> list:
    GEMINI_API_KEY = _get_key()
    if not GEMINI_API_KEY:
        log.warning("No GEMINI_API_KEY — skipping AI extraction")
        return []
    try:
        resp = requests.post(
            GEMINI_URL.format(key=GEMINI_API_KEY),  # uses local var from line above
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": EXTRACT_PROMPT.format(
                    article_text=article_text[:6000]
                )}]}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2048}
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        # Strip markdown fences Gemini sometimes adds
        raw = re.sub(r"^```[a-z]*\s*", "", raw, flags=re.MULTILINE)
        raw = raw.strip().rstrip("`").strip()

        parsed = json.loads(raw)

        # Gemini sometimes returns a dict instead of array — normalise both
        if isinstance(parsed, dict):
            # e.g. {"deals": [...]} or a single deal object
            if "deals" in parsed:
                parsed = parsed["deals"]
            elif "startup_name" in parsed:
                parsed = [parsed]
            else:
                # Try to find any list value
                for v in parsed.values():
                    if isinstance(v, list):
                        parsed = v
                        break
                else:
                    parsed = []

        return parsed if isinstance(parsed, list) else []

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            log.warning("Gemini rate limit — sleeping 65s and retrying once")
            time.sleep(65)
            return gemini_extract(article_text)
        log.error(f"Gemini HTTP error: {e}")
        return []
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        log.error(f"Gemini parse error: {e} | raw snippet: {raw[:200] if 'raw' in dir() else 'N/A'}")
        return []
    except Exception as e:
        log.error(f"Gemini extraction failed: {e}")
        return []


# ─── SCRAPER 1: StartupTalky ──────────────────────────────────────────────────

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
    log.info(f"StartupTalky total rows: {len(rows)}")
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
# Inc42's tag page is JS-rendered — BS can't list articles from it.
# Strategy: build article URLs directly using their known slug pattern,
# verified via Google search: inc42.com/buzz/funding-galore-* and
# inc42.com/buzz/*weekly-funding-*
# We generate candidate URLs for the last 8 weeks and probe which exist.

def _inc42_candidate_urls() -> list:
    """
    Generate likely Inc42 weekly funding URLs for the past 8 weeks.
    Pattern 1: /buzz/funding-galore-<week-date-range>/
    Pattern 2: /buzz/<topic>-weekly-funding-rundown-<more>/
    We probe both and keep the ones that return 200.
    """
    urls = []
    today = datetime.utcnow()
    for weeks_back in range(0, 8):
        week_end = today - timedelta(weeks=weeks_back)
        week_start = week_end - timedelta(days=6)
        # Format: "14-march-2026" style used in Inc42 slugs
        end_str   = week_end.strftime("%-d-%B-%Y").lower()
        start_str = week_start.strftime("%-d-%B-%Y").lower()
        # Also try month-number style
        end_str2   = week_end.strftime("%-d-%b-%Y").lower()
        start_str2 = week_start.strftime("%-d-%b-%Y").lower()

        candidates = [
            f"https://inc42.com/buzz/funding-galore-{start_str}-{end_str}/",
            f"https://inc42.com/buzz/funding-galore-indian-startup-funding-{start_str}-{end_str}/",
            f"https://inc42.com/buzz/funding-galore-{start_str2}-{end_str2}/",
        ]
        urls.extend(candidates)
    return urls


def scrape_inc42() -> list:
    rows = []
    log.info("Inc42: probing weekly funding article URLs")
    candidates = _inc42_candidate_urls()
    found = []

    for url in candidates:
        try:
            r = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                found.append(url)
                log.info(f"  Inc42 URL found: {url}")
            time.sleep(1)
        except Exception:
            pass

    # Fallback: if no URLs found via probing, try scraping inc42.com/buzz/ directly
    if not found:
        log.info("Inc42: probing failed, trying /buzz/ listing page")
        found = _scrape_inc42_buzz_listing()

    log.info(f"Inc42: scraping {len(found)} articles")
    for url in found[:6]:
        rows.extend(_fetch_and_extract(url, source="inc42"))
        time.sleep(7)   # stay under 10 RPM Gemini limit

    log.info(f"Inc42 total rows: {len(rows)}")
    return rows


def _scrape_inc42_buzz_listing() -> list:
    """Scrape inc42.com/buzz/ for links containing 'funding' in href or text."""
    found = []
    try:
        resp = requests.get("https://inc42.com/buzz/", headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if (
                "inc42.com/buzz/" in href
                and any(kw in href + text for kw in ["funding-galore", "funding-rundown", "weekly-funding"])
                and href not in found
            ):
                found.append(href)
        log.info(f"  Inc42 /buzz/ listing: found {len(found)} links")
    except Exception as e:
        log.error(f"Inc42 /buzz/ listing failed: {e}")
    return found[:6]


# ─── SCRAPER 3: Entrackr ──────────────────────────────────────────────────────
# Entrackr's /news/ page uses a different structure — articles are in <h2>/<h3>
# tags inside card divs, not plain <a> tags with funding keywords in link text.

def scrape_entrackr(max_articles: int = 8) -> list:
    rows = []
    log.info("Entrackr: scraping news listing")
    try:
        resp = requests.get("https://entrackr.com/news/", headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        funding_keywords = {"funding", "raises", "raised", "secures", "secured",
                            "investment", "round", "crore", "mn", "million"}
        found = []

        # Strategy 1: any <a> whose href is an entrackr article path
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Normalise relative URLs
            if href.startswith("/"):
                href = "https://entrackr.com" + href
            if "entrackr.com" not in href:
                continue
            combined_text = (a.get_text(strip=True) + " " + href).lower()
            if any(kw in combined_text for kw in funding_keywords):
                if href not in found:
                    found.append(href)

        # Strategy 2: grab ALL internal article links and let Gemini filter
        if len(found) < 3:
            log.info("  Entrackr: few keyword matches, widening to all article links")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/"):
                    href = "https://entrackr.com" + href
                if (
                    "entrackr.com" in href
                    and len(href) > 30           # skip homepage/category pages
                    and href not in found
                    and not href.endswith("/news/")
                    and not href.endswith("/entrackr.com/")
                ):
                    found.append(href)

        found = list(dict.fromkeys(found))[:max_articles]
        log.info(f"  Entrackr: scraping {len(found)} articles")

        for url in found:
            rows.extend(_fetch_and_extract(url, source="entrackr"))
            time.sleep(7)

    except Exception as e:
        log.error(f"Entrackr scraper failed: {e}")

    log.info(f"Entrackr total rows: {len(rows)}")
    return rows


# ─── Shared: fetch article text + Gemini extract ──────────────────────────────

def _fetch_and_extract(url: str, source: str) -> list:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav, footer, sidebar noise
        for tag in soup(["nav", "footer", "header", "script", "style", "aside"]):
            tag.decompose()

        article = (
            soup.find("article")
            or soup.find("div", class_=re.compile(r"\b(content|post-body|entry|article)\b", re.I))
            or soup.find("main")
        )
        text = article.get_text("\n", strip=True) if article else soup.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        rows = []
        for deal in gemini_extract(text):
            if not isinstance(deal, dict):
                continue
            for inv in _split_investors(",".join(
                deal.get("investors") or []
                if isinstance(deal.get("investors"), list)
                else [str(deal.get("investors", ""))]
            )):
                rows.append({
                    "startup_name": str(deal.get("startup_name", "")).strip(),
                    "domain":       str(deal.get("domain", "")).strip(),
                    "round":        str(deal.get("round", "")).strip(),
                    "amount_usd":   deal.get("amount_usd"),
                    "investor":     inv,
                    "date":         str(deal.get("date", "")).strip(),
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
    if not raw or raw.strip() in ("—", "-", "", "Undisclosed", "None", "nan"):
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
    # Fill NA before concat to silence FutureWarning
    existing = existing.fillna("")
    new = new.fillna("")
    combined = pd.concat([existing, new], ignore_index=True)
    return (combined
            .drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
            .sort_values(["domain", "startup_name"])
            .reset_index(drop=True))


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== India Funding Scraper starting ===")
    all_rows = []

    log.info("--- Source: StartupTalky ---")
    all_rows.extend(scrape_startuptalky())

    log.info("--- Source: Inc42 ---")
    all_rows.extend(scrape_inc42())

    log.info("--- Source: Entrackr ---")
    all_rows.extend(scrape_entrackr(max_articles=8))

    if not all_rows:
        log.warning("No rows scraped — exiting without writing")
        return

    new_df = _normalise(pd.DataFrame(all_rows))
    log.info(f"New rows scraped this run: {len(new_df)}")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV).fillna("")
        log.info(f"Existing CSV rows: {len(existing_df)}")
        final_df = _dedup(existing_df, new_df)
    else:
        final_df = new_df

    final_df.to_csv(OUTPUT_CSV, index=False)
    log.info(f"=== Done. {len(final_df)} total rows → {OUTPUT_CSV} ===")


if __name__ == "__main__":
    main()
