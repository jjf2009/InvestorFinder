"""
India Startup Funding Scraper
Source:  StartupTalky only (clean HTML tables, no AI needed)
Output:  data/india_funding_2024.csv
         data/india_funding_2025.csv
         data/india_funding_2026.csv
"""

import os
import re
import time
import logging
import requests
import pandas as pd
from datetime import datetime, UTC
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

STARTUPTALKY_URLS = {
    "2024": "https://startuptalky.com/indian-startups-funding-investor-data-2024/",
    "2025": "https://startuptalky.com/indian-startups-funding-investors-data-2025/",
    "2026": "https://startuptalky.com/indian-startups-funding-investors-data-2026/",
}

# Canonical column names in final CSV
FINAL_COLS = [
    "startup_name",
    "domain",
    "round",
    "amount_usd",
    "investor",
    "headquarters",
    "date",
    "scraped_at",
]

# ─── Scraper ──────────────────────────────────────────────────────────────────

def scrape_year(year: str, url: str) -> pd.DataFrame:
    log.info(f"Scraping {year}: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"  Failed to fetch {year}: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    log.info(f"  Found {len(tables)} tables")

    all_rows = []
    for table in tables:
        ths = [th.get_text(strip=True) for th in table.find_all("th")]
        col_map = _map_columns(ths)
        if not col_map:
            continue
        for tr in table.find_all("tr")[1:]:
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 3:
                continue
            row = _parse_row(tds, col_map)
            if row:
                # One row per investor (explode multi-investor cells)
                for inv in _split_investors(row.pop("investors_raw", "")):
                    all_rows.append({**row, "investor": inv})

    if not all_rows:
        log.warning(f"  No rows parsed for {year}")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    log.info(f"  Raw rows (before cleaning): {len(df)}")
    return df


def _map_columns(headers: list) -> dict:
    """Map raw table header names to canonical keys."""
    mapping = {}
    for i, h in enumerate(headers):
        hl = h.lower().strip()
        if any(k in hl for k in ("company", "startup")):
            mapping["startup_name"] = i
        elif any(k in hl for k in ("sector", "industry", "vertical", "domain")):
            mapping["domain"] = i
        elif any(k in hl for k in ("headquarter", "location", "city", "state")):
            mapping["headquarters"] = i
        elif ("amount" in hl or "raised" in hl or "funding" in hl) and "round" not in hl and "type" not in hl:
            mapping["amount_raw"] = i
        elif any(k in hl for k in ("round", "stage", "type")):
            mapping["round"] = i
        elif any(k in hl for k in ("investor", "lead")):
            mapping["investors_raw"] = i
        elif "date" in hl:
            mapping["date"] = i
    return mapping if "startup_name" in mapping else {}


def _parse_row(tds: list, col_map: dict) -> dict | None:
    def g(key):
        idx = col_map.get(key)
        return tds[idx].strip() if idx is not None and idx < len(tds) else ""

    startup = g("startup_name")
    if not startup or startup.lower() in ("company", "startup", "name", "—", "-", ""):
        return None

    return {
        "startup_name":  startup,
        "domain":        g("domain"),
        "round":         g("round"),
        "amount_usd":    _parse_amount(g("amount_raw")),
        "headquarters":  g("headquarters"),
        "investors_raw": g("investors_raw"),
        "date":          g("date"),
    }


# ─── Cleaning ─────────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame, year: str) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # ── 1. Normalise text columns ──────────────────────────────────────────────
    for col in ["startup_name", "domain", "round", "investor", "headquarters", "date"]:
        if col in df.columns:
            df[col] = (df[col]
                       .astype(str)
                       .str.strip()
                       .str.replace(r"\s+", " ", regex=True))   # collapse internal spaces

    # ── 2. Drop clearly empty / header rows ───────────────────────────────────
    junk = {"", "nan", "none", "—", "-", "n/a", "na", "company", "startup", "name"}
    df = df[~df["startup_name"].str.lower().isin(junk)]

    # ── 3. Clean startup_name ─────────────────────────────────────────────────
    # Remove trailing punctuation, superscripts, footnote markers
    df["startup_name"] = df["startup_name"].str.replace(r"[\*†‡§#]+$", "", regex=True).str.strip()

    # ── 4. Normalise domain ───────────────────────────────────────────────────
    # Collapse minor variants: "Ed Tech" → "EdTech", "Fin Tech" → "FinTech" etc.
    domain_fixes = {
        r"\bEd[\s\-]?[Tt]ech\b":     "EdTech",
        r"\bFin[\s\-]?[Tt]ech\b":    "FinTech",
        r"\bHealth[\s\-]?[Tt]ech\b": "HealthTech",
        r"\bAgri[\s\-]?[Tt]ech\b":   "AgriTech",
        r"\bHR[\s\-]?[Tt]ech\b":     "HRTech",
        r"\bProp[\s\-]?[Tt]ech\b":   "PropTech",
        r"\bLegal[\s\-]?[Tt]ech\b":  "LegalTech",
        r"\bClean[\s\-]?[Tt]ech\b":  "CleanTech",
        r"\bRe[\s\-]?[Tt]ech\b":     "RetailTech",
    }
    for pattern, replacement in domain_fixes.items():
        df["domain"] = df["domain"].str.replace(pattern, replacement, regex=True)

    # ── 5. Normalise round ────────────────────────────────────────────────────
    round_fixes = {
        r"(?i)^pre[\s\-]?seed$":          "Pre-Seed",
        r"(?i)^seed$":                     "Seed",
        r"(?i)^series[\s\-]?a$":          "Series A",
        r"(?i)^series[\s\-]?b$":          "Series B",
        r"(?i)^series[\s\-]?c$":          "Series C",
        r"(?i)^series[\s\-]?d$":          "Series D",
        r"(?i)^series[\s\-]?e$":          "Series E",
        r"(?i)^series[\s\-]?f$":          "Series F",
        r"(?i)^angel$":                    "Angel",
        r"(?i)^(conventional[\s\-]?)?debt$": "Debt",
        r"(?i)^(private[\s\-]?equity|pe)$":  "PE",
        r"(?i)^post[\s\-]?ipo$":          "Post-IPO",
        r"(?i)^grant$":                    "Grant",
        r"(?i)^bridge$":                   "Bridge",
    }
    for pattern, replacement in round_fixes.items():
        mask = df["round"].str.match(pattern, na=False)
        df.loc[mask, "round"] = replacement

    # ── 6. Clean investor ─────────────────────────────────────────────────────
    df["investor"] = df["investor"].str.replace(r"[\*†‡§#]+$", "", regex=True).str.strip()
    # Replace empty/undisclosed with a consistent value
    df["investor"] = df["investor"].replace(
        to_replace=["", "nan", "none", "—", "-", "n/a", "undisclosed"],
        value="Undisclosed"
    )

    # ── 7. Drop rows where amount_usd is 0 ────────────────────────────────────
    if "amount_usd" in df.columns:
        df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
        df.loc[df["amount_usd"] == 0, "amount_usd"] = None

    # ── 8. Add metadata ───────────────────────────────────────────────────────
    df["scraped_at"] = datetime.now(UTC).strftime("%Y-%m-%d")

    # ── 9. Enforce final column order (add missing cols as empty) ─────────────
    for col in FINAL_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[FINAL_COLS]

    # ── 10. Deduplicate ───────────────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
    dupes_removed = before - len(df)
    if dupes_removed:
        log.info(f"  Removed {dupes_removed} duplicate rows")

    df = df.sort_values(["domain", "startup_name", "investor"]).reset_index(drop=True)
    log.info(f"  Clean rows for {year}: {len(df)}")
    return df


# ─── Merge with existing CSV ──────────────────────────────────────────────────

def merge_with_existing(new_df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """Append new rows to existing CSV, then dedup the combined set."""
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 50:
        existing = pd.read_csv(csv_path, dtype=str).fillna("")
        log.info(f"  Existing rows in CSV: {len(existing)}")
        combined = pd.concat([existing, new_df.fillna("")], ignore_index=True)
    else:
        combined = new_df.fillna("")

    before = len(combined)
    combined = combined.drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
    log.info(f"  After merge dedup: {before} → {len(combined)} rows")
    return combined.sort_values(["domain", "startup_name"]).reset_index(drop=True)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_amount(raw: str):
    if not raw or raw.lower() in ("undisclosed", "—", "-", "n/a", "na", ""):
        return None
    raw = raw.replace(",", "").replace(" ", "")
    # INR crore
    m = re.search(r"₹([\d.]+)[Cc]r", raw)
    if m:
        return round(float(m.group(1)) * 1_00_00_000 / 84, 0)
    # USD shorthand: $3M, $500K, $1B
    m = re.search(r"\$?([\d.]+)([BMKbmk]?)", raw)
    if m:
        n, s = float(m.group(1)), m.group(2).upper()
        multiplier = {"B": 1e9, "M": 1e6, "K": 1e3}.get(s, 1)
        val = n * multiplier
        return val if val > 0 else None
    return None


def _split_investors(raw: str) -> list:
    """'Sequoia, Blume / Accel' → ['Sequoia', 'Blume', 'Accel']"""
    if not raw or raw.strip() in ("", "—", "-", "nan", "None", "Undisclosed"):
        return ["Undisclosed"]
    parts = re.split(r",\s*|\s*/\s*|\s+and\s+", raw)
    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned if cleaned else ["Undisclosed"]


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== India Funding Scraper starting ===")
    os.makedirs(DATA_DIR, exist_ok=True)

    for year, url in STARTUPTALKY_URLS.items():
        log.info(f"\n--- Year {year} ---")

        raw_df = scrape_year(year, url)
        if raw_df.empty:
            log.warning(f"No data scraped for {year}, skipping")
            time.sleep(2)
            continue

        clean_df = clean(raw_df, year)
        if clean_df.empty:
            log.warning(f"No clean rows for {year}, skipping")
            time.sleep(2)
            continue

        csv_path = os.path.join(DATA_DIR, f"india_funding_{year}.csv")
        final_df = merge_with_existing(clean_df, csv_path)
        final_df.to_csv(csv_path, index=False)
        log.info(f"  Saved {len(final_df)} rows → {csv_path}")

        time.sleep(2)   # be polite between year pages

    log.info("\n=== Scraper done ===")


if __name__ == "__main__":
    main()
