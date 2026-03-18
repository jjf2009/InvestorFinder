"""
India EdTech Investor Finder
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 1 — Scrape StartupTalky (2024/2025/2026) → filter EdTech rows
Step 2 — Scrape SEBI AIF registry → get contact emails for matched investors
Output → data/edtech_investors.csv
         data/india_funding_2024.csv  (full year data, all sectors)
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

# Every keyword pattern that means EdTech on StartupTalky
EDTECH_KEYWORDS = [
    "edtech", "ed tech", "ed-tech",
    "education", "e-learning", "elearning",
    "online learning", "upskill", "skilling",
    "k-12", "k12", "lms", "learning management",
    "test prep", "tutoring", "coaching",
    "vocational", "higher education",
]

FINAL_COLS_FULL = [
    "startup_name", "domain", "round", "amount_usd",
    "investor", "headquarters", "date", "scraped_at",
]

FINAL_COLS_EDTECH = [
    "investor",
    "investor_email",
    "sebi_reg_no",
    "aif_category",
    "investor_city",
    "startup_name",
    "domain",
    "round",
    "amount_usd",
    "headquarters",
    "date",
]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — StartupTalky scraper
# ══════════════════════════════════════════════════════════════════════════════

def scrape_year(year: str, url: str) -> pd.DataFrame:
    log.info(f"Scraping StartupTalky {year} …")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"  Fetch failed: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    log.info(f"  {len(tables)} tables found")

    rows = []
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
                for inv in _split_investors(row.pop("investors_raw", "")):
                    rows.append({**row, "investor": inv})

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    log.info(f"  Raw rows: {len(df)}")
    return df


def _map_columns(headers: list) -> dict:
    m = {}
    for i, h in enumerate(headers):
        hl = h.lower().strip()
        if any(k in hl for k in ("company", "startup")):
            m["startup_name"] = i
        elif any(k in hl for k in ("sector", "industry", "vertical", "domain")):
            m["domain"] = i
        elif any(k in hl for k in ("headquarter", "location", "city", "state")):
            m["headquarters"] = i
        elif ("amount" in hl or "raised" in hl) and "round" not in hl and "type" not in hl:
            m["amount_raw"] = i
        elif any(k in hl for k in ("round", "stage", "type")):
            m["round"] = i
        elif any(k in hl for k in ("investor", "lead")):
            m["investors_raw"] = i
        elif "date" in hl:
            m["date"] = i
    return m if "startup_name" in m else {}


def _parse_row(tds: list, col_map: dict):
    def g(key):
        idx = col_map.get(key)
        return tds[idx].strip() if idx is not None and idx < len(tds) else ""

    name = g("startup_name")
    if not name or name.lower() in ("company", "startup", "name", "—", "-", ""):
        return None
    return {
        "startup_name":  name,
        "domain":        g("domain"),
        "round":         g("round"),
        "amount_usd":    _parse_amount(g("amount_raw")),
        "headquarters":  g("headquarters"),
        "investors_raw": g("investors_raw"),
        "date":          g("date"),
    }


def _is_edtech(domain: str) -> bool:
    dl = domain.lower()
    return any(kw in dl for kw in EDTECH_KEYWORDS)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — SEBI AIF registry scraper
# ══════════════════════════════════════════════════════════════════════════════

SEBI_BASE = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do"
SEBI_PARAMS = "?doRecognisedFpi=yes&intmId=16"

def scrape_sebi_aif() -> pd.DataFrame:
    """
    Scrape all pages of the SEBI registered AIF directory.
    Returns DataFrame with: fund_name, sebi_reg_no, aif_category,
                            contact_email, city
    """
    log.info("Scraping SEBI AIF registry …")
    all_funds = []
    page = 0

    while True:
        if page == 0:
            url = SEBI_BASE + SEBI_PARAMS
        else:
            # SEBI paginates via JS form — we POST with page offset
            url = SEBI_BASE + SEBI_PARAMS + f"&pageNo={page}"

        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            log.error(f"  SEBI page {page} failed: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        funds = _parse_sebi_page(soup)

        if not funds:
            log.info(f"  No more funds at page {page} — stopping")
            break

        all_funds.extend(funds)
        log.info(f"  Page {page}: +{len(funds)} funds (total {len(all_funds)})")

        # Check if there's a next page link
        next_link = soup.find("a", string=lambda t: t and "Next" in t)
        if not next_link:
            break

        page += 1
        time.sleep(1.5)

    df = pd.DataFrame(all_funds) if all_funds else pd.DataFrame(
        columns=["fund_name", "sebi_reg_no", "aif_category", "contact_email", "city"]
    )
    log.info(f"SEBI registry total funds: {len(df)}")
    return df


def _parse_sebi_page(soup: BeautifulSoup) -> list:
    """Extract fund records from one SEBI AIF page."""
    funds = []

    # SEBI renders each fund as a block of label/value rows inside a table or div
    # Each fund block starts with "Registration No." and contains Name, Email, Address
    text = soup.get_text(separator="\n")
    blocks = re.split(r"\n(?=Registration No\.)", text)

    for block in blocks:
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if not lines:
            continue

        fund = {
            "fund_name":      "",
            "sebi_reg_no":    "",
            "aif_category":   "",
            "contact_email":  "",
            "city":           "",
        }

        for i, line in enumerate(lines):
            if line.startswith("Registration No."):
                # Next non-empty line is the reg number
                for j in range(i+1, min(i+3, len(lines))):
                    if re.match(r"IN/AIF", lines[j]):
                        fund["sebi_reg_no"] = lines[j]
                        # Derive category from reg no: IN/AIF1 = Cat I, AIF2 = Cat II etc.
                        m = re.search(r"IN/AIF(\d)", lines[j])
                        if m:
                            fund["aif_category"] = f"Category {['','I','II','III'][int(m.group(1))]}"
                        break

            elif line.startswith("Name"):
                for j in range(i+1, min(i+3, len(lines))):
                    if lines[j] and not lines[j].startswith("Registration"):
                        fund["fund_name"] = lines[j]
                        break

            elif line.startswith("E-mail"):
                for j in range(i+1, min(i+3, len(lines))):
                    if "@" in lines[j]:
                        fund["contact_email"] = lines[j].strip()
                        break

            elif line.startswith("Address"):
                # City is usually the second-to-last token before the pincode
                for j in range(i+1, min(i+3, len(lines))):
                    addr = lines[j]
                    # Extract city: pattern is "... CITY, STATE, PINCODE"
                    city_m = re.search(r",\s*([A-Z][A-Z\s]+),\s*[A-Z\s]+,\s*\d{6}", addr)
                    if city_m:
                        fund["city"] = city_m.group(1).title().strip()
                    elif addr:
                        # Fallback: take the last capitalised word segment
                        parts = [p.strip() for p in addr.split(",") if p.strip()]
                        if len(parts) >= 2:
                            fund["city"] = parts[-2].title().strip()

        if fund["sebi_reg_no"]:
            funds.append(fund)

    return funds


# ══════════════════════════════════════════════════════════════════════════════
# Matching — investor name → SEBI fund record
# ══════════════════════════════════════════════════════════════════════════════

def match_investors(edtech_df: pd.DataFrame, sebi_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fuzzy-ish match: for each unique investor name in edtech_df,
    find the best matching fund_name in sebi_df.
    Strategy: normalise both sides → check if investor tokens appear in fund name.
    """
    if sebi_df.empty:
        log.warning("SEBI data empty — skipping match, returning edtech data without contacts")
        edtech_df["investor_email"] = ""
        edtech_df["sebi_reg_no"]    = ""
        edtech_df["aif_category"]   = ""
        edtech_df["investor_city"]  = ""
        return edtech_df

    def normalise(s: str) -> str:
        s = s.lower()
        # Remove common suffixes that differ between sources
        s = re.sub(r"\b(fund|trust|capital|ventures?|investments?|partners?|india|pvt|ltd|llp|aif)\b", "", s)
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    sebi_df = sebi_df.copy()
    sebi_df["_norm"] = sebi_df["fund_name"].apply(normalise)

    results = []
    for _, row in edtech_df.iterrows():
        inv_norm = normalise(str(row["investor"]))
        inv_tokens = set(t for t in inv_norm.split() if len(t) > 2)

        best_match = None
        best_score = 0

        for _, fund in sebi_df.iterrows():
            fund_tokens = set(t for t in fund["_norm"].split() if len(t) > 2)
            if not fund_tokens:
                continue
            overlap = inv_tokens & fund_tokens
            score = len(overlap) / max(len(inv_tokens), 1)
            if score > best_score and score >= 0.5:   # at least 50% token overlap
                best_score = score
                best_match = fund

        new_row = row.to_dict()
        if best_match is not None:
            new_row["investor_email"] = best_match["contact_email"]
            new_row["sebi_reg_no"]    = best_match["sebi_reg_no"]
            new_row["aif_category"]   = best_match["aif_category"]
            new_row["investor_city"]  = best_match["city"]
        else:
            new_row["investor_email"] = ""
            new_row["sebi_reg_no"]    = ""
            new_row["aif_category"]   = ""
            new_row["investor_city"]  = ""

        results.append(new_row)

    return pd.DataFrame(results)


# ══════════════════════════════════════════════════════════════════════════════
# Cleaning helpers
# ══════════════════════════════════════════════════════════════════════════════

ROUND_MAP = {
    r"(?i)^pre[\s\-]?seed$":               "Pre-Seed",
    r"(?i)^seed$":                          "Seed",
    r"(?i)^series[\s\-]?a$":               "Series A",
    r"(?i)^series[\s\-]?b$":               "Series B",
    r"(?i)^series[\s\-]?c$":               "Series C",
    r"(?i)^series[\s\-]?d$":               "Series D",
    r"(?i)^series[\s\-]?e$":               "Series E",
    r"(?i)^series[\s\-]?f$":               "Series F",
    r"(?i)^angel$":                         "Angel",
    r"(?i)^(conventional[\s\-]?)?debt$":   "Debt",
    r"(?i)^(private[\s\-]?equity|pe)$":    "PE",
    r"(?i)^post[\s\-]?ipo$":               "Post-IPO",
    r"(?i)^grant$":                         "Grant",
    r"(?i)^bridge$":                        "Bridge",
}

DOMAIN_MAP = {
    r"\bEd[\s\-]?[Tt]ech\b":     "EdTech",
    r"\bFin[\s\-]?[Tt]ech\b":    "FinTech",
    r"\bHealth[\s\-]?[Tt]ech\b": "HealthTech",
    r"\bAgri[\s\-]?[Tt]ech\b":   "AgriTech",
    r"\bHR[\s\-]?[Tt]ech\b":     "HRTech",
    r"\bProp[\s\-]?[Tt]ech\b":   "PropTech",
}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()

    # Text columns
    for col in ["startup_name", "domain", "round", "investor", "headquarters", "date"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

    # Drop junk rows
    junk = {"", "nan", "none", "—", "-", "n/a", "na", "company", "startup", "name"}
    df = df[~df["startup_name"].str.lower().isin(junk)]

    # Domain normalise
    for pat, rep in DOMAIN_MAP.items():
        df["domain"] = df["domain"].str.replace(pat, rep, regex=True)

    # Round normalise
    for pat, rep in ROUND_MAP.items():
        mask = df["round"].str.match(pat, na=False)
        df.loc[mask, "round"] = rep

    # Investor clean
    df["investor"] = df["investor"].str.replace(r"[\*†‡§#]+$", "", regex=True).str.strip()
    df["investor"] = df["investor"].replace(
        ["", "nan", "none", "—", "-", "n/a", "undisclosed"], "Undisclosed"
    )

    # Amount
    if "amount_usd" in df.columns:
        df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
        df.loc[df["amount_usd"] == 0, "amount_usd"] = None

    df["scraped_at"] = datetime.now(UTC).strftime("%Y-%m-%d")
    return df


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
    if before - len(df):
        log.info(f"  Dedup removed {before - len(df)} rows")
    return df.reset_index(drop=True)


def merge_csv(new_df: pd.DataFrame, path: str) -> pd.DataFrame:
    if os.path.exists(path) and os.path.getsize(path) > 50:
        existing = pd.read_csv(path, dtype=str).fillna("")
        combined = pd.concat([existing, new_df.fillna("")], ignore_index=True)
    else:
        combined = new_df.fillna("")
    return (combined
            .drop_duplicates(subset=["startup_name", "investor", "round"], keep="last")
            .sort_values(["domain", "startup_name"])
            .reset_index(drop=True))


def _parse_amount(raw: str):
    if not raw or raw.lower() in ("undisclosed", "—", "-", "n/a", "na", ""):
        return None
    raw = raw.replace(",", "").replace(" ", "")
    m = re.search(r"₹([\d.]+)[Cc]r", raw)
    if m:
        return round(float(m.group(1)) * 1e7 / 84, 0)
    m = re.search(r"\$?([\d.]+)([BMKbmk]?)", raw)
    if m:
        n, s = float(m.group(1)), m.group(2).upper()
        val = n * {"B": 1e9, "M": 1e6, "K": 1e3}.get(s, 1)
        return val if val > 0 else None
    return None


def _split_investors(raw: str) -> list:
    if not raw or raw.strip() in ("", "—", "-", "nan", "None", "Undisclosed"):
        return ["Undisclosed"]
    parts = re.split(r",\s*|\s*/\s*|\s+and\s+", raw)
    return [p.strip() for p in parts if p.strip()] or ["Undisclosed"]


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("India EdTech Investor Finder")
    log.info("=" * 60)
    os.makedirs(DATA_DIR, exist_ok=True)

    all_edtech_rows = []

    # ── Step 1: scrape all years ───────────────────────────────────────────
    for year, url in STARTUPTALKY_URLS.items():
        log.info(f"\n── Year {year} ──")
        raw = scrape_year(year, url)
        if raw.empty:
            time.sleep(2)
            continue

        cleaned = clean(raw)
        cleaned = dedup(cleaned)

        # Save full-year CSV (all sectors)
        year_path = os.path.join(DATA_DIR, f"india_funding_{year}.csv")
        for col in FINAL_COLS_FULL:
            if col not in cleaned.columns:
                cleaned[col] = ""
        full_year = merge_csv(cleaned[FINAL_COLS_FULL], year_path)
        full_year.to_csv(year_path, index=False)
        log.info(f"  Saved {len(full_year)} rows → {year_path}")

        # Filter EdTech for this year
        edtech = cleaned[cleaned["domain"].apply(_is_edtech)].copy()
        log.info(f"  EdTech rows this year: {len(edtech)}")
        all_edtech_rows.append(edtech)

        time.sleep(2)

    if not all_edtech_rows:
        log.error("No EdTech data scraped — exiting")
        return

    edtech_df = pd.concat(all_edtech_rows, ignore_index=True)
    edtech_df = dedup(edtech_df)
    log.info(f"\nTotal unique EdTech deals: {len(edtech_df)}")
    log.info(f"Unique EdTech investors:   {edtech_df['investor'].nunique()}")

    # ── Step 2: SEBI AIF registry ─────────────────────────────────────────
    log.info("\n── SEBI AIF Registry ──")
    sebi_df = scrape_sebi_aif()

    # ── Step 3: Match & enrich ────────────────────────────────────────────
    log.info("\n── Matching investors to SEBI registry ──")
    enriched = match_investors(edtech_df, sebi_df)

    matched = enriched[enriched["investor_email"] != ""]
    log.info(f"  Investors matched with SEBI contact: {enriched['investor'].nunique()} total, "
             f"{matched['investor'].nunique()} with email found")

    # ── Step 4: Save final EdTech investor CSV ────────────────────────────
    for col in FINAL_COLS_EDTECH:
        if col not in enriched.columns:
            enriched[col] = ""

    final = (enriched[FINAL_COLS_EDTECH]
             .sort_values(["investor", "startup_name"])
             .reset_index(drop=True))

    out_path = os.path.join(DATA_DIR, "edtech_investors.csv")
    final.to_csv(out_path, index=False)
    log.info(f"\n✓ Saved {len(final)} rows → {out_path}")
    log.info(f"  Columns: {list(final.columns)}")
    log.info("\n" + "=" * 60)
    log.info("Done.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
