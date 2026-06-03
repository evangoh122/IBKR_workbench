"""
etl/extract_edgar.py
EDGAR (SEC) data connector using the free SEC EDGAR REST API.
No API key required. Respects SEC's 10 req/s rate-limit guideline.

Endpoints used:
  submissions  – filing history per CIK (maps ticker → CIK, recent filings)
  company_facts – XBRL financial facts (revenue, assets, EPS, etc.)

Data stored:
  edgar_filings  – filing metadata (form type, date, accession number)
  edgar_facts    – numerical XBRL facts per ticker/concept/period
"""
import time
from typing import Dict, List, Optional

import requests
from loguru import logger

from db.database import get_connection

# SEC requires a descriptive User-Agent: "Name email@domain.com"
_HEADERS = {
    "User-Agent": "IBKR-Workbench research@example.com",
    "Accept":     "application/json",
}
_BASE    = "https://data.sec.gov"
_DELAY   = 0.12   # ~8 req/s, safely under the 10/s limit


# ── CIK lookup ────────────────────────────────────────────────────────────────

def get_cik(ticker: str) -> Optional[str]:
    """Return zero-padded 10-digit CIK for a ticker, or None if not found."""
    cik_map = _build_cik_map([ticker])
    return cik_map.get(ticker.upper())


# ── Submissions (filing history) ──────────────────────────────────────────────

def _symbol(t) -> str:
    """Accept either a plain ticker string or a tickers-dict and return the symbol."""
    return t["symbol"] if isinstance(t, dict) else str(t)


def run_edgar_filings_etl(
    tickers,
    form_types: Optional[List[str]] = None,
) -> int:
    """
    Fetch recent filing metadata for each ticker and store in edgar_filings.
    form_types: filter to specific forms e.g. ['10-K', '10-Q', '8-K'].
                Defaults to ['10-K', '10-Q', '8-K'].
    """
    if form_types is None:
        form_types = ["10-K", "10-Q", "8-K"]

    form_set = set(form_types)
    total    = 0

    symbols  = [_symbol(t) for t in tickers]
    cik_map  = _build_cik_map(symbols)

    with get_connection() as conn:
        for ticker in symbols:
            cik = cik_map.get(ticker.upper())
            if not cik:
                continue

            data = _get(f"{_BASE}/submissions/CIK{cik}.json")
            if data is None:
                continue

            recent = data.get("filings", {}).get("recent", {})
            forms      = recent.get("form",           [])
            filed_dates= recent.get("filingDate",     [])
            accessions = recent.get("accessionNumber",[])
            descriptions = recent.get("primaryDocument", [])

            rows_written = 0
            for form, filed, accession, doc in zip(forms, filed_dates, accessions, descriptions):
                if form not in form_set:
                    continue
                conn.execute("""
                    INSERT OR IGNORE INTO edgar_filings
                        (ticker, cik, form_type, filed_date, accession_number, primary_doc)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (ticker, cik, form, filed, accession, doc))
                rows_written += 1

            conn.commit()
            total += rows_written
            logger.debug(f"EDGAR filings {ticker}: {rows_written} filings stored")
            time.sleep(_DELAY)

    logger.info(f"EDGAR filings ETL complete: {total} filings across {len(tickers)} tickers")
    return total


# ── Company facts (XBRL financials) ──────────────────────────────────────────

# Concepts we care about — (taxonomy, concept_name, human label)
_CONCEPTS = [
    ("us-gaap", "Revenues",                          "revenue"),
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", "revenue_alt"),
    ("us-gaap", "NetIncomeLoss",                     "net_income"),
    ("us-gaap", "EarningsPerShareBasic",              "eps_basic"),
    ("us-gaap", "EarningsPerShareDiluted",            "eps_diluted"),
    ("us-gaap", "Assets",                            "total_assets"),
    ("us-gaap", "Liabilities",                       "total_liabilities"),
    ("us-gaap", "StockholdersEquity",                "stockholders_equity"),
    ("us-gaap", "OperatingIncomeLoss",               "operating_income"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue", "cash"),
    ("dei",     "EntityCommonStockSharesOutstanding", "shares_outstanding"),
]


def run_edgar_facts_etl(tickers) -> int:
    """
    Fetch XBRL financial facts for each ticker and store in edgar_facts.
    Only annual (10-K) and quarterly (10-Q) frames are stored.
    """
    total   = 0
    symbols = [_symbol(t) for t in tickers]
    cik_map = _build_cik_map(symbols)

    with get_connection() as conn:
        for ticker in symbols:
            cik = cik_map.get(ticker.upper())
            if not cik:
                continue

            data = _get(f"{_BASE}/api/xbrl/companyfacts/CIK{cik}.json")
            if data is None:
                continue

            facts_root = data.get("facts", {})
            rows_written = 0

            for taxonomy, concept, label in _CONCEPTS:
                concept_data = (
                    facts_root
                    .get(taxonomy, {})
                    .get(concept, {})
                    .get("units", {})
                )
                for unit, entries in concept_data.items():
                    for entry in entries:
                        form = entry.get("form", "")
                        if form not in ("10-K", "10-Q"):
                            continue
                        conn.execute("""
                            INSERT OR IGNORE INTO edgar_facts
                                (ticker, cik, taxonomy, concept, label,
                                 unit, value, period_start, period_end,
                                 form_type, filed_date, accession_number)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            ticker, cik, taxonomy, concept, label,
                            unit,
                            entry.get("val"),
                            entry.get("start"),
                            entry.get("end"),
                            form,
                            entry.get("filed"),
                            entry.get("accn"),
                        ))
                        rows_written += 1

            conn.commit()
            total += rows_written
            logger.debug(f"EDGAR facts {ticker}: {rows_written} fact entries")
            time.sleep(_DELAY)

    logger.info(f"EDGAR facts ETL complete: {total} entries across {len(tickers)} tickers")
    return total


# ── Helpers ───────────────────────────────────────────────────────────────────

_cik_cache: Optional[Dict[str, str]] = None   # ticker.upper() → zero-padded CIK


def _build_cik_map(tickers: List[str]) -> Dict[str, str]:
    """Download the SEC company_tickers.json once per process and cache it."""
    global _cik_cache
    if _cik_cache is None:
        data = _get("https://www.sec.gov/files/company_tickers.json")
        _cik_cache = {}
        if data:
            for entry in data.values():
                t = entry.get("ticker", "").upper()
                _cik_cache[t] = str(entry["cik_str"]).zfill(10)
    want = {t.upper() for t in tickers}
    missing = want - set(_cik_cache.keys())
    if missing:
        logger.warning(f"EDGAR: CIK not found for: {', '.join(sorted(missing))}")
    return {t: _cik_cache[t] for t in want if t in _cik_cache}


def _get(url: str, retries: int = 3) -> Optional[dict]:
    for attempt in range(retries):
        resp = None
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = resp.status_code if resp is not None else 0
            if status == 429:
                wait = 2 ** attempt
                logger.warning(f"EDGAR rate-limited, retrying in {wait}s…")
                time.sleep(wait)
            else:
                logger.warning(f"EDGAR HTTP {status} for {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"EDGAR request failed for {url}: {e}")
            return None
    return None
