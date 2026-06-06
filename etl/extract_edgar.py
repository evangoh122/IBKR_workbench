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
import os
import time
from typing import Dict, List, Optional

import requests
from loguru import logger

from db.database import get_connection

# SEC requires a descriptive User-Agent: "Name email@domain.com"
_EMAIL   = os.getenv("EDGAR_EMAIL", "")
if not _EMAIL:
    _EMAIL = "research@example.com"
    logger.warning(
        "EDGAR_EMAIL not set in .env — using placeholder. "
        "SEC may throttle requests. Set EDGAR_EMAIL to your real email."
    )
_HEADERS = {
    "User-Agent": f"IBKR-Workbench {_EMAIL}",
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


def _stk_only(tickers) -> list:
    """Filter to equities only — EDGAR has no filings for forex, futures, or indices."""
    return [t for t in tickers
            if not isinstance(t, dict) or t.get("secType", "STK") == "STK"]


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

    symbols  = [_symbol(t) for t in _stk_only(tickers)]
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

    logger.info(f"EDGAR filings ETL complete: {total} filings across {len(symbols)} tickers")
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
    symbols = [_symbol(t) for t in _stk_only(tickers)]
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

    logger.info(f"EDGAR facts ETL complete: {total} entries across {len(symbols)} tickers")
    return total


# ── 13-F Institutional Holdings ──────────────────────────────────────────────

_EFTS_SEARCH = "https://efts.sec.gov/LATEST/search-index"


def run_edgar_13f_etl(tickers, lookback_quarters: int = 8) -> int:
    """
    Fetch 13-F-HR institutional holdings for each ticker.
    Uses EDGAR EFTS full-text search to find all institutional filers
    that held the stock, then parses the InfoTable XML for share counts.
    lookback_quarters: how many recent quarters to fetch (default 8 = 2 years).
    """
    from datetime import date, timedelta

    symbols = [_symbol(t) for t in _stk_only(tickers)]
    cik_map = _build_cik_map(symbols)
    total   = 0

    # Date cutoff
    cutoff = (date.today() - timedelta(days=lookback_quarters * 92)).isoformat()

    with get_connection() as conn:
        for ticker in symbols:
            cik = cik_map.get(ticker.upper())
            if not cik:
                continue

            # Search EFTS for 13-F-HR filings mentioning this ticker's CIK
            results = _get(f"{_EFTS_SEARCH}?q=%22{cik}%22&forms=13F-HR&dateRange=custom&startdt={cutoff}&hits.hits._source=period_of_report,entity_name,file_date,accession_no")
            if not results:
                time.sleep(_DELAY)
                continue

            hits = results.get("hits", {}).get("hits", [])
            rows_written = 0

            for hit in hits:
                src            = hit.get("_source", {})
                filer_name     = src.get("entity_name", "")
                filed_date     = src.get("file_date", "")
                period         = src.get("period_of_report", "")
                accession      = src.get("accession_no", "").replace("-", "")
                filer_cik_raw  = hit.get("_id", "").split(":")[0] if ":" in hit.get("_id", "") else ""

                if not accession or not period:
                    continue

                # Try to get holdings detail from the filing index
                shares, value, discretion, put_call = None, None, None, None
                infotable_url = _find_infotable_url(accession, filer_cik_raw or accession[:10])
                if infotable_url:
                    xml_data = _get_raw(infotable_url)
                    if xml_data:
                        shares, value, discretion, put_call = _parse_infotable(xml_data)

                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO edgar_13f
                            (filer_cik, filer_name, ticker, period_of_report,
                             filed_date, accession_number, shares, value,
                             investment_discretion, put_call)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        filer_cik_raw, filer_name, ticker, period,
                        filed_date, accession, shares, value, discretion, put_call,
                    ))
                    rows_written += 1
                except Exception as e:
                    logger.debug(f"13-F insert skip {ticker}/{filer_name}: {e}")

            conn.commit()
            total += rows_written
            logger.debug(f"EDGAR 13-F {ticker}: {rows_written} institutional holders stored")
            time.sleep(_DELAY)

    logger.info(f"EDGAR 13-F ETL complete: {total} holdings across {len(symbols)} tickers")
    return total


def _quarter(date_str: str) -> str:
    month = int(date_str[5:7])
    return str((month - 1) // 3 + 1)


def _find_infotable_url(accession: str, filer_cik: str) -> Optional[str]:
    """Look up the InfoTable document URL from the filing index."""
    cik_padded = filer_cik.zfill(10)
    data = _get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json")
    if not data:
        return None
    # Find the InfoTable document in recent filings
    recent = data.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    docs       = recent.get("primaryDocument", [])
    for acc, doc in zip(accessions, docs):
        if acc.replace("-", "") == accession:
            return f"https://www.sec.gov/Archives/edgar/data/{int(cik_padded)}/{accession}/{doc}"
    return None


def _get_raw(url: str) -> Optional[str]:
    """Fetch raw text content (for XML parsing)."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


def _parse_infotable(xml_text: str) -> tuple:
    """Parse 13-F InfoTable XML, return (shares, value_thousands, discretion, put_call)."""
    import xml.etree.ElementTree as ET
    try:
        # Strip namespaces for simpler parsing
        xml_clean = xml_text
        for ns in ["ns1:", "ns2:", "com:", "n1:", "n2:"]:
            xml_clean = xml_clean.replace(ns, "")
        root = ET.fromstring(xml_clean)

        total_shares = 0
        total_value  = 0
        discretion   = None
        put_call     = None

        for entry in root.iter("infoTable"):
            try:
                sh  = int(entry.findtext("sshPrnamt") or 0)
                val = int(entry.findtext("value") or 0)
                total_shares += sh
                total_value  += val
                if not discretion:
                    discretion = entry.findtext("investmentDiscretion")
                if not put_call:
                    put_call = entry.findtext("putCall")
            except (ValueError, TypeError):
                continue

        return (total_shares or None, total_value or None, discretion, put_call)
    except Exception:
        return (None, None, None, None)


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
