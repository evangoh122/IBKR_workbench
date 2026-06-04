"""
etl/extract_cot.py
Commitments of Traders (COT) data connector using the CFTC Socrata API.
Pulls Legacy (Futures Only) reports.
"""
import requests
from loguru import logger
from typing import List, Optional
from db.database import get_connection

_BASE_URL = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

# Mapping of IBKR Ticker -> CFTC market_and_exchange_names
# COT names are usually ALL CAPS.
_TICKER_MAP = {
    # Equity Indices
    "ES":  "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "NQ":  "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
    "RTY": "RUSSELL 2000 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "YM":  "DJIA Consolidated - CHICAGO BOARD OF TRADE",
    "VIX": "VIX FUTURES - CBOE FUTURES EXCHANGE",
    
    # FX
    "6E":  "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "6B":  "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE",
    "6J":  "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "6C":  "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "6A":  "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "6S":  "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "6N":  "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    
    # Rates
    "ZB":  "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE",
    "ZN":  "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "ZF":  "5-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "ZT":  "2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "ZQ":  "30-DAY FEDERAL FUNDS - CHICAGO BOARD OF TRADE",
    
    # Metals
    "GC":  "GOLD - COMMODITY EXCHANGE INC.",
    "SI":  "SILVER - COMMODITY EXCHANGE INC.",
    "HG":  "COPPER - COMMODITY EXCHANGE INC.",
    "PL":  "PLATINUM - NEW YORK MERCANTILE EXCHANGE",
    "PA":  "PALLADIUM - NEW YORK MERCANTILE EXCHANGE",
    
    # Energy
    "CL":  "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "NG":  "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
    "RB":  "GASOLINE RBOB - NEW YORK MERCANTILE EXCHANGE",
    "HO":  "NY HARBOR ULSD - NEW YORK MERCANTILE EXCHANGE",
    
    # Agriculture
    "ZC":  "CORN - CHICAGO BOARD OF TRADE",
    "ZS":  "SOYBEANS - CHICAGO BOARD OF TRADE",
    "ZW":  "WHEAT - CHICAGO BOARD OF TRADE",
    "ZL":  "SOYBEAN OIL - CHICAGO BOARD OF TRADE",
    "ZM":  "SOYBEAN MEAL - CHICAGO BOARD OF TRADE",
    "KE":  "WHEAT - KANSAS CITY BOARD OF TRADE",
    "ZO":  "OATS - CHICAGO BOARD OF TRADE",
    "ZR":  "ROUGH RICE - CHICAGO BOARD OF TRADE",
    
    # Crypto
    "BTC": "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
    "ETH": "ETHER - CHICAGO MERCANTILE EXCHANGE",
}

def run_cot_etl(limit: int = 2000) -> int:
    """
    Fetch latest COT reports and store in cot_reports table.
    Defaults to 2000 rows to cover most recent weeks for all markets.
    """
    # Inverse map for easy lookup
    market_to_ticker = {v.upper(): k for k, v in _TICKER_MAP.items()}
    
    params = {
        "$limit": limit,
        "$order": "report_date_as_yyyy_mm_dd DESC",
    }
    
    logger.info(f"Fetching COT data from {_BASE_URL}...")
    try:
        resp = requests.get(_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch COT data: {e}")
        return 0

    rows = []
    for entry in data:
        market = entry.get("market_and_exchange_names", "").strip().upper()
        ticker = market_to_ticker.get(market)
        
        # We store the row regardless of whether we have a ticker mapping,
        # but the mapping helps with joined queries.
        report_date = entry.get("report_date_as_yyyy_mm_dd")
        if not report_date:
            continue
            
        # Socrata dates can be ISO timestamps or just YYYY-MM-DD
        if "T" in report_date:
            report_date = report_date.split("T")[0]

        rows.append({
            "market_name":     market,
            "ticker":          ticker,
            "report_date":     report_date,
            "noncomm_long":    _to_int(entry.get("noncomm_positions_long_all")),
            "noncomm_short":   _to_int(entry.get("noncomm_positions_short_all")),
            "comm_long":       _to_int(entry.get("comm_positions_long_all")),
            "comm_short":      _to_int(entry.get("comm_positions_short_all")),
            "total_long":      _to_int(entry.get("tot_rept_positions_long_all")),
            "total_short":     _to_int(entry.get("tot_rept_positions_short")),
            "noncomm_spreads": _to_int(entry.get("noncomm_postions_spread_all")),
            "open_interest":   _to_int(entry.get("open_interest_all")),
        })

    if not rows:
        logger.warning("No COT rows found in API response")
        return 0

    conn = get_connection()
    try:
        # Use INSERT OR IGNORE to handle duplicates (market_name + report_date UNIQUE constraint)
        conn.executemany("""
            INSERT OR IGNORE INTO cot_reports
                (market_name, ticker, report_date, noncomm_long, noncomm_short,
                 comm_long, comm_short, total_long, total_short, noncomm_spreads, open_interest)
            VALUES
                ($market_name, $ticker, $report_date, $noncomm_long, $noncomm_short,
                 $comm_long, $comm_short, $total_long, $total_short, $noncomm_spreads, $open_interest)
        """, rows)
        conn.commit()
    finally:
        conn.close()

    logger.info(f"Processed {len(rows)} COT report rows")
    return len(rows)

def _to_int(val: Optional[str]) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        # Remove commas if any and handle float strings
        clean_val = str(val).replace(",", "")
        return int(float(clean_val))
    except:
        return None
