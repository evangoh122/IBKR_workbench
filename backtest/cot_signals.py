"""
backtest/cot_signals.py
COT-based signal generators for mid-frequency strategy.

Timing discipline:
  - COT data: Tuesday snapshot, published Friday after close
  - signal_date = the Friday the data became available
  - fill_date = following Monday (T+3 from Tuesday, executed at Monday open)
  - Holding period: 1-4 weeks (5-20 trading days)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import duckdb

logger = logging.getLogger(__name__)

ZSCORE_EXTREME = 2.0        # |z| >= this -> extreme crowd position
DIVERGENCE_THRESHOLD = 3.0  # spec_comm_divergence abs threshold

# COT market ticker -> futures ticker used for the price-confluence check
COT_TO_FUTURES = {"ES": "ES1:COM", "NQ": "NQ1:COM", "RTY": "RTY1:COM"}


@dataclass
class COTSignal:
    ticker: str          # COT market ticker (ES, NQ, RTY, VIX)
    signal_date: date    # Friday COT was published
    fill_date: date      # Following Monday - execution date
    direction: str       # 'long' or 'short'
    strength: float      # abs(net_pos_zscore_52w) or abs(divergence)
    signal_type: str     # 'cot_crowd_fade' | 'cot_confluence' | 'cot_comm_divergence'
    net_pos_zscore: float | None
    crowd_flag: str | None
    spec_comm_divergence: float | None


def _next_monday(d: date) -> date:
    """Return the Monday on or after date d."""
    days_ahead = (7 - d.weekday()) % 7  # Monday = 0
    if days_ahead == 0:
        days_ahead = 7
    return d + timedelta(days=days_ahead)


def _to_date(value) -> date:
    return value.date() if hasattr(value, "date") else value


def _clean_float(value) -> float | None:
    """Convert a dict-record value to float, treating None/NaN as None."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # f != f only for NaN


def generate_cot_signals(
    conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    zscore_threshold: float = ZSCORE_EXTREME,
) -> list[COTSignal]:
    """
    Signal A: COT Crowd Fade.
    Fade large speculator extremes (|net_pos_zscore_52w| >= zscore_threshold).
    LONG when specs are extreme_short (mean-reversion logic).
    SHORT when specs are extreme_long.
    """
    df = conn.execute("""
        SELECT
            report_date,
            ticker,
            net_pos_zscore_52w,
            crowd_flag,
            spec_comm_divergence
        FROM silver_cot_features
        WHERE report_date BETWEEN ? AND ?
          AND crowd_flag IN ('extreme_long', 'extreme_short')
          AND net_pos_zscore_52w IS NOT NULL
          AND ABS(net_pos_zscore_52w) >= ?
        ORDER BY ticker, report_date
    """, [start_date, end_date, zscore_threshold]).df()

    signals: list[COTSignal] = []
    for row in df.to_dict("records"):
        rdate = _to_date(row["report_date"])
        # COT published Friday after the Tuesday report_date
        days_to_friday = (4 - rdate.weekday()) % 7
        friday = rdate + timedelta(days=days_to_friday)
        fill = _next_monday(friday)

        if friday < start_date or friday > end_date:
            continue

        crowd_flag = str(row["crowd_flag"])
        direction = "long" if crowd_flag == "extreme_short" else "short"
        signals.append(COTSignal(
            ticker=str(row["ticker"]),
            signal_date=friday,
            fill_date=fill,
            direction=direction,
            strength=abs(float(row["net_pos_zscore_52w"])),
            signal_type="cot_crowd_fade",
            net_pos_zscore=float(row["net_pos_zscore_52w"]),
            crowd_flag=crowd_flag,
            spec_comm_divergence=_clean_float(row["spec_comm_divergence"]),
        ))

    logger.info("cot_crowd_fade: %d signals in [%s, %s]", len(signals), start_date, end_date)
    return signals


def generate_cot_confluence_signals(
    conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
) -> list[COTSignal]:
    """
    Signal B: COT + Price Confluence (LONG only for now).
    Requires: COT crowd_flag = extreme_short AND the mapped futures market's
    zscore_ret_20 <= -2.0 (oversold at the index level).
    Maps ES->ES1:COM, NQ->NQ1:COM, RTY->RTY1:COM in silver_futures_features,
    using the most recent futures row on or before the COT report_date
    (VIX has no equity proxy and is excluded from this signal).
    """
    df = conn.execute("""
        SELECT
            c.report_date,
            c.ticker        AS cot_ticker,
            c.net_pos_zscore_52w,
            c.crowd_flag,
            c.spec_comm_divergence,
            f.ticker        AS fut_ticker,
            f.zscore_ret_20
        FROM silver_cot_features c
        JOIN (
            SELECT ticker, trade_date, zscore_ret_20,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
            FROM silver_futures_features
            WHERE trade_date <= ?  -- report_date upper bound, passed as parameter
        ) f ON f.ticker = CASE c.ticker
            WHEN 'ES'  THEN 'ES1:COM'
            WHEN 'NQ'  THEN 'NQ1:COM'
            WHEN 'RTY' THEN 'RTY1:COM'
            ELSE NULL
        END AND f.rn = 1
        WHERE c.report_date BETWEEN ? AND ?
          AND c.crowd_flag = 'extreme_short'
          AND c.net_pos_zscore_52w IS NOT NULL
          AND f.zscore_ret_20 IS NOT NULL
          AND f.zscore_ret_20 <= -2.0
        ORDER BY c.ticker, c.report_date
    """, [end_date, start_date, end_date]).df()

    signals: list[COTSignal] = []
    for row in df.to_dict("records"):
        rdate = _to_date(row["report_date"])
        days_to_friday = (4 - rdate.weekday()) % 7
        friday = rdate + timedelta(days=days_to_friday)
        fill = _next_monday(friday)
        if friday < start_date or friday > end_date:
            continue
        logger.debug(
            "cot_confluence: %s price check via %s (zscore_ret_20=%.2f)",
            row["cot_ticker"], row["fut_ticker"], float(row["zscore_ret_20"]),
        )
        signals.append(COTSignal(
            ticker=str(row["cot_ticker"]),
            signal_date=friday,
            fill_date=fill,
            direction="long",
            strength=abs(float(row["net_pos_zscore_52w"])),
            signal_type="cot_confluence",
            net_pos_zscore=float(row["net_pos_zscore_52w"]),
            crowd_flag=str(row["crowd_flag"]),
            spec_comm_divergence=_clean_float(row.get("spec_comm_divergence")),
        ))

    logger.info("cot_confluence: %d signals in [%s, %s]", len(signals), start_date, end_date)
    return signals


def generate_comm_divergence_signals(
    conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    divergence_threshold: float = DIVERGENCE_THRESHOLD,
) -> list[COTSignal]:
    """
    Signal C: Commercial Divergence.
    When spec_comm_divergence >= +threshold: specs extremely more bullish than
    commercials -> fade specs (short).
    When spec_comm_divergence <= -threshold: specs extremely more bearish than
    commercials -> fade specs (long).
    """
    df = conn.execute("""
        SELECT
            report_date,
            ticker,
            net_pos_zscore_52w,
            crowd_flag,
            spec_comm_divergence
        FROM silver_cot_features
        WHERE report_date BETWEEN ? AND ?
          AND spec_comm_divergence IS NOT NULL
          AND ABS(spec_comm_divergence) >= ?
        ORDER BY ticker, report_date
    """, [start_date, end_date, divergence_threshold]).df()

    signals: list[COTSignal] = []
    for row in df.to_dict("records"):
        rdate = _to_date(row["report_date"])
        days_to_friday = (4 - rdate.weekday()) % 7
        friday = rdate + timedelta(days=days_to_friday)
        fill = _next_monday(friday)
        if friday < start_date or friday > end_date:
            continue
        div = float(row["spec_comm_divergence"])
        # Fade the specs: if specs >> commercials (div > 0), go short
        direction = "short" if div > 0 else "long"
        crowd_flag = row.get("crowd_flag")
        signals.append(COTSignal(
            ticker=str(row["ticker"]),
            signal_date=friday,
            fill_date=fill,
            direction=direction,
            strength=abs(div),
            signal_type="cot_comm_divergence",
            net_pos_zscore=_clean_float(row["net_pos_zscore_52w"]),
            crowd_flag=str(crowd_flag) if crowd_flag is not None else None,
            spec_comm_divergence=div,
        ))

    logger.info("cot_comm_divergence: %d signals in [%s, %s]", len(signals), start_date, end_date)
    return signals
