"""
etl/ingest_loop.py
──────────────────
Coordinated end-of-day ingestion pipeline for the IBKR Equity Workbench.

Pipeline stages (run in order each cycle):
    1. polygon-bars       – stock OHLCV day bars (Bronze)
    2. polygon-option-bars– option OHLCV day bars (Bronze)
    3. silver-stock       – MA / std / z-score / sigma_flag per ticker (Silver)
    4. silver-options     – BSM implied-vol + greeks per contract (Silver)

After stage 3 the v_zscore_alerts view is automatically up to date.
Any ticker whose close is outside ±3σ on any lookback window will appear
in that view with sigma_flag = '+3σ' or '-3σ'.

Usage
─────
    # One-shot: run the full pipeline once then exit
    python -m etl.ingest_loop

    # Scheduled: run once at startup, then every day at 17:00 ET
    python -m etl.ingest_loop --schedule

    # Override the daily run time (24h HH:MM, local clock)
    python -m etl.ingest_loop --schedule --run-at 16:30

    # Run on a fixed interval (seconds) instead of a daily cron
    python -m etl.ingest_loop --schedule --interval 3600

    # Skip specific stages
    python -m etl.ingest_loop --skip-options   # bars + silver-stock only
    python -m etl.ingest_loop --skip-silver     # bronze only

    # Dry run: print what would execute, don't write
    python -m etl.ingest_loop --dry-run

Environment variables (inherit from .env)
──────────────────────────────────────────
    POLYGON_API_KEY                  Required for stages 1-2
    DB_PATH                          Path to equity.duckdb
    POLYGON_OPTION_BARS_TICKERS      Comma-separated option universe
    POLYGON_BARS_LOOKBACK            Days of history for stock bars (default 7)
    POLYGON_OPTION_BARS_LOOKBACK     Days for option bars (default 30)
    LOG_LEVEL                        DEBUG | INFO | WARNING (default INFO)
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import schedule as _schedule
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

logger.add(
    "logs/ingest_loop_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="14 days",
    level=os.getenv("LOG_LEVEL", "INFO"),
)

_REPO_ROOT = Path(__file__).resolve().parent.parent


# ── Stage helpers ─────────────────────────────────────────────────────────────

def _stage_bars(dry_run: bool = False) -> int:
    """Stage 1 — stock OHLCV day bars (Bronze)."""
    logger.info("── Stage 1: Polygon stock bars ──────────────────────────")
    if dry_run:
        logger.info("[dry-run] would run: python main.py --job polygon-bars")
        return 0
    from polygon import RESTClient
    from config.tickers import get_all_tickers
    from etl.extract_polygon import run_polygon_bars_etl
    from db.database import get_connection

    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        logger.error("POLYGON_API_KEY not set — skipping stage 1")
        return 0

    lookback = int(os.getenv("POLYGON_BARS_LOOKBACK", "7"))
    tickers  = get_all_tickers()
    client   = RESTClient(api_key)
    rows     = run_polygon_bars_etl(client, tickers, timespan="day", lookback_days=lookback)
    logger.info(f"Stage 1 complete: {rows:,} stock bar rows written")
    return rows


def _stage_option_bars(dry_run: bool = False) -> int:
    """Stage 2 — option OHLCV day bars (Bronze)."""
    logger.info("── Stage 2: Polygon option bars ─────────────────────────")
    if dry_run:
        logger.info("[dry-run] would run: python main.py --job polygon-option-bars")
        return 0
    from polygon import RESTClient
    from etl.extract_polygon import run_polygon_option_bars_etl
    from config.tickers import get_all_tickers

    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        logger.error("POLYGON_API_KEY not set — skipping stage 2")
        return 0

    # Option tickers: env override or full universe
    opt_env = os.getenv("POLYGON_OPTION_BARS_TICKERS", "")
    if opt_env:
        opt_tickers = [{"symbol": s.strip()} for s in opt_env.split(",") if s.strip()]
    else:
        opt_tickers = get_all_tickers()

    lookback     = int(os.getenv("POLYGON_OPTION_BARS_LOOKBACK", "30"))
    max_contracts = int(os.getenv("POLYGON_OPTION_BARS_MAX_CONTRACTS", "1000"))
    client       = RESTClient(api_key)
    rows         = run_polygon_option_bars_etl(
        client, opt_tickers,
        lookback_days=lookback,
        max_contracts=max_contracts,
    )
    logger.info(f"Stage 2 complete: {rows:,} option bar rows written")
    return rows


def _stage_silver_stock(universe_path: str = "config/semi_universe.json",
                        dry_run: bool = False) -> int:
    """Stage 3 — silver stock features: MA / std / z-score / sigma_flag."""
    logger.info("── Stage 3: Silver stock features (z-score + sigma_flag) ─")
    if dry_run:
        logger.info("[dry-run] would run: python -m etl.silver_stock_features")
        return 0
    from etl.silver_stock_features import load_universe, _build_sql, _log_etl_run
    from db.database import get_connection, init_db

    init_db()   # ensures sigma_flag columns + view exist
    tickers = load_universe(universe_path)
    if not tickers:
        logger.warning("No tickers in universe — skipping stage 3")
        return 0

    started_at   = datetime.now(timezone.utc).isoformat()
    placeholders = ", ".join(["?"] * len(tickers))
    try:
        with get_connection() as conn:
            conn.execute(
                f"DELETE FROM silver_stock_features WHERE ticker IN ({placeholders})",
                tickers,
            )
            conn.execute(_build_sql(placeholders), tickers)
            rows = conn.execute(
                f"SELECT COUNT(*) FROM silver_stock_features WHERE ticker IN ({placeholders})",
                tickers,
            ).fetchone()[0]
            conn.commit()
        finished_at = datetime.now(timezone.utc).isoformat()
        msg = f"{rows:,} rows across {len(tickers)} tickers"
        _log_etl_run("silver_stock_features", "ok", msg, rows, started_at, finished_at)
        logger.info(f"Stage 3 complete: {msg}")

        # Log current σ-breach summary
        _log_sigma_summary()
        return rows
    except Exception as e:
        finished_at = datetime.now(timezone.utc).isoformat()
        _log_etl_run("silver_stock_features", "error", str(e), 0, started_at, finished_at)
        logger.error(f"Stage 3 FAILED: {e}")
        return 0


def _stage_silver_options(dry_run: bool = False) -> int:
    """Stage 4 — BSM implied-vol + greeks (Silver)."""
    logger.info("── Stage 4: Silver option greeks (BSM IV + Δ Γ Θ V ρ) ───")
    if dry_run:
        logger.info("[dry-run] would run: python -m etl.silver_option_greeks")
        return 0
    try:
        from etl.silver_option_greeks import main as _silver_options_main
        _silver_options_main()
        logger.info("Stage 4 complete")
        return 1
    except Exception as e:
        logger.error(f"Stage 4 FAILED: {e}")
        return 0


def _log_sigma_summary():
    """After stage 3, log which tickers are outside ±3σ on any window."""
    try:
        from db.database import get_connection
        with get_connection() as conn:
            breaches = conn.execute("""
                SELECT ticker, trade_date, close, pct_change,
                       ROUND(zscore_20,      2) AS z20,
                       ROUND(zscore_50,      2) AS z50,
                       ROUND(zscore_100,     2) AS z100,
                       sigma_flag_20, sigma_flag_50, sigma_flag_100,
                       ROUND(zscore_ret_20,  2) AS zr20,
                       ROUND(zscore_ret_50,  2) AS zr50,
                       ROUND(zscore_ret_100, 2) AS zr100,
                       sigma_flag_ret_20, sigma_flag_ret_50, sigma_flag_ret_100
                FROM v_zscore_alerts
                WHERE any_breach = true
                ORDER BY max_abs_zscore DESC
            """).fetchall()

        if not breaches:
            logger.info("σ-scan: no tickers outside ±3σ on any window ✓")
            return

        logger.warning(f"σ-scan: {len(breaches)} ticker(s) outside ±3σ:")
        for row in breaches:
            ticker, date, close, pct_chg, z20, z50, z100, f20, f50, f100, zr20, zr50, zr100, fr20, fr50, fr100 = row
            price_flags = " | ".join(
                f"{w}={f}" for w, f in [("px20d", f20), ("px50d", f50), ("px100d", f100)]
                if f and f != "normal"
            )
            ret_flags = " | ".join(
                f"{w}={f}" for w, f in [("ret20d", fr20), ("ret50d", fr50), ("ret100d", fr100)]
                if f and f != "normal"
            )
            pct_str = f"{pct_chg:+.2f}%" if pct_chg is not None else "n/a"
            logger.warning(
                f"  {ticker:<6} {date}  close=${close:.2f}  chg={pct_str}  "
                f"pxZ=[{z20},{z50},{z100}]  retZ=[{zr20},{zr50},{zr100}]  "
                f"breaches: {price_flags}  {ret_flags}".rstrip()
            )
    except Exception as e:
        logger.warning(f"σ-summary failed: {e}")


# ── Full pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(
    skip_options: bool = False,
    skip_silver:  bool = False,
    universe:     str  = "config/semi_universe.json",
    dry_run:      bool = False,
) -> dict:
    """
    Run all four stages in order. Returns a results dict with row counts.

    Args:
        skip_options: omit stages 2 and 4 (option bronze + silver greeks)
        skip_silver:  omit stages 3 and 4 (silver features + silver greeks)
        universe:     path to the semi_universe.json config
        dry_run:      log what would run without writing anything
    """
    started = time.monotonic()
    ts      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    logger.info(f"╔══ Ingest loop start: {ts} {'[DRY RUN] ' if dry_run else ''}══╗")

    results = {"stock_bars": 0, "option_bars": 0, "silver_stock": 0, "silver_options": 0}

    results["stock_bars"]    = _stage_bars(dry_run)

    if not skip_options:
        results["option_bars"] = _stage_option_bars(dry_run)

    if not skip_silver:
        results["silver_stock"] = _stage_silver_stock(universe, dry_run)

        if not skip_options:
            results["silver_options"] = _stage_silver_options(dry_run)

    elapsed = time.monotonic() - started
    logger.info(
        f"╚══ Ingest loop done in {elapsed:.0f}s — "
        f"stock_bars={results['stock_bars']:,}  "
        f"option_bars={results['option_bars']:,}  "
        f"silver_stock={results['silver_stock']:,} ══╝"
    )
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Coordinated ingest loop: Polygon bars → option bars → Silver features"
    )
    parser.add_argument(
        "--schedule", action="store_true",
        help="Run continuously. Use --run-at or --interval to control timing."
    )
    parser.add_argument(
        "--run-at", default="17:00", metavar="HH:MM",
        help="Daily run time in 24h local clock (default: 17:00). Ignored if --interval is set."
    )
    parser.add_argument(
        "--interval", type=int, default=0, metavar="SECONDS",
        help="Run every N seconds instead of a daily cron. 0 = use --run-at (default)."
    )
    parser.add_argument(
        "--skip-options", action="store_true",
        help="Skip option bar fetch (stage 2) and silver greeks (stage 4)."
    )
    parser.add_argument(
        "--skip-silver", action="store_true",
        help="Skip silver feature rebuild (stages 3-4). Bronze only."
    )
    parser.add_argument(
        "--universe", default="config/semi_universe.json",
        help="Path to ticker universe JSON (default: config/semi_universe.json)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would run without writing to the database."
    )
    args = parser.parse_args()

    kwargs = dict(
        skip_options=args.skip_options,
        skip_silver=args.skip_silver,
        universe=args.universe,
        dry_run=args.dry_run,
    )

    if not args.schedule:
        run_pipeline(**kwargs)
        return

    # ── Scheduled mode ────────────────────────────────────────────
    if args.interval > 0:
        logger.info(
            f"Scheduled mode: running every {args.interval}s (Ctrl-C to stop)"
        )
        run_pipeline(**kwargs)   # run immediately on start
        _schedule.every(args.interval).seconds.do(run_pipeline, **kwargs)
    else:
        logger.info(
            f"Scheduled mode: daily at {args.run_at} local time (Ctrl-C to stop)"
        )
        run_pipeline(**kwargs)   # run immediately on start
        _schedule.every().day.at(args.run_at).do(run_pipeline, **kwargs)

    try:
        while True:
            _schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Ingest loop stopped by user")


if __name__ == "__main__":
    main()
