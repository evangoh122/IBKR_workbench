"""
etl/silver_option_greeks.py
Silver-layer per-contract daily option greeks (Black-Scholes-Merton).

Source : polygon_option_bars pob  (option daily bar = the market price)
         JOIN polygon_bars ub ON ub.ticker = pob.underlying
                             AND ub.ts     = pob.ts
                             AND ub.timespan = 'day'   (underlying daily close, S)
Target : silver_option_greeks                                          (Silver)
Grain  : one row per (option_ticker, trade_date), rebuilt INSERT OR REPLACE.

Method — Black-Scholes-Merton with continuous dividend yield q and risk-free r:
  1. Implied vol σ is solved from the option's MARKET price (option_close) by a
     vectorised Newton iteration seeded at σ0=0.5 and stepped with vega. σ is
     clipped to [1e-4, 5] each step; a fixed number of iterations is run, then
     non-converged / out-of-bound / non-solvable rows are masked to NaN.
  2. Greeks are computed analytically from (S, K, T, r, q, σ) — fully vectorised
     with numpy + scipy.stats.norm (no per-row Python loop over contracts).

Rows filtered out before solving (IV undefined): T<=0, option_close<=0,
und_close<=0, strike<=0, and price below intrinsic value. These become NaN → the
target stores NULL greeks.

Until the options bronze layer lands (us_options_opra entitlement), the joined
source is empty; the module logs an ok / 0-row run and returns gracefully.

Usage:
    python -m etl.silver_option_greeks
    python -m etl.silver_option_greeks --risk-free 0.045 --dividend-yield 0.0
    python -m etl.silver_option_greeks --universe config/semi_universe.json
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from scipy.stats import norm

from db.database import get_connection, init_db

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

# Repo root = parent of the etl/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"

# Vectorised Newton solver configuration
_SIGMA_SEED = 0.5
_SIGMA_MIN = 1e-4
_SIGMA_MAX = 5.0
_NEWTON_ITERS = 100
_PRICE_TOL = 1e-4  # absolute + relative price tolerance for convergence

try:
    from etl.utils import utcnow as _utcnow
except ImportError:  # pragma: no cover - utils always present, defensive only
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()


def load_universe(universe_path: str):
    """Load the list of ticker symbols from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return list(cfg.get("tickers", []))


def _log_etl_run(run_type: str, status: str, message: str,
                 rows_written: int, started_at: str, finished_at: str):
    """Write a single row into etl_runs."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO etl_runs
                (run_type, status, message, rows_written, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (run_type, status, message, rows_written, started_at, finished_at))
        conn.commit()


# ── Vectorised Black-Scholes-Merton primitives ────────────────────────────────
def _d1_d2(S, K, T, r, q, sigma):
    """d1, d2 for BSM with continuous dividend yield q. Arrays in, arrays out."""
    sqrtT = np.sqrt(T)
    vol_sqrtT = sigma * sqrtT
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / vol_sqrtT
    d2 = d1 - vol_sqrtT
    return d1, d2


def _bsm_price(S, K, T, r, q, sigma, is_call):
    """BSM option price (call/put selected element-wise by is_call)."""
    d1, d2 = _d1_d2(S, K, T, r, q, sigma)
    disc_q = np.exp(-q * T)
    disc_r = np.exp(-r * T)
    call = S * disc_q * norm.cdf(d1) - K * disc_r * norm.cdf(d2)
    put = K * disc_r * norm.cdf(-d2) - S * disc_q * norm.cdf(-d1)
    return np.where(is_call, call, put)


def _vega_raw(S, T, q, d1):
    """Vega per unit vol (∂V/∂σ). Also the Newton derivative for IV solving."""
    return S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T)


def _solve_iv(S, K, T, r, q, market, is_call):
    """
    Vectorised Newton solve of implied vol from the market price.

    Returns σ (NaN where the solve failed / was masked). Newton step is
    σ ← σ - (price(σ) - market) / vega(σ); σ clipped to [1e-4, 5] each step.
    A fixed iteration count is used (no per-row brentq), then rows whose final
    price misses `market` beyond tolerance, or that sit at the σ bounds, are NaN.
    """
    sigma = np.full_like(S, _SIGMA_SEED, dtype=float)
    with np.errstate(all="ignore"):
        for _ in range(_NEWTON_ITERS):
            d1, _ = _d1_d2(S, K, T, r, q, sigma)
            price = _bsm_price(S, K, T, r, q, sigma, is_call)
            vega = _vega_raw(S, T, q, d1)
            # guard: tiny/zero vega → skip update (NaN step masks later)
            step = np.where(vega > 1e-12, (price - market) / vega, np.nan)
            sigma = np.clip(sigma - step, _SIGMA_MIN, _SIGMA_MAX)

        final_price = _bsm_price(S, K, T, r, q, sigma, is_call)
        converged = np.abs(final_price - market) <= (_PRICE_TOL + _PRICE_TOL * np.abs(market))
        at_bounds = (sigma <= _SIGMA_MIN * 1.0001) | (sigma >= _SIGMA_MAX * 0.9999)

    sigma = np.where(converged & ~at_bounds & np.isfinite(sigma), sigma, np.nan)
    return sigma


def _compute_greeks(df: pd.DataFrame, r: float, q: float) -> pd.DataFrame:
    """
    Given the joined option/underlying rows, solve IV and compute the greeks.

    Adds trade_date, time_to_expiry, moneyness, risk_free_rate, dividend_yield,
    implied_vol, delta, gamma, theta (per calendar day), vega (per vol point),
    rho (per 1% rate). Invalid / non-solvable rows carry NaN greeks (→ NULL).
    """
    # ts is tz-aware (…+00:00); expiry is tz-naive. Normalise both to naive dates
    # so the subtraction is valid (we only need whole-day differences).
    trade_date = pd.to_datetime(df["ts"], utc=True).dt.tz_localize(None).dt.normalize()
    expiry_date = pd.to_datetime(df["expiry"], errors="coerce").dt.normalize()
    T = (expiry_date - trade_date).dt.days.to_numpy(dtype=float) / 365.0

    S = df["und_close"].to_numpy(dtype=float)
    K = df["strike"].to_numpy(dtype=float)
    market = df["option_close"].to_numpy(dtype=float)
    # Accept both 'call'/'put' (bronze) and 'C'/'P' defensively.
    is_call = df["right"].astype(str).str.lower().str.startswith("c").to_numpy()

    disc_q = np.exp(-q * T)
    disc_r = np.exp(-r * T)

    # Validity mask: IV is only solvable for live, positively-priced contracts
    # whose price is at or above intrinsic value.
    with np.errstate(all="ignore"):
        intrinsic = np.where(
            is_call,
            np.maximum(S * disc_q - K * disc_r, 0.0),
            np.maximum(K * disc_r - S * disc_q, 0.0),
        )
        valid = (
            np.isfinite(T) & (T > 0)
            & (market > 0) & (S > 0) & (K > 0)
            & (market >= intrinsic - _PRICE_TOL)
        )

    # Solve IV only on valid rows; invalid rows are forced NaN.
    S_v = np.where(valid, S, np.nan)
    K_v = np.where(valid, K, np.nan)
    T_v = np.where(valid, T, np.nan)
    mkt_v = np.where(valid, market, np.nan)

    sigma = _solve_iv(S_v, K_v, T_v, r, q, mkt_v, is_call)
    sigma = np.where(valid, sigma, np.nan)

    # Analytic greeks at the solved sigma (NaN sigma → NaN greeks).
    with np.errstate(all="ignore"):
        sqrtT = np.sqrt(T_v)
        d1, d2 = _d1_d2(S_v, K_v, T_v, r, q, sigma)
        disc_q_v = np.exp(-q * T_v)
        disc_r_v = np.exp(-r * T_v)
        pdf_d1 = norm.pdf(d1)

        delta = np.where(is_call,
                         disc_q_v * norm.cdf(d1),
                         disc_q_v * (norm.cdf(d1) - 1.0))
        gamma = disc_q_v * pdf_d1 / (S_v * sigma * sqrtT)
        vega = S_v * disc_q_v * pdf_d1 * sqrtT / 100.0  # per 1 vol point

        theta_common = -(S_v * disc_q_v * pdf_d1 * sigma) / (2.0 * sqrtT)
        theta_call = (theta_common
                      - r * K_v * disc_r_v * norm.cdf(d2)
                      + q * S_v * disc_q_v * norm.cdf(d1))
        theta_put = (theta_common
                     + r * K_v * disc_r_v * norm.cdf(-d2)
                     - q * S_v * disc_q_v * norm.cdf(-d1))
        theta = np.where(is_call, theta_call, theta_put) / 365.0  # per calendar day

        rho = np.where(is_call,
                       K_v * T_v * disc_r_v * norm.cdf(d2) / 100.0,
                       -K_v * T_v * disc_r_v * norm.cdf(-d2) / 100.0)  # per 1% rate

        moneyness = np.where((K > 0), S / K, np.nan)

    out = pd.DataFrame({
        "option_ticker": df["option_ticker"].to_numpy(),
        "underlying": df["underlying"].to_numpy(),
        "expiry": df["expiry"].to_numpy(),
        "strike": K,
        "right": df["right"].to_numpy(),
        "ts": df["ts"].to_numpy(),
        "trade_date": trade_date.dt.date.to_numpy(),
        "option_close": market,
        "und_close": S,
        "time_to_expiry": T,
        "moneyness": moneyness,
        "risk_free_rate": np.full_like(S, r, dtype=float),
        "dividend_yield": np.full_like(S, q, dtype=float),
        "implied_vol": sigma,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "rho": rho,
    })
    # inf (from masked/degenerate rows) → NaN so DuckDB stores NULL
    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Silver-layer per-contract daily option greeks (BSM)."
    )
    parser.add_argument("--risk-free", type=float, default=0.045,
                        help="Continuous risk-free rate r (default: 0.045)")
    parser.add_argument("--dividend-yield", type=float, default=0.0,
                        help="Continuous dividend yield q (default: 0.0)")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    parser.add_argument("--batch-underlying", action="store_true",
                        help="(reserved) process one underlying at a time to cap "
                             "memory; the vectorised path handles the full set at "
                             "once by default")
    args = parser.parse_args()

    init_db()

    r = args.risk_free
    q = args.dividend_yield
    tickers = load_universe(args.universe)
    logger.info(f"Loaded {len(tickers)} tickers from {args.universe} (r={r}, q={q})")
    started_at = _utcnow()

    if not tickers:
        finished_at = _utcnow()
        message = f"No tickers in universe={args.universe}; nothing to build"
        logger.info(message)
        _log_etl_run("silver_option_greeks", "ok", message, 0, started_at, finished_at)
        print(f"[silver_option_greeks] {message}")
        return

    placeholders = ", ".join(["?"] * len(tickers))
    select_sql = f"""
        SELECT
            pob.option_ticker,
            pob.underlying,
            pob.expiry,
            CAST(pob.strike AS DOUBLE)     AS strike,
            pob."right"                    AS "right",
            pob.ts,
            CAST(pob.close AS DOUBLE)      AS option_close,
            CAST(ub.close  AS DOUBLE)      AS und_close
        FROM polygon_option_bars pob
        JOIN polygon_bars ub
          ON ub.ticker = pob.underlying
         AND ub.ts     = pob.ts
         AND ub.timespan = 'day'
        WHERE pob.timespan = 'day'
          AND pob.underlying IN ({placeholders})
    """

    try:
        with get_connection() as conn:
            df = conn.execute(select_sql, tickers).fetch_df()

            if df.empty:
                finished_at = _utcnow()
                message = (
                    f"No option bars for universe={args.universe} "
                    f"(options bronze not yet landed); 0 rows"
                )
                logger.info(message)
                _log_etl_run("silver_option_greeks", "ok", message, 0,
                             started_at, finished_at)
                print(f"[silver_option_greeks] {message}")
                return

            logger.info(f"Solving BSM greeks for {len(df):,} option-day rows")
            out = _compute_greeks(df, r, q)

            conn.register("_silver_option_greeks_df", out)
            conn.execute("""
                INSERT OR REPLACE INTO silver_option_greeks
                    (option_ticker, underlying, expiry, strike, "right", ts,
                     trade_date, option_close, und_close, time_to_expiry,
                     moneyness, risk_free_rate, dividend_yield, implied_vol,
                     delta, gamma, theta, vega, rho)
                SELECT
                    option_ticker, underlying, expiry, strike, "right", ts,
                    trade_date, option_close, und_close, time_to_expiry,
                    moneyness, risk_free_rate, dividend_yield, implied_vol,
                    delta, gamma, theta, vega, rho
                FROM _silver_option_greeks_df
            """)
            conn.unregister("_silver_option_greeks_df")

            rows = conn.execute(
                f"SELECT COUNT(*) FROM silver_option_greeks WHERE underlying IN ({placeholders})",
                tickers,
            ).fetchone()[0]
            solved = int(out["implied_vol"].notna().sum())
            conn.commit()

        finished_at = _utcnow()
        message = (
            f"{rows} contract-day rows ({solved} with solved IV) across "
            f"{len(tickers)} underlyings; r={r}, q={q}"
        )
        _log_etl_run("silver_option_greeks", "ok", message, rows,
                     started_at, finished_at)
        summary = (
            f"[silver_option_greeks] OK — {rows:,} rows "
            f"({solved:,} IV-solved), r={r}, q={q}"
        )
        logger.info(summary)
        print(summary)
    except Exception as e:
        finished_at = _utcnow()
        message = f"FAILED: {e} | universe={args.universe}, r={r}, q={q}"
        _log_etl_run("silver_option_greeks", "error", message, 0,
                     started_at, finished_at)
        logger.error(message)
        print(f"[silver_option_greeks] ERROR — {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
