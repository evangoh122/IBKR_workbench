"""
backtest/oos_metrics.py
Aggregate metrics for walk-forward out-of-sample (OOS) results.

The combined Sharpe here is computed on the CONCATENATED OOS daily return
stream (all folds glued together), not on the mean of per-fold Sharpes.
Averaging fold Sharpes over-weights quiet folds and under-weights volatile
ones; the concatenated stream is what an investor holding the strategy
through every OOS window would actually have experienced.

Each fold's NAV curve starts fresh at initial_capital, so daily returns are
computed WITHIN each fold and the artificial jump at a fold boundary is
never treated as a return.

Annualization matches backtest/metrics.py: ann_return = mean * 252,
ann_vol = std * sqrt(252).
"""
import math

import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def compute_combined_sharpe(
    oos_equity: pd.DataFrame,
    risk_free_rate: float = 0.05,
) -> float | None:
    """
    Compute Sharpe on concatenated OOS daily returns (not mean of fold Sharpes).

    `oos_equity` must have columns: date, nav, fold_id. Returns None when
    there are not enough observations or volatility is zero.
    """
    if oos_equity is None or oos_equity.empty:
        return None

    fold_returns = []
    for _, fold in oos_equity.groupby("fold_id", sort=True):
        nav = fold.sort_values("date")["nav"].astype(float)
        fold_returns.append(nav.pct_change().dropna())

    daily_ret = pd.concat(fold_returns) if fold_returns else pd.Series(dtype=float)
    if daily_ret.empty:
        return None

    ann_return = float(daily_ret.mean() * TRADING_DAYS_PER_YEAR)
    ann_vol = float(daily_ret.std() * math.sqrt(TRADING_DAYS_PER_YEAR))
    if not ann_vol or math.isnan(ann_vol):
        return None
    return (ann_return - risk_free_rate) / ann_vol


def consistency_ratio(fold_sharpes: list[float]) -> float | None:
    """
    Fraction of folds with Sharpe > 0. Folds whose Sharpe is None/NaN (e.g.
    no trades) count against consistency — they are in the denominator but
    never the numerator. Returns None when there are no folds at all.
    """
    if not fold_sharpes:
        return None
    positive = sum(1 for s in fold_sharpes if pd.notna(s) and s > 0)
    return positive / len(fold_sharpes)


def fold_summary_table(fold_metrics: pd.DataFrame) -> str:
    """Pretty-print fold table for logging."""
    if fold_metrics is None or fold_metrics.empty:
        return "(no folds)"

    columns = [
        c for c in
        ("fold_id", "oos_start", "oos_end", "sharpe", "sortino", "mdd",
         "calmar", "win_rate", "n_trades")
        if c in fold_metrics.columns
    ]

    def fmt(value) -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return "-"
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value)

    rows = [[fmt(row[c]) for c in columns] for _, row in fold_metrics.iterrows()]
    widths = [
        max(len(col), *(len(r[i]) for r in rows))
        for i, col in enumerate(columns)
    ]
    header = "  ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    rule = "-" * len(header)
    body = "\n".join(
        "  ".join(cell.rjust(widths[i]) for i, cell in enumerate(r)) for r in rows
    )
    return f"{header}\n{rule}\n{body}"
