"""
backtest/walk_forward.py
Walk-forward out-of-sample backtesting framework.

Split types:
  expanding  — IS window grows each fold, OOS steps forward (default)
  rolling    — fixed IS length slides forward

Embargo gap (default 21 days) between IS end and OOS start prevents
signal leakage from positions that straddle the boundary.

No look-ahead by construction:
  - IS/OOS splits are generated purely from dates; no price data touches
    the split logic.
  - Each fold calls BacktestEngine.run() for its OOS window only, so the
    engine never sees data after oos_end (its SQL is bounded by the
    start/end dates, and feature lookups are ASOF strictly-before joins).
  - The IS window is not traded: regime thresholds in backtest/regime.py
    are FIXED constants, so there is no parameter fitting step and IS
    exists purely to document what history was "available" before each
    OOS window.

Usage:
    wf = WalkForwardEngine(
        db_path="equity.duckdb",
        config={...},           # same config dict as BacktestEngine
        n_splits=5,
        embargo_days=21,
        split_type="expanding", # or "rolling"
        is_window_days=None,    # required when split_type="rolling"
    )
    results = wf.run(
        universe=["NVDA", "AMD", "INTC"],
        start_date=date(2020, 1, 1),
        end_date=date(2024, 12, 31),
    )
    # results.oos_metrics  — per-fold metrics DataFrame
    # results.oos_equity   — concatenated OOS equity curve
    # results.summary      — aggregate stats
"""
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from loguru import logger

from .engine import BacktestEngine
from .oos_metrics import (
    compute_combined_sharpe,
    consistency_ratio,
    fold_summary_table,
)

_VALID_SPLIT_TYPES = ("expanding", "rolling")


@dataclass
class WalkForwardResult:
    splits: list[dict]           # each: {fold, is_start, is_end, embargo_end, oos_start, oos_end}
    fold_metrics: pd.DataFrame   # one row per fold: fold_id, sharpe, sortino, mdd, calmar, win_rate, n_trades
    oos_equity: pd.DataFrame     # date, nav, fold_id — concatenated across all OOS windows
    summary: dict                # aggregate: mean_sharpe, consistency_ratio, worst_fold_mdd, combined_sharpe

    @property
    def oos_metrics(self) -> pd.DataFrame:
        """Alias for fold_metrics."""
        return self.fold_metrics


def _coerce_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class WalkForwardEngine:
    def __init__(
        self,
        db_path: str,
        config: dict | None = None,
        n_splits: int = 5,
        embargo_days: int = 21,
        split_type: str = "expanding",
        is_window_days: int | None = None,
    ):
        if split_type not in _VALID_SPLIT_TYPES:
            raise ValueError(
                f"split_type must be one of {_VALID_SPLIT_TYPES}, got {split_type!r}"
            )
        if split_type == "rolling" and not is_window_days:
            raise ValueError("is_window_days is required when split_type='rolling'")
        if n_splits < 1:
            raise ValueError(f"n_splits must be >= 1, got {n_splits}")
        if embargo_days < 0:
            raise ValueError(f"embargo_days must be >= 0, got {embargo_days}")

        self.db_path = db_path
        self.config = config or {}
        self.n_splits = n_splits
        self.embargo_days = embargo_days
        self.split_type = split_type
        self.is_window_days = is_window_days

    # ── Split generation: pure date arithmetic, no price data ─────────────
    def generate_splits(self, start_date: date, end_date: date) -> list[dict]:
        start_date = _coerce_date(start_date)
        end_date = _coerce_date(end_date)
        if end_date <= start_date:
            raise ValueError(f"end_date {end_date} must be after start_date {start_date}")

        total_days = (end_date - start_date).days
        oos_days = total_days // (self.n_splits + 1)  # rough equal OOS slices
        if oos_days < 1:
            raise ValueError(
                f"window of {total_days} days is too short for {self.n_splits} splits"
            )

        splits = []
        for fold in range(self.n_splits):
            if self.split_type == "expanding":
                is_start = start_date
                is_end = start_date + timedelta(days=oos_days * (fold + 1))
            else:  # rolling
                is_end = start_date + timedelta(days=self.is_window_days + oos_days * fold)
                is_start = is_end - timedelta(days=self.is_window_days)

            embargo_end = is_end + timedelta(days=self.embargo_days)
            oos_start = embargo_end + timedelta(days=1)
            oos_end = min(oos_start + timedelta(days=oos_days - 1), end_date)

            if oos_start >= end_date:
                break

            splits.append({
                "fold": fold,
                "is_start": is_start,
                "is_end": is_end,
                "embargo_end": embargo_end,
                "oos_start": oos_start,
                "oos_end": oos_end,
            })
        return splits

    # ── OOS schema (self-contained, mirrors BacktestEngine's approach) ────
    @staticmethod
    def _ensure_oos_schema(conn) -> None:
        try:
            conn.execute("ALTER TABLE gold_backtest_runs ADD COLUMN fold_id INTEGER")
        except Exception:
            pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_oos_summary (
                run_id TEXT NOT NULL,
                run_ts TIMESTAMPTZ DEFAULT current_timestamp,
                n_folds INTEGER,
                mean_sharpe DOUBLE,
                consistency_ratio DOUBLE,
                combined_sharpe DOUBLE,
                worst_fold_mdd DOUBLE,
                split_type TEXT,
                embargo_days INTEGER,
                PRIMARY KEY (run_id)
            )
        """)

    def run(self, universe: list[str], start_date: date, end_date: date) -> WalkForwardResult:
        splits = self.generate_splits(start_date, end_date)
        if not splits:
            raise ValueError("no usable folds for the given date range and n_splits")

        wf_run_id = str(uuid.uuid4())
        risk_free_rate = self.config.get("risk_free_rate", 0.05)

        engine = BacktestEngine(self.db_path, self.config)
        fold_rows = []
        equity_frames = []
        try:
            self._ensure_oos_schema(engine.conn)

            for split in splits:
                fold = split["fold"]
                logger.info(
                    f"Fold {fold}: IS {split['is_start']} -> {split['is_end']} | "
                    f"embargo -> {split['embargo_end']} | "
                    f"OOS {split['oos_start']} -> {split['oos_end']}"
                )
                # OOS only — the engine's queries are bounded by these dates,
                # so nothing after oos_end can influence this fold.
                result = engine.run(
                    universe,
                    split["oos_start"].isoformat(),
                    split["oos_end"].isoformat(),
                )
                engine.conn.execute(
                    "UPDATE gold_backtest_runs SET fold_id = ? WHERE run_id = ?",
                    [fold, result["run_id"]],
                )
                engine.conn.commit()

                m = result["metrics"]
                fold_rows.append({
                    "fold_id": fold,
                    "run_id": result["run_id"],
                    "oos_start": split["oos_start"],
                    "oos_end": split["oos_end"],
                    "sharpe": m["sharpe"],
                    "sortino": m["sortino"],
                    "mdd": m["mdd_pct"],
                    "calmar": m["calmar"],
                    "win_rate": m["win_rate"],
                    "n_trades": result["n_trades"],
                })
                if result["portfolio_history"]:
                    eq = pd.DataFrame(
                        [{"date": s["date"], "nav": s["nav"]} for s in result["portfolio_history"]]
                    )
                    eq["fold_id"] = fold
                    equity_frames.append(eq)

            fold_metrics = pd.DataFrame(fold_rows)
            oos_equity = (
                pd.concat(equity_frames, ignore_index=True)
                if equity_frames
                else pd.DataFrame(columns=["date", "nav", "fold_id"])
            )
            summary = self._aggregate_oos(wf_run_id, fold_metrics, oos_equity, risk_free_rate)

            engine.conn.execute(
                "INSERT INTO gold_oos_summary (run_id, n_folds, mean_sharpe, consistency_ratio, "
                "combined_sharpe, worst_fold_mdd, split_type, embargo_days) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [wf_run_id, summary["n_folds"], summary["mean_sharpe"],
                 summary["consistency_ratio"], summary["combined_sharpe"],
                 summary["worst_fold_mdd"], self.split_type, self.embargo_days],
            )
            engine.conn.commit()
        finally:
            engine.close()

        return WalkForwardResult(
            splits=splits,
            fold_metrics=fold_metrics,
            oos_equity=oos_equity,
            summary=summary,
        )

    def _aggregate_oos(
        self,
        wf_run_id: str,
        fold_metrics: pd.DataFrame,
        oos_equity: pd.DataFrame,
        risk_free_rate: float,
    ) -> dict:
        sharpes = fold_metrics["sharpe"].tolist() if not fold_metrics.empty else []
        valid_sharpes = fold_metrics["sharpe"].dropna() if not fold_metrics.empty else pd.Series(dtype=float)
        valid_mdd = fold_metrics["mdd"].dropna() if not fold_metrics.empty else pd.Series(dtype=float)

        summary = {
            "run_id": wf_run_id,
            "n_folds": len(fold_metrics),
            "mean_sharpe": float(valid_sharpes.mean()) if len(valid_sharpes) else None,
            "consistency_ratio": consistency_ratio(sharpes),
            "combined_sharpe": compute_combined_sharpe(oos_equity, risk_free_rate),
            "worst_fold_mdd": float(valid_mdd.min()) if len(valid_mdd) else None,
        }
        logger.info(
            f"Walk-forward {wf_run_id} ({self.split_type}, embargo={self.embargo_days}d) "
            f"fold-by-fold OOS results:\n{fold_summary_table(fold_metrics)}"
        )
        logger.info(
            "OOS summary: mean_sharpe={mean_sharpe}, combined_sharpe={combined_sharpe}, "
            "consistency_ratio={consistency_ratio}, worst_fold_mdd={worst_fold_mdd}".format(**summary)
        )
        return summary
