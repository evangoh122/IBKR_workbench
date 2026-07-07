"""
etl/export_hf.py
Stage polygon_bars (filtered to the semiconductor universe) for HuggingFace
upload, and optionally push the dataset to the Hub.

Exports a single Parquet file plus a dataset card (README.md with HF YAML
frontmatter) into an output directory ready for `huggingface_hub.upload_folder`.

Usage:
    # Export only (default when no flags given)
    python -m etl.export_hf

    # Export to a custom directory / universe
    python -m etl.export_hf --out-dir data/exports/hf --universe config/semi_universe.json

    # Export then push to the Hub (needs HF_TOKEN or --token via env)
    python -m etl.export_hf --push --repo my-org/semi-bars --private
"""
import argparse
import json
import os
import re
from pathlib import Path

from typing import Optional

from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

# Repo root = parent of the etl/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"
_DEFAULT_OUT_DIR = "data/exports/hf"


def _load_tickers(universe_path: str):
    """Load and sanitise the ticker list from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    raw = cfg.get("tickers", [])
    # These are trusted config values, but sanitise anyway: uppercase alnum + dot.
    tickers = []
    for t in raw:
        clean = re.sub(r"[^A-Z0-9.]", "", str(t).upper())
        if clean:
            tickers.append(clean)
    return tickers


def export_parquet(universe_path: str = _DEFAULT_UNIVERSE,
                   out_dir: str = _DEFAULT_OUT_DIR) -> Path:
    """Copy universe-filtered polygon_bars rows to a Parquet file + dataset card."""
    tickers = _load_tickers(universe_path)
    if not tickers:
        raise ValueError(f"No tickers found in universe config: {universe_path}")

    out_path = Path(out_dir)
    if not out_path.is_absolute():
        out_path = _REPO_ROOT / out_path
    out_path.mkdir(parents=True, exist_ok=True)

    parquet_file = out_path / "polygon_bars.parquet"
    in_list = ", ".join(f"'{t}'" for t in tickers)
    # Escape single quotes in the output path before interpolating into COPY ... TO.
    parquet_posix = parquet_file.as_posix().replace("'", "''")

    with get_connection() as conn:
        select_sql = f"""
            SELECT ticker, ts, timespan, open, high, low, close,
                   volume, vwap, transactions
            FROM polygon_bars
            WHERE ticker IN ({in_list})
        """
        conn.execute(f"""
            COPY ({select_sql})
            TO '{parquet_posix}' (FORMAT PARQUET)
        """)
        row = conn.execute(
            f"SELECT COUNT(*) FROM polygon_bars WHERE ticker IN ({in_list})"
        ).fetchone()
        row_count = row[0] if row else 0

    size_mb = parquet_file.stat().st_size / (1024 * 1024)
    logger.info(
        f"Exported {row_count:,} rows for {len(tickers)} tickers "
        f"→ {parquet_file} ({size_mb:.2f} MB)"
    )

    _write_dataset_card(out_path, tickers, row_count)
    return out_path


def _write_dataset_card(out_dir: Path, tickers, row_count: int):
    """Write a HuggingFace dataset card (README.md) with YAML frontmatter."""
    ticker_list = ", ".join(tickers)
    card = f"""---
license: other
tags:
- finance
- equities
- semiconductors
- ohlcv
pretty_name: Semiconductor Daily OHLCV Bars (Bronze)
configs:
- config_name: default
  data_files:
  - split: train
    path: polygon_bars.parquet
---

# Semiconductor Daily OHLCV Bars (Bronze Layer)

Daily OHLCV bars for a fixed semiconductor equity universe, sourced from
[Polygon.io](https://polygon.io/) aggregate bars. This is a **bronze-layer**
dataset: minimally processed, deduplicated bars intended for downstream
silver/gold transformation.

> **Adjustment note:** bars are fetched with Polygon `adjusted=True`
> (split/dividend-adjusted). Values are captured as-of ingestion time and are
> **not** retroactively re-adjusted after later corporate actions, so adjusted
> prices for dates preceding a subsequent split may differ from a fresh pull.
> Treat this layer as an append-only, point-in-time capture.

## Columns

| Column         | Description                                              |
| -------------- | -------------------------------------------------------- |
| `ticker`       | Equity symbol (e.g. NVDA)                                |
| `ts`           | Bar open time, ISO-8601 UTC                              |
| `timespan`     | Bar granularity (`day`)                                  |
| `open`         | Opening price                                            |
| `high`         | High price                                               |
| `low`          | Low price                                                |
| `close`        | Closing price                                            |
| `volume`       | Trade volume                                             |
| `vwap`         | Volume-weighted average price                            |
| `transactions` | Number of transactions in the bar                        |

## Provenance

- **Source:** Polygon.io daily aggregate bars (`adjusted=True`).
- **Layer:** Bronze (raw, deduplicated on `ticker, ts, timespan`).
- **Universe:** Semiconductor + equipment/materials equities.
- **Dedup key:** `(ticker, ts, timespan)` via `INSERT OR IGNORE`.
- **Rows:** {row_count:,}
- **Tickers ({len(tickers)}):** {ticker_list}
"""
    readme = out_dir / "README.md"
    readme.write_text(card, encoding="utf-8")
    logger.info(f"Wrote dataset card → {readme}")


def push_to_hub(out_dir: str, repo: str, private: bool = False, token: Optional[str] = None):
    """Push the staged dataset folder to the HuggingFace Hub."""
    try:
        from huggingface_hub import HfApi
    except ImportError:
        logger.error(
            "huggingface_hub is not installed. Run `pip install huggingface_hub` "
            "to enable --push."
        )
        return

    if not repo:
        logger.error("No repo specified. Pass --repo or set HF_DATASET_REPO.")
        return

    folder = Path(out_dir)
    if not folder.is_absolute():
        folder = _REPO_ROOT / folder

    api = HfApi(token=token or os.getenv("HF_TOKEN"))
    api.create_repo(repo, repo_type="dataset", private=private, exist_ok=True)
    api.upload_folder(folder_path=str(folder), repo_id=repo, repo_type="dataset")
    logger.info(f"Pushed {folder} → https://huggingface.co/datasets/{repo}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Stage polygon_bars for HuggingFace and optionally push to the Hub."
    )
    parser.add_argument("--export", action="store_true",
                        help="Export Parquet + dataset card (default if no flags)")
    parser.add_argument("--push", action="store_true",
                        help="Push the staged folder to the HuggingFace Hub")
    parser.add_argument("--repo", default=os.getenv("HF_DATASET_REPO"),
                        help="HF dataset repo id (default: env HF_DATASET_REPO)")
    parser.add_argument("--private", action="store_true",
                        help="Create the HF repo as private")
    parser.add_argument("--out-dir", default=_DEFAULT_OUT_DIR,
                        help=f"Output directory (default: {_DEFAULT_OUT_DIR})")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    parser.add_argument("--token", default=None,
                        help="HF token (default: env HF_TOKEN)")
    args = parser.parse_args()

    # Export by default unless the user asked only to push.
    do_export = args.export or not args.push
    if do_export:
        export_parquet(args.universe, args.out_dir)

    if args.push:
        push_to_hub(args.out_dir, args.repo, private=args.private, token=args.token)


if __name__ == "__main__":
    main()
