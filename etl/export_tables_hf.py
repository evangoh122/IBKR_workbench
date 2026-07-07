"""
etl/export_tables_hf.py
Export each DuckDB table (bronze + silver) to its own Parquet file and push the
folder to a HuggingFace dataset — Spark-native, so Databricks Free Edition /
PySpark can read every table directly (unlike the opaque .duckdb blob).

Usage:
    python -m etl.export_tables_hf                         # export only
    python -m etl.export_tables_hf --push --repo egoh33/semi-workbench-tables
"""
import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OUT = "data/exports/hf_tables"

# Tables to publish (bronze + silver + reference). Empty tables are skipped.
_TABLES = [
    "polygon_bars",
    "polygon_option_bars",
    "polygon_tickers",
    "cot_reports",
    "silver_stock_features",
    "silver_option_greeks",
    "etl_runs",
]


def export_parquet(out_dir: str = _DEFAULT_OUT) -> Path:
    """Export each non-empty table to <out_dir>/<table>.parquet."""
    out_path = Path(out_dir)
    if not out_path.is_absolute():
        out_path = _REPO_ROOT / out_path
    out_path.mkdir(parents=True, exist_ok=True)

    manifest = []
    with get_connection() as conn:
        for table in _TABLES:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            count = row[0] if row else 0
            if count == 0:
                logger.warning(f"{table}: empty, skipping")
                continue
            dest = out_path / f"{table}.parquet"
            conn.execute(
                f"COPY {table} TO '{dest.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            size_mb = dest.stat().st_size / (1024 * 1024)
            logger.info(f"{table}: {count:,} rows -> {dest.name} ({size_mb:.1f} MB)")
            manifest.append((table, count, size_mb))

    _write_card(out_path, manifest)
    return out_path


def _write_card(out_dir: Path, manifest):
    """Write a dataset card with a Databricks/PySpark usage snippet."""
    rows = "\n".join(f"| `{t}.parquet` | {c:,} | {s:.1f} MB |" for t, c, s in manifest)
    card = f"""---
license: other
tags:
- finance
- equities
- semiconductors
- options
- ohlcv
pretty_name: Semiconductor Equity Workbench (Bronze + Silver, per-table Parquet)
configs:
{chr(10).join(f"- config_name: {t}{chr(10)}  data_files: {t}.parquet" for t, _, _ in manifest)}
---

# Semiconductor Equity Workbench — Bronze + Silver (Parquet)

One Parquet file per table. Bronze = raw Polygon flat-file bars; Silver = derived
features (stock technicals; option greeks via Black-Scholes-Merton).

| File | Rows | Size |
| ---- | ---- | ---- |
{rows}

## Read in Databricks Free Edition (PySpark)

```python
from huggingface_hub import hf_hub_download
path = hf_hub_download(
    repo_id="egoh33/semi-workbench-tables", repo_type="dataset",
    filename="silver_option_greeks.parquet",
)
df = spark.read.parquet(f"file://{{path}}")
df.printSchema(); df.show(5)
```

Or read directly with pandas / DuckDB / polars via the same `hf_hub_download` path.
"""
    (out_dir / "README.md").write_text(card, encoding="utf-8")
    logger.info(f"Wrote dataset card -> {out_dir / 'README.md'}")


def push_to_hub(out_dir: str, repo: str, private: bool = False):
    """Push the Parquet folder to a HuggingFace dataset repo."""
    try:
        from huggingface_hub import HfApi
    except ImportError:
        logger.error("huggingface_hub not installed. Run `pip install huggingface_hub`.")
        return
    folder = Path(out_dir)
    if not folder.is_absolute():
        folder = _REPO_ROOT / folder
    api = HfApi(token=os.getenv("HF_TOKEN"))
    api.create_repo(repo, repo_type="dataset", private=private, exist_ok=True)
    api.upload_folder(folder_path=str(folder), repo_id=repo, repo_type="dataset")
    logger.info(f"Pushed {folder} -> https://huggingface.co/datasets/{repo}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Export DuckDB tables to Parquet and push to HF.")
    parser.add_argument("--push", action="store_true", help="Push to the HuggingFace Hub")
    parser.add_argument("--repo", default=os.getenv("HF_TABLES_REPO", "egoh33/semi-workbench-tables"))
    parser.add_argument("--private", action="store_true")
    parser.add_argument("--out-dir", default=_DEFAULT_OUT)
    args = parser.parse_args()

    export_parquet(args.out_dir)
    if args.push:
        push_to_hub(args.out_dir, args.repo, private=args.private)


if __name__ == "__main__":
    main()
