"""
config/update_tickers.py
Fetches ALL tickers from US stock exchanges via Finviz,
categorizes them by industry, and updates config/tickers.yaml.

Fetches one page at a time (20 tickers/page) with retry + checkpoint so it
can resume from the last saved page if Finviz resets the connection.
"""
import re
import time
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List

from finvizfinance.screener.overview import Overview
from loguru import logger

YAML_PATH        = Path(__file__).parent / "tickers.yaml"
CHECKPOINT_PATH  = Path(__file__).parent / "_ticker_checkpoint.csv"
CHECKPOINT_PAGE  = Path(__file__).parent / "_ticker_checkpoint_page.txt"
PAGE_SLEEP       = 2     # seconds between pages
CHECKPOINT_EVERY = 50    # save checkpoint every N pages


def _clean_key(industry: str) -> str:
    key = industry.lower().replace("&", "and").replace("-", " ")
    key = re.sub(r"[^a-z0-9]", " ", key)
    return "_".join(key.split())


def fetch_all_finviz_tickers() -> pd.DataFrame:
    """
    Fetches all tickers page-by-page using select_page.
    Resumes from checkpoint if one exists.
    Returns a combined DataFrame with Ticker + Industry columns.
    """
    foverview = Overview()

    # Resume from checkpoint if available
    if CHECKPOINT_PATH.exists() and CHECKPOINT_PAGE.exists():
        existing   = pd.read_csv(CHECKPOINT_PATH)
        start_page = int(CHECKPOINT_PAGE.read_text().strip()) + 1
        logger.info(f"Resuming from page {start_page} ({len(existing)} tickers so far)")
        all_rows = [existing]
    else:
        start_page = 1
        all_rows = []

    page = start_page
    consecutive_errors = 0

    while True:
        for attempt in range(4):
            try:
                df_page = foverview.screener_view(
                    order="Ticker",
                    select_page=page,
                    verbose=0,
                    sleep_sec=0,
                )
                consecutive_errors = 0
                break
            except Exception as e:
                wait = 10 * (attempt + 1)
                logger.warning(f"Page {page} attempt {attempt+1} failed: {e} — retrying in {wait}s")
                time.sleep(wait)
        else:
            # All 4 attempts failed
            consecutive_errors += 1
            logger.error(f"Page {page} failed after 4 attempts — skipping")
            if consecutive_errors >= 3:
                logger.error("3 consecutive page failures — stopping early")
                break
            page += 1
            continue

        if df_page is None or df_page.empty:
            logger.info(f"Empty page at {page} — fetch complete")
            break

        all_rows.append(df_page)
        count = sum(len(d) for d in all_rows)
        logger.info(f"Page {page}: +{len(df_page)} tickers ({count} total)")

        # Checkpoint every N pages
        if page % CHECKPOINT_EVERY == 0:
            checkpoint_df = pd.concat(all_rows, ignore_index=True)
            checkpoint_df.to_csv(CHECKPOINT_PATH, index=False)
            CHECKPOINT_PAGE.write_text(str(page))
            logger.info(f"Checkpoint saved at page {page} ({len(checkpoint_df)} tickers)")

        if len(df_page) < 20:
            break

        page += 1
        time.sleep(PAGE_SLEEP)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True).drop_duplicates(subset="Ticker")

    # Clean up checkpoint on success
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
    if CHECKPOINT_PAGE.exists():
        CHECKPOINT_PAGE.unlink()

    return result


def build_industry_map(df: pd.DataFrame) -> Dict[str, List[str]]:
    industry_map: Dict[str, List[str]] = {}
    for industry, group in df.groupby("Industry"):
        if pd.isna(industry) or str(industry).upper() in ("N/A", ""):
            continue
        key = _clean_key(str(industry))
        tickers = sorted(group["Ticker"].dropna().tolist())
        if key in industry_map:
            industry_map[key] = sorted(set(industry_map[key] + tickers))
        else:
            industry_map[key] = tickers
    return industry_map


def update_tickers_yaml(industry_map: Dict[str, List[str]], path: Path = YAML_PATH):
    if path.exists():
        with open(path) as f:
            full_config = yaml.safe_load(f) or {}
    else:
        full_config = {}

    full_config.setdefault("groups", {})
    for key, tickers in industry_map.items():
        full_config["groups"][key] = {"tickers": tickers}

    with open(path, "w") as f:
        yaml.dump(full_config, f, sort_keys=True, indent=2)

    total = sum(len(v) for v in industry_map.values())
    logger.info(f"Saved {total} tickers across {len(industry_map)} industries → {path}")


if __name__ == "__main__":
    df = fetch_all_finviz_tickers()
    if df.empty:
        logger.error("No ticker data fetched.")
    else:
        logger.info(f"Total tickers fetched: {len(df)}")
        industry_map = build_industry_map(df)
        update_tickers_yaml(industry_map)
