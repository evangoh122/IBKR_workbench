"""
databricks/bronze_ticks_spark.py
Bronze-layer PySpark job for Polygon S3 FLAT FILES at TICK (trade) scale — the
Databricks/Spark sibling of the local DuckDB day-aggregate loader
(etl/bulk_load_flatfiles.py). This runs on a Databricks job cluster or via
spark-submit; it is NOT meant to run locally (tick trades are far too large for
a single machine / CI runner).

Ingests the Massive/Polygon flat-file TRADE files:
  * STOCKS  : s3://flatfiles/us_stocks_sip/trades_v1/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz   (~5yr)
  * OPTIONS : s3://flatfiles/us_options_opra/trades_v1/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz (~2yr)

Trades CSV (gzip, header):
    ticker,conditions,correction,exchange,id,participant_timestamp,price,
    sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp
  * sip_timestamp is NANOSECONDS since epoch.
  * For OPTIONS, `ticker` is an OPRA symbol, e.g. O:MU240119C00080000
    = underlying MU / expiry 2024-01-19 / Call / strike 80.000

Output (Delta, partitioned by trade_date):
  * <output-root>/stock_trades
  * <output-root>/option_trades

Run commands
------------
Databricks Asset Bundle (see resources.jobs.bronze_ticks in databricks.yml):
    databricks bundle deploy -t dev
    databricks bundle run bronze_ticks -t dev -- \
        --dataset both --start-year 2021 --end-year 2026 \
        --output-root /Volumes/main/ibkr/bronze

spark-submit (any Spark 3.x with hadoop-aws / delta on the classpath):
    spark-submit databricks/bronze_ticks_spark.py \
        --dataset stocks --start-year 2021 --end-year 2026 \
        --output-root /Volumes/main/ibkr/bronze

Credentials / endpoint (env or Databricks secrets scope "polygon"):
    POLYGON_Access_KEY_ID / POLYGON_SECRET_ACCESS_KEY
    endpoint https://files.massive.com , bucket s3://flatfiles (path-style access)

ASSUMPTIONS (documented, since this is a build-only template):
  * hadoop-aws (s3a) + delta-spark are available on the cluster. On Databricks
    Runtime both are present by default. For plain spark-submit you must supply
    --packages org.apache.hadoop:hadoop-aws:<ver>,io.delta:delta-spark_2.12:<ver>.
  * Secrets, if used, live in a scope named "polygon" with keys
    "access_key_id" / "secret_access_key". Falls back to env vars.
  * The S3A path-style + custom endpoint config mirrors the AWS CLI setup used by
    etl/bulk_load_flatfiles.py (endpoint https://files.massive.com).
  * Trade files use the schema above; only the columns we need are typed
    strictly, the rest are read as strings to be tolerant of minor drift.
"""
import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

# Guard so the module works both as a Databricks job and via spark-submit.
spark = SparkSession.builder.getOrCreate()

# ── S3 / endpoint constants (mirror etl/bulk_load_flatfiles.py) ───────────────
S3_ENDPOINT = "https://files.massive.com"
BUCKET_ROOT = "s3a://flatfiles"
STOCKS_PREFIX = "us_stocks_sip/trades_v1"
OPTIONS_PREFIX = "us_options_opra/trades_v1"

# OPRA symbol pattern: O:{UNDERLYING}{YYMMDD}{C|P}{STRIKE8}
_OPRA_REGEX = r"^O:([A-Z.]+)([0-9]{6})([CP])([0-9]{8})$"

# Repo root = parent of the databricks/ package directory
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_UNIVERSE = "config/semi_universe.json"

# Explicit schema for the Polygon trades CSV (gzip, header row present).
TRADES_SCHEMA = StructType([
    StructField("ticker", StringType(), True),
    StructField("conditions", StringType(), True),
    StructField("correction", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("id", StringType(), True),
    StructField("participant_timestamp", LongType(), True),
    StructField("price", StringType(), True),
    StructField("sequence_number", StringType(), True),
    StructField("sip_timestamp", LongType(), True),
    StructField("size", StringType(), True),
    StructField("tape", StringType(), True),
    StructField("trf_id", StringType(), True),
    StructField("trf_timestamp", StringType(), True),
])


# ── Credentials / S3A wiring ──────────────────────────────────────────────────
def _get_secret(scope: str, key: str):
    """Read a Databricks secret if dbutils is present, else return None."""
    try:
        # dbutils is injected into the Databricks job runtime.
        return dbutils.secrets.get(scope=scope, key=key)  # type: ignore[name-defined]  # noqa: F821
    except Exception:
        return None


def _resolve_credentials():
    """Return (access_key, secret_key) from Databricks secrets or env vars."""
    access_key = _get_secret("polygon", "access_key_id") or os.environ.get("POLYGON_ACCESS_KEY_ID") or os.environ.get("POLYGON_Access_KEY_ID")
    secret_key = _get_secret("polygon", "secret_access_key") or os.environ.get("POLYGON_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Missing Polygon S3 credentials. Set Databricks secrets "
            "scope 'polygon' (access_key_id/secret_access_key) or env vars "
            "POLYGON_ACCESS_KEY_ID / POLYGON_SECRET_ACCESS_KEY."
        )
    return access_key, secret_key


def configure_s3a(spark_session):
    """Point Spark's S3A filesystem at the Polygon/Massive flat-file endpoint."""
    access_key, secret_key = _resolve_credentials()
    hconf = spark_session._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", S3_ENDPOINT)
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.path.style.access", "true")
    # Simple credentials provider (static keys, not instance profile).
    hconf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


# ── Universe loading ──────────────────────────────────────────────────────────
def load_universe(universe_path: str):
    """Return the list of ticker symbols from a universe JSON config file."""
    path = Path(universe_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg.get("tickers", [])


def _year_globs(prefix: str, start_year: int, end_year: int) -> list:
    """Build S3A glob paths (one per year) for the trade CSVs in a date range."""
    globs = []
    for y in range(start_year, end_year + 1):
        # {YYYY}/{MM}/{YYYY-MM-DD}.csv.gz  → glob every month/day under the year.
        globs.append(f"{BUCKET_ROOT}/{prefix}/{y}/*/*.csv.gz")
    return globs


def _read_trades(paths: list):
    """Read the gzip trade CSVs at the given S3A globs with the explicit schema."""
    return (
        spark.read
        .option("header", True)
        .option("compression", "gzip")
        .schema(TRADES_SCHEMA)
        .csv(paths)
    )


def _with_time_cols(df):
    """Add `ts` (ns→micros timestamp) and `trade_date` from sip_timestamp."""
    return (
        df
        # sip_timestamp is nanoseconds; timestamp_micros expects micros. Use
        # integer division (`div`) — float `/ 1000` on ~1.7e18 exceeds 2^53 and
        # loses precision before truncation.
        .withColumn("ts", F.expr("timestamp_micros(sip_timestamp div 1000)"))
        .withColumn("trade_date", F.to_date("ts"))
    )


def _write_delta(df, output_root: str, table: str):
    """Idempotent Delta write partitioned by trade_date (dynamic partition overwrite)."""
    dest = output_root.rstrip("/") + "/" + table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        # Only rewrite the partitions present in this batch → idempotent re-runs.
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("trade_date")
        .save(dest)
    )
    return dest


# ── Stocks ────────────────────────────────────────────────────────────────────
def load_stocks(start_year: int, end_year: int, universe, output_root: str) -> str:
    """Filter stock trades to the universe and write Delta stock_trades."""
    paths = _year_globs(STOCKS_PREFIX, start_year, end_year)
    print(f"[bronze_ticks] STOCKS reading {len(paths)} year-glob(s): {paths}")

    df = _read_trades(paths)
    df = df.filter(F.col("ticker").isin(universe))
    df = _with_time_cols(df)
    out = df.select(
        "ticker",
        F.col("price").cast("double").alias("price"),
        F.col("size").cast("double").alias("size"),
        "exchange",
        "conditions",
        "sip_timestamp",
        "ts",
        "trade_date",
    )
    dest = _write_delta(out, output_root, "stock_trades")
    print(f"[bronze_ticks] STOCKS → Delta {dest} (partitioned by trade_date)")
    return dest


# ── Options ───────────────────────────────────────────────────────────────────
def load_options(start_year: int, end_year: int, universe, output_root: str) -> str:
    """Parse OPRA symbols, filter to universe underlyings, write Delta option_trades."""
    paths = _year_globs(OPTIONS_PREFIX, start_year, end_year)
    print(f"[bronze_ticks] OPTIONS reading {len(paths)} year-glob(s): {paths}")

    df = _read_trades(paths)
    df = (
        df
        .withColumn("underlying", F.regexp_extract("ticker", _OPRA_REGEX, 1))
        .withColumn("_expiry_raw", F.regexp_extract("ticker", _OPRA_REGEX, 2))
        .withColumn("_right_raw", F.regexp_extract("ticker", _OPRA_REGEX, 3))
        .withColumn("_strike_raw", F.regexp_extract("ticker", _OPRA_REGEX, 4))
    )
    df = df.filter((F.col("underlying") != "") & F.col("underlying").isin(universe))
    df = _with_time_cols(df)

    out = df.select(
        F.col("ticker").alias("option_ticker"),
        "underlying",
        F.to_date(
            F.concat_ws(
                "-",
                F.concat(F.lit("20"), F.substring("_expiry_raw", 1, 2)),
                F.substring("_expiry_raw", 3, 2),
                F.substring("_expiry_raw", 5, 2),
            )
        ).alias("expiry"),
        (F.col("_strike_raw").cast("double") / F.lit(1000.0)).alias("strike"),
        F.when(F.col("_right_raw") == "C", F.lit("call")).otherwise(F.lit("put")).alias("right"),
        F.col("price").cast("double").alias("price"),
        F.col("size").cast("double").alias("size"),
        "exchange",
        "conditions",
        "sip_timestamp",
        "ts",
        "trade_date",
    )
    dest = _write_delta(out, output_root, "option_trades")
    print(f"[bronze_ticks] OPTIONS → Delta {dest} (partitioned by trade_date)")
    return dest


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Bronze-layer PySpark TICK (trade) loader for the semiconductor "
                    "universe from Polygon/Massive S3 flat files."
    )
    parser.add_argument("--dataset", choices=["stocks", "options", "both"],
                        default="both", help="Which dataset(s) to load (default: both)")
    parser.add_argument("--start-year", type=int, default=2021,
                        help="First calendar year to load (default: 2021, ~5yr for stocks)")
    parser.add_argument("--end-year", type=int, default=2026,
                        help="Last calendar year to load, inclusive (default: 2026)")
    parser.add_argument("--options-start-year", type=int, default=2024,
                        help="Override first year for OPTIONS when --dataset both "
                             "(default: 2024, ~2yr). Ignored for --dataset stocks.")
    parser.add_argument("--output-root", default="/Volumes/main/ibkr/bronze",
                        help="Delta output root; tables written under "
                             "<root>/stock_trades and <root>/option_trades")
    parser.add_argument("--universe", default=_DEFAULT_UNIVERSE,
                        help=f"Universe config path (default: {_DEFAULT_UNIVERSE})")
    args = parser.parse_args()

    configure_s3a(spark)

    universe = load_universe(args.universe)
    print(f"[bronze_ticks] Loaded {len(universe)} tickers from {args.universe}")

    if args.dataset in ("stocks", "both"):
        load_stocks(args.start_year, args.end_year, universe, args.output_root)

    if args.dataset in ("options", "both"):
        opt_start = args.options_start_year if args.dataset == "both" else args.start_year
        load_options(opt_start, args.end_year, universe, args.output_root)

    print("[bronze_ticks] Done.")


if __name__ == "__main__":
    main()
