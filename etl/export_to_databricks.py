"""
etl/export_to_databricks.py
Export DuckDB tables to Parquet and upload to Databricks Unity Catalog.

Usage:
    # Step 1: export DuckDB → Parquet
    python -m etl.export_to_databricks --export

    # Step 2: upload Parquet → Databricks Volume
    python -m etl.export_to_databricks --upload

    # Step 3: create Delta tables (run notebook in Databricks, or via CLI)
    python -m etl.export_to_databricks --create-tables

    # All steps in one go
    python -m etl.export_to_databricks --all
"""
import argparse
from pathlib import Path

from databricks.sdk.errors import NotFound
from dotenv import load_dotenv
from loguru import logger

from db.database import get_connection

load_dotenv(".databricks/.databricks.env")

logger.add("logs/etl_{time:YYYY-MM-DD}.log", rotation="1 day", retention="14 days", level="DEBUG")

EXPORT_DIR = Path("data/exports")

# Unity Catalog coordinates — change if your catalog/schema differ
UC_CATALOG = "main"
UC_SCHEMA  = "ibkr"
UC_VOLUME  = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/raw"

TABLES = [
    "polygon_bars",
    "polygon_option_bars",
    "edgar_filings",
    "edgar_facts",
    "edgar_13f",
    "cot_reports",
    "polygon_tickers",
    "ticker_embeddings",
]


def export_to_parquet():
    """Export each DuckDB table to a Parquet file in data/exports/."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Exporting {len(TABLES)} tables to {EXPORT_DIR}")

    with get_connection() as conn:
        for table in TABLES:
            out = EXPORT_DIR / f"{table}.parquet"
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count == 0:
                    logger.warning(f"{table}: empty, skipping")
                    continue
                conn.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET)")
                size_mb = round(out.stat().st_size / 1024 / 1024, 1)
                logger.info(f"{table}: {count:,} rows → {out.name} ({size_mb} MB)")
            except Exception as e:
                logger.error(f"{table}: export failed — {e}")


def upload_to_databricks():
    """Upload Parquet files to Databricks Unity Catalog Volume via SDK."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import VolumeType

    files = list(EXPORT_DIR.glob("*.parquet"))
    if not files:
        logger.error(f"No Parquet files found in {EXPORT_DIR}. Run --export first.")
        return

    w = WorkspaceClient()

    # Ensure schema / volume exist (catalog must already exist)
    try:
        w.schemas.get(f"{UC_CATALOG}.{UC_SCHEMA}")
    except NotFound:
        logger.info(f"Creating schema {UC_CATALOG}.{UC_SCHEMA}")
        w.schemas.create(UC_SCHEMA, catalog_name=UC_CATALOG)

    try:
        w.volumes.read(f"{UC_CATALOG}.{UC_SCHEMA}.raw")
    except NotFound:
        logger.info(f"Creating volume {UC_CATALOG}.{UC_SCHEMA}.raw")
        w.volumes.create(
            catalog_name=UC_CATALOG,
            schema_name=UC_SCHEMA,
            name="raw",
            volume_type=VolumeType.MANAGED,
        )

    logger.info(f"Uploading {len(files)} files to {UC_VOLUME}")
    for f in files:
        dest = f"{UC_VOLUME}/{f.name}"
        logger.info(f"Uploading {f.name} ({round(f.stat().st_size/1024/1024, 1)} MB) → {dest}")
        try:
            with open(f, "rb") as fh:
                w.files.upload(dest, fh, overwrite=True)
            logger.info(f"{f.name}: uploaded OK")
        except Exception as e:
            logger.error(f"{f.name}: upload failed — {e}")


def create_delta_tables():
    """Generate and run notebook commands to create Delta tables from Parquet."""
    # Build the Python notebook cell content
    cell = f"""
# Run this cell in a Databricks notebook to create Delta tables from uploaded Parquet files

catalog = "{UC_CATALOG}"
schema  = "{UC_SCHEMA}"
volume  = "{UC_VOLUME}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {{catalog}}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}}")

tables = {TABLES}

for table in tables:
    parquet_path = f"{{volume}}/{{table}}.parquet"
    try:
        spark.sql(f\"\"\"
            CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{{table}}
            USING DELTA AS
            SELECT * FROM parquet.`{{parquet_path}}`
        \"\"\")
        count = spark.table(f"{{catalog}}.{{schema}}.{{table}}").count()
        print(f"✓ {{table}}: {{count:,}} rows")
    except Exception as e:
        print(f"✗ {{table}}: {{e}}")
"""
    notebook_path = Path("databricks_create_tables.py")
    notebook_path.write_text(cell.strip(), encoding="utf-8")
    logger.info(f"Notebook cell written to {notebook_path}")
    logger.info("Copy the contents into a Databricks notebook and run it.")
    print(f"\n{'='*60}")
    print(f"Notebook saved to: {notebook_path}")
    print(f"Open it and paste into a Databricks notebook cell to create tables.")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export",        action="store_true", help="Export DuckDB → Parquet")
    parser.add_argument("--upload",        action="store_true", help="Upload Parquet → Databricks Volume")
    parser.add_argument("--create-tables", action="store_true", help="Generate Delta table creation notebook")
    parser.add_argument("--all",           action="store_true", help="Run all three steps")
    args = parser.parse_args()

    if args.all or args.export:
        export_to_parquet()
    if args.all or args.upload:
        upload_to_databricks()
    if args.all or args.create_tables:
        create_delta_tables()

    if not any([args.export, args.upload, args.create_tables, args.all]):
        parser.print_help()


if __name__ == "__main__":
    main()
