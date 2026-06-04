"""etl/utils.py — Shared utilities for ETL modules."""
from datetime import datetime, timezone


def utcnow() -> str:
    """Return the current UTC time as an ISO-8601 string (seconds precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
