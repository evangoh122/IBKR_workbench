"""
etl/polygon_client.py
Factory for the polygon.io REST client.
"""
import os
from polygon import RESTClient


def get_polygon_client() -> RESTClient:
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        raise ValueError(
            "POLYGON_API_KEY is not set. Add it to .env (https://polygon.io/dashboard/api-keys)"
        )
    return RESTClient(api_key=api_key)
