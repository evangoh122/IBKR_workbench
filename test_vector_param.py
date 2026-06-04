
import duckdb
import numpy as np
from query import search_similar_tickers

# Mocking a vector search
try:
    # 384-dimension dummy vector
    dummy_vector = np.random.rand(384).tolist()
    print("Testing vector search parameterization...")
    results = search_similar_tickers(dummy_vector, limit=1)
    print("✅ Success! DuckDB handled the parameterization.")
except Exception as e:
    print(f"❌ Failed! Vector search error: {e}")
