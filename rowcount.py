import duckdb
from loguru import logger

# rowcount.py
# Handy utility for quick DB inspection.
# Note: Uses f-strings for table names. This is safe here because table names
# are sourced directly from 'SHOW TABLES' in the database itself (trusted schema).
def run_rowcount():
    conn = None
    try:
        conn = duckdb.connect("./data/ibkr.duckdb", read_only=True)
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        
        print(f"{'Table':<35} {'Rows':>12}")
        print("-" * 50)
        total = 0
        for t in sorted(tables):
            # Double-check that t is a valid identifier (alphanumeric or underscore)
            # as an extra layer of defense.
            if not all(c.isalnum() or c == '_' for c in t):
                logger.warning(f"Skipping suspicious table name: {t}")
                continue
                
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            total += n
            print(f"{t:<35} {n:>12,}")
        print("-" * 50)
        print(f"{'TOTAL':<35} {total:>12,}")
    except Exception as e:
        logger.error(f"Rowcount failed: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_rowcount()
