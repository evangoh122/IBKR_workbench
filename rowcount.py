import duckdb
conn = duckdb.connect("./data/ibkr.duckdb", read_only=True)
tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
print(f"{'Table':<35} {'Rows':>12}")
print("-" * 50)
total = 0
for t in sorted(tables):
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    total += n
    print(f"{t:<35} {n:>12,}")
print("-" * 50)
print(f"{'TOTAL':<35} {total:>12,}")
conn.close()
