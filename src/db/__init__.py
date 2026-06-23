"""
src/db
──────
ClickHouse connection pool and query helpers.

All connection parameters are read from ``config.settings`` (CLICKHOUSE_HOST,
CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD).
**Never pass raw connection params** — use the factory functions below.

Usage
-----
    # One-shot SELECT → DataFrame (most common)
    from src.db import query_df
    df = query_df("SELECT * FROM market_data.daily_prices FINAL LIMIT 10")

    # Context-manager checkout (auto-returns to pool, preferred for inserts)
    from src.db import acquire
    with acquire() as client:
        client.insert_df("market_data.daily_prices", df)

    # DDL / non-SELECT statement
    from src.db import execute
    execute("OPTIMIZE TABLE market_data.daily_prices FINAL")

    # Unmanaged client for scripts with long exclusive access (caller closes)
    from src.db import get_client
    client = get_client()
    try:
        client.insert_df(...)
    finally:
        client.close()
"""

from src.db.pool import CHPool, get_pool, query_df, execute, get_client  # noqa: F401

__all__ = ["CHPool", "get_pool", "query_df", "execute", "get_client"]
