"""
src/db
──────
ClickHouse connection pool and query helpers.

Usage
-----
    from src.db import pool

    # Context-manager checkout (recommended — auto-returns to pool)
    with pool.acquire() as client:
        df = client.query_df("SELECT ...")

    # One-shot helper (checks out, queries, returns)
    df = pool.query_df("SELECT ...")
    pool.execute("INSERT INTO ...")

    # Raw client for scripts / one-off use (caller must close)
    client = pool.get_client()
    client.close()
"""

from src.db.pool import CHPool, get_pool, query_df, execute  # noqa: F401

__all__ = ["CHPool", "get_pool", "query_df", "execute"]
