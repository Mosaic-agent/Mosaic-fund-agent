#!/usr/bin/env python3
"""
config/clickhouse/migrations/001_add_news_embedding.py
──────────────────────────────────────────────────────
Adds an `embedding` column (Array(Float32)) to market_data.news_articles
for semantic RAG retrieval in the correlation engine.

Run once:
    python config/clickhouse/migrations/001_add_news_embedding.py
"""
from src.db.pool import execute, query_df

def migrate():
    # Check if column already exists
    df = query_df(
        "SELECT name FROM system.columns "
        "WHERE database = 'market_data' AND table = 'news_articles' AND name = 'embedding'"
    )
    if not df.empty:
        print("✅ Column 'embedding' already exists in market_data.news_articles")
        return

    execute(
        "ALTER TABLE market_data.news_articles "
        "ADD COLUMN IF NOT EXISTS embedding Array(Float32) DEFAULT []"
    )
    print("✅ Added 'embedding' column to market_data.news_articles")


if __name__ == "__main__":
    migrate()
