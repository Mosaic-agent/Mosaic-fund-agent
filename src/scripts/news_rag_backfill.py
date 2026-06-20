#!/usr/bin/env python3
"""
src/scripts/news_rag_backfill.py
────────────────────────────────
One-shot backfill: embeds all existing news_articles rows that have
an empty embedding column.

Usage:
    python src/scripts/news_rag_backfill.py [--batch-size 32] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill news article embeddings")
    parser.add_argument("--batch-size", type=int, default=32, help="Articles per embed batch")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't embed")
    args = parser.parse_args()

    from src.db.pool import query_df, get_pool
    from src.ml.news_rag import embed_batch

    # Count unembedded rows
    df_count = query_df(
        "SELECT count() AS cnt FROM market_data.news_articles FINAL "
        "WHERE length(embedding) = 0"
    )
    total = int(df_count.iloc[0]["cnt"]) if not df_count.empty else 0
    log.info("Found %d articles without embeddings", total)

    if total == 0:
        print("✅ All articles already have embeddings.")
        return

    if args.dry_run:
        print(f"DRY RUN: Would embed {total} articles in batches of {args.batch_size}")
        return

    # Process in batches using OFFSET pagination
    processed = 0
    batch_size = args.batch_size
    start_time = time.time()

    while processed < total:
        # Fetch a batch of unembedded articles
        df = query_df(
            "SELECT fetched_at, title, source, url, published_at, "
            "       source_type, fetch_source, category, etfs_impacted, "
            "       sentiment, impact_tier "
            "FROM market_data.news_articles FINAL "
            "WHERE length(embedding) = 0 "
            f"LIMIT {batch_size}"
        )
        if df.empty:
            break

        # Build texts for embedding
        texts = []
        for _, row in df.iterrows():
            text = str(row["title"] or "")
            texts.append(text[:512])

        # Embed batch
        try:
            vectors = embed_batch(texts)
        except Exception as e:
            log.error("Embedding batch failed: %s", e)
            break

        # Write back via INSERT (ReplacingMergeTree will deduplicate)
        with get_pool().acquire() as client:
            data = []
            for i, (_, row) in enumerate(df.iterrows()):
                data.append([
                    row["fetched_at"],
                    row["published_at"],
                    row["source_type"],
                    row["fetch_source"],
                    row["category"],
                    row["etfs_impacted"],
                    row["sentiment"],
                    row["impact_tier"],
                    row["title"],
                    row["source"],
                    row["url"],
                    vectors[i] if i < len(vectors) else [],
                ])
            client.insert(
                "market_data.news_articles",
                data,
                column_names=[
                    "fetched_at", "published_at", "source_type", "fetch_source",
                    "category", "etfs_impacted", "sentiment", "impact_tier",
                    "title", "source", "url", "embedding",
                ],
            )

        processed += len(df)
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        log.info(
            "Progress: %d/%d (%.0f articles/sec)",
            processed, total, rate,
        )

    elapsed = time.time() - start_time
    print(f"✅ Backfill complete: {processed} articles embedded in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
