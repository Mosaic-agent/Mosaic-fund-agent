"""
src/ml/news_rag.py — Backward-compatibility re-export stub.

The implementation has moved to src/ml/correlation/news_rag.py.
This file preserves the old import path:
    from src.ml.news_rag import embed_text, score_news_quality, ...
"""

from src.ml.correlation.news_rag import (  # noqa: F401
    embed_batch,
    embed_text,
    retrieve_articles,
    score_event_relevance,
    score_news_quality,
    upsert_to_qdrant,
)
