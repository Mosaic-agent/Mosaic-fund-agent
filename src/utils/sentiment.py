"""
src/utils/sentiment.py
──────────────────────
Shared rule-based sentiment inference used across news tools.

Sentiment is determined by keyword hit counts:
  positive hits > negative → POSITIVE / BULLISH
  negative hits > positive → NEGATIVE / BEARISH
  tied or no signal        → NEUTRAL

Single source of truth for `_POSITIVE_WORDS`, `_NEGATIVE_WORDS`, and
`infer_sentiment()`.  Previously duplicated across news_search.py,
etf_news_scanner.py, and macro_event_scanner.py.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.portfolio import Sentiment

_POSITIVE_WORDS: frozenset[str] = frozenset({
    "surge", "rally", "gain", "profit", "record", "growth", "beat",
    "strong", "upgrade", "buy", "bullish", "outperform", "dividend",
    "expansion", "robust", "soar", "rise", "high", "positive", "boom",
})

_NEGATIVE_WORDS: frozenset[str] = frozenset({
    "fall", "drop", "loss", "crash", "decline", "miss", "weak", "sell",
    "bearish", "underperform", "cut", "downgrade", "risk", "concern",
    "fraud", "penalty", "regulatory", "debt", "pressure", "plunge",
    "slowdown", "warning", "default", "lawsuit",
})


def infer_sentiment(text: str) -> "Sentiment":
    """
    Rule-based sentiment from article title + description.

    Scores positive and negative keyword hits and returns the dominant
    ``Sentiment`` enum value from ``src.models.portfolio``.

    Args:
        text: Raw article title + description concatenated.

    Returns:
        ``Sentiment.POSITIVE``, ``Sentiment.NEGATIVE``, or ``Sentiment.NEUTRAL``.
    """
    from src.models.portfolio import Sentiment

    words = set(text.lower().split())
    pos_hits = len(words & _POSITIVE_WORDS)
    neg_hits = len(words & _NEGATIVE_WORDS)

    if pos_hits > neg_hits:
        return Sentiment.POSITIVE
    if neg_hits > pos_hits:
        return Sentiment.NEGATIVE
    return Sentiment.NEUTRAL
