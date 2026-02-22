"""
src/tools/news_search.py
─────────────────────────
LangChain tool for fetching Indian financial news via GNews (Google News).

No API key required — GNews scrapes Google News RSS feeds.
Rate-limit friendly: no daily quota.

[NON-SENSITIVE] No credentials needed for this module.
"""

from __future__ import annotations

import logging
from typing import Any

from gnews import GNews
from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import NewsItem, Sentiment

logger = logging.getLogger(__name__)

# ── GNews URL-expansion patch ─────────────────────────────────────────────────
# gnews resolves each Google-redirect URL via requests.head() with no timeout,
# which can hang indefinitely on slow networks.  We replace process_url with a
# version that uses a short timeout and falls back to the raw Google URL.

try:
    from gnews.utils import utils as _gnews_utils
    import requests as _requests

    def _process_url_with_timeout(item, exclude_websites=None, proxy=None):  # type: ignore[no-redef]
        raw = item.link if hasattr(item, "link") else item.get("link", "")
        try:
            resp = _requests.head(raw, timeout=5, allow_redirects=True)
            return resp.url
        except Exception:
            return raw

    _gnews_utils.process_url = _process_url_with_timeout
except Exception:
    pass  # If the patch fails, gnews still works (just potentially slower)


# ── GNews Client ─────────────────────────────────────────────────────────────

def _make_gnews_client() -> GNews:
    """Create a GNews client configured for Indian English financial news."""
    return GNews(
        language="en",
        country="IN",
        max_results=settings.news_articles_per_stock,
        period=f"{settings.news_lookback_days}d",
    )


# ── Sentiment heuristic ───────────────────────────────────────────────────────

_POSITIVE_WORDS = {
    "surge", "rally", "gain", "profit", "record", "growth", "beat",
    "strong", "upgrade", "buy", "bullish", "outperform", "dividend",
    "expansion", "robust", "soar", "rise", "high", "positive", "boom",
}

_NEGATIVE_WORDS = {
    "fall", "drop", "loss", "crash", "decline", "miss", "weak", "sell",
    "bearish", "underperform", "cut", "downgrade", "risk", "concern",
    "fraud", "penalty", "regulatory", "debt", "pressure", "plunge",
    "slowdown", "warning", "default", "lawsuit",
}


def _infer_sentiment(text: str) -> Sentiment:
    """
    Rule-based sentiment from article title + description.
    Scores positive and negative keyword hits and returns the dominant sentiment.
    """
    words = set(text.lower().split())
    pos_hits = len(words & _POSITIVE_WORDS)
    neg_hits = len(words & _NEGATIVE_WORDS)

    if pos_hits > neg_hits:
        return Sentiment.POSITIVE
    if neg_hits > pos_hits:
        return Sentiment.NEGATIVE
    return Sentiment.NEUTRAL


def fetch_news_for_symbol(symbol: str, company_name: str = "") -> list[NewsItem]:
    """
    Fetch recent news articles for a given NSE/BSE stock symbol via Google News.

    Args:
        symbol:       Zerodha trading symbol e.g. 'RELIANCE', 'TCS'
        company_name: Optional full company name for better query results.

    Returns:
        List of NewsItem models (up to news_articles_per_stock from config).
    """
    client = _make_gnews_client()
    query = f"{company_name} NSE stock" if company_name else f"{symbol} NSE stock"

    try:
        articles = client.get_news(query)
    except Exception as exc:
        logger.warning("GNews request failed for %s: %s", symbol, exc)
        return []

    # Fallback: bare symbol query if primary returned nothing
    if not articles:
        try:
            articles = client.get_news(symbol)
        except Exception:
            return []

    items: list[NewsItem] = []
    for article in articles[: settings.news_articles_per_stock]:
        title = article.get("title") or ""
        description = article.get("description") or ""
        publisher = article.get("publisher", {})
        source = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)

        items.append(
            NewsItem(
                title=title,
                source=source,
                published_at=str(article.get("published date", "")),
                url=article.get("url") or "",
                description=description,
                sentiment=_infer_sentiment(f"{title} {description}"),
            )
        )

    logger.info(
        "Fetched %d news articles for %s (lookback=%dd)",
        len(items),
        symbol,
        settings.news_lookback_days,
    )
    return items


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_stock_news(input_str: str) -> dict[str, Any]:
    """
    Fetch the latest Indian financial news for a stock symbol using Google News.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                   → searches for RELIANCE NSE stock news
      "TCS|Tata Consultancy"       → searches for Tata Consultancy NSE stock news

    Returns a list of news articles with title, source, date, URL,
    sentiment (POSITIVE/NEUTRAL/NEGATIVE), and overall sentiment summary.

    Note: No API key required — powered by Google News RSS via gnews.
    """
    parts = input_str.strip().split("|")
    symbol = parts[0].strip().upper()
    company_name = parts[1].strip() if len(parts) > 1 else ""

    news_items = fetch_news_for_symbol(symbol, company_name)

    if not news_items:
        return {
            "symbol": symbol,
            "articles": [],
            "overall_sentiment": "NEUTRAL",
            "note": "No articles found.",
        }

    # Aggregate sentiment
    sentiments = [item.sentiment for item in news_items]
    pos_count = sentiments.count(Sentiment.POSITIVE)
    neg_count = sentiments.count(Sentiment.NEGATIVE)

    if pos_count > neg_count:
        overall = "POSITIVE"
    elif neg_count > pos_count:
        overall = "NEGATIVE"
    else:
        overall = "NEUTRAL"

    return {
        "symbol": symbol,
        "articles": [
            {
                "title": item.title,
                "source": item.source,
                "published_at": item.published_at,
                "url": item.url,
                "sentiment": item.sentiment.value,
            }
            for item in news_items
        ],
        "overall_sentiment": overall,
        "positive_count": pos_count,
        "negative_count": neg_count,
        "neutral_count": sentiments.count(Sentiment.NEUTRAL),
    }


# Convenience list of news tools
NEWS_TOOLS = [get_stock_news]
