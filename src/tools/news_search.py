"""
src/tools/news_search.py
─────────────────────────
LangChain tool for fetching Indian financial news via NewsAPI.org.

Free tier limits:
  • 100 requests / day
  • Articles up to 30 days old
  • 100 articles per request (we cap at NEWS_ARTICLES_PER_STOCK)

News sources targeted for Indian markets:
  • The Economic Times (economictimes.indiatimes.com)
  • Business Standard (business-standard.com)
  • LiveMint (livemint.com)
  • Moneycontrol (moneycontrol.com)
  • NDTV Profit (profit.ndtv.com)

[SENSITIVE] NEWSAPI_KEY must be set in .env – never hard-coded here.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from langchain_core.tools import tool
from newsapi import NewsApiClient

from config.settings import settings
from src.models.portfolio import NewsItem, Sentiment

logger = logging.getLogger(__name__)

# ── Indian financial news domains ─────────────────────────────────────────────
INDIAN_NEWS_DOMAINS = (
    "economictimes.indiatimes.com,"
    "business-standard.com,"
    "livemint.com,"
    "moneycontrol.com,"
    "profit.ndtv.com,"
    "financialexpress.com,"
    "thehinduBusinessLine.com"
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


def _newsapi_client() -> NewsApiClient | None:
    """
    Create a NewsApiClient using the [SENSITIVE] NEWSAPI_KEY from config.
    Returns None if the key is not configured.
    """
    # [SENSITIVE] Key loaded from config/settings.py → .env
    key = settings.newsapi_key
    if not key:
        logger.warning(
            "NEWSAPI_KEY is not set. News enrichment will be skipped. "
            "Get a free key at https://newsapi.org/register"
        )
        return None
    return NewsApiClient(api_key=key)


def fetch_news_for_symbol(symbol: str, company_name: str = "") -> list[NewsItem]:
    """
    Fetch recent news articles for a given NSE/BSE stock symbol.

    Args:
        symbol:       Zerodha trading symbol e.g. 'RELIANCE', 'TCS'
        company_name: Optional full company name for better query results.

    Returns:
        List of NewsItem models (up to NEWS_ARTICLES_PER_STOCK from config).
    """
    client = _newsapi_client()
    if client is None:
        return []

    from_date = (datetime.utcnow() - timedelta(days=settings.news_lookback_days)).strftime(
        "%Y-%m-%d"
    )

    # Build query: symbol + company name gives best Indian market coverage
    query = symbol
    if company_name:
        query = f'"{company_name}" OR "{symbol}"'

    try:
        response = client.get_everything(
            q=query,
            domains=INDIAN_NEWS_DOMAINS,
            from_param=from_date,
            language="en",
            sort_by="publishedAt",
            page_size=settings.news_articles_per_stock,
        )
    except Exception as exc:
        logger.warning("NewsAPI request failed for %s: %s", symbol, exc)
        return []

    articles = response.get("articles", [])
    items: list[NewsItem] = []

    for article in articles[: settings.news_articles_per_stock]:
        title = article.get("title") or ""
        description = article.get("description") or ""
        combined_text = f"{title} {description}"

        items.append(
            NewsItem(
                title=title,
                source=article.get("source", {}).get("name", ""),
                published_at=article.get("publishedAt", ""),
                url=article.get("url", ""),
                description=description,
                sentiment=_infer_sentiment(combined_text),
            )
        )

    logger.info(
        "Fetched %d news articles for %s (lookback=%d days)",
        len(items),
        symbol,
        settings.news_lookback_days,
    )
    return items


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_stock_news(input_str: str) -> dict[str, Any]:
    """
    Fetch the latest Indian financial news for a stock symbol using NewsAPI.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                   → searches for RELIANCE news
      "TCS|Tata Consultancy"       → searches for TCS OR Tata Consultancy news

    Returns a list of news articles with title, source, date, URL,
    sentiment (POSITIVE/NEUTRAL/NEGATIVE), and overall sentiment summary.

    Note: Requires NEWSAPI_KEY in .env. Free tier allows 100 requests/day.
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
            "note": "No articles found or NewsAPI key not configured.",
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
