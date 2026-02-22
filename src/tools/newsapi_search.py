"""
src/tools/newsapi_search.py
────────────────────────────
LangChain tool for fetching Indian financial news via NewsAPI.org.

Covers premium Indian financial publications:
  • The Economic Times (economictimes.indiatimes.com)
  • Business Standard (business-standard.com)
  • LiveMint (livemint.com)
  • Moneycontrol (moneycontrol.com)
  • NDTV Profit (profit.ndtv.com)
  • Financial Express (financialexpress.com)

Free tier: 100 requests/day, articles up to 30 days old.
[SENSITIVE] NEWSAPI_KEY must be set in .env
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from langchain_core.tools import tool
from newsapi import NewsApiClient

from config.settings import settings
from src.models.portfolio import NewsItem, Sentiment
from src.tools.news_search import _infer_sentiment

logger = logging.getLogger(__name__)

# ── Indian financial news domains ─────────────────────────────────────────────

INDIAN_NEWS_DOMAINS = (
    "economictimes.indiatimes.com,"
    "business-standard.com,"
    "livemint.com,"
    "moneycontrol.com,"
    "profit.ndtv.com,"
    "financialexpress.com,"
    "thehindu.com"
)


# ── Client factory ────────────────────────────────────────────────────────────

def _newsapi_client() -> NewsApiClient | None:
    """
    Build a NewsApiClient from the [SENSITIVE] NEWSAPI_KEY config field.
    Returns None if the key is not set.
    """
    key = settings.newsapi_key
    if not key:
        logger.warning(
            "NEWSAPI_KEY not configured — NewsAPI enrichment will be skipped. "
            "Set NEWSAPI_KEY in .env to enable premium Indian news sources."
        )
        return None
    return NewsApiClient(api_key=key)


# ── Fetcher ───────────────────────────────────────────────────────────────────

def fetch_newsapi_articles(symbol: str, company_name: str = "") -> list[NewsItem]:
    """
    Fetch recent articles for a stock symbol from NewsAPI.org.

    Args:
        symbol:       NSE/BSE trading symbol e.g. 'RELIANCE', 'TCS'
        company_name: Optional full company name for richer query coverage.

    Returns:
        List of NewsItem models; empty list if key not set or request fails.
    """
    client = _newsapi_client()
    if client is None:
        return []

    from_date = (
        datetime.utcnow() - timedelta(days=settings.news_lookback_days)
    ).strftime("%Y-%m-%d")

    # Company name in quotes gives best relevance for Indian financials
    query = f'"{company_name}" OR "{symbol}"' if company_name else symbol

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

    items: list[NewsItem] = []
    for article in response.get("articles", [])[: settings.news_articles_per_stock]:
        title = article.get("title") or ""
        description = article.get("description") or ""
        items.append(
            NewsItem(
                title=title,
                source=article.get("source", {}).get("name", ""),
                published_at=article.get("publishedAt", ""),
                url=article.get("url", ""),
                description=description,
                sentiment=_infer_sentiment(f"{title} {description}"),
            )
        )

    logger.info(
        "NewsAPI: fetched %d articles for %s (lookback=%dd)",
        len(items),
        symbol,
        settings.news_lookback_days,
    )
    return items


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_newsapi_stock_news(input_str: str) -> dict[str, Any]:
    """
    Fetch recent Indian financial news for a stock symbol via NewsAPI.org.

    Covers premium sources: Economic Times, Business Standard, LiveMint,
    Moneycontrol, NDTV Profit, Financial Express.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                  → searches for RELIANCE articles
      "TCS|Tata Consultancy"      → searches for Tata Consultancy OR TCS articles

    Returns articles with title, source, published_at, url, and per-article sentiment.
    Requires NEWSAPI_KEY in .env (free tier: 100 req/day).
    """
    parts = input_str.strip().split("|")
    symbol = parts[0].strip().upper()
    company_name = parts[1].strip() if len(parts) > 1 else ""

    items = fetch_newsapi_articles(symbol, company_name)

    if not items:
        return {
            "symbol": symbol,
            "source": "NewsAPI",
            "articles": [],
            "overall_sentiment": "NEUTRAL",
            "note": "No articles found or NEWSAPI_KEY not set.",
        }

    sentiments = [i.sentiment for i in items]
    pos = sentiments.count(Sentiment.POSITIVE)
    neg = sentiments.count(Sentiment.NEGATIVE)
    overall = "POSITIVE" if pos > neg else "NEGATIVE" if neg > pos else "NEUTRAL"

    return {
        "symbol": symbol,
        "source": "NewsAPI",
        "articles": [
            {
                "title": i.title,
                "source": i.source,
                "published_at": i.published_at,
                "url": i.url,
                "description": i.description,
                "sentiment": i.sentiment.value,
            }
            for i in items
        ],
        "overall_sentiment": overall,
        "positive_count": pos,
        "negative_count": neg,
        "neutral_count": sentiments.count(Sentiment.NEUTRAL),
    }


# Convenience list for registration in agent tool sets
NEWSAPI_TOOLS = [get_newsapi_stock_news]
