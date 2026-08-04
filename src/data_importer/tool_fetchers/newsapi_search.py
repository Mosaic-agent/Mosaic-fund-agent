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
from src.utils.cache import cache_age_seconds, cache_get, cache_set

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

def fetch_newsapi_articles(symbol: str, company_name: str = "", target_date: str = "") -> list[NewsItem]:
    """
    Fetch recent articles for a stock symbol from NewsAPI.org, filtered by target date.

    Responses are cached for newsapi_cache_ttl_seconds (default 1 hour) to
    protect the free-tier 100 req/day quota from repeated runs.

    Args:
        symbol:       NSE/BSE trading symbol e.g. 'RELIANCE', 'TCS'
        company_name: Optional full company name for richer query coverage.
        target_date:  Optional target date in YYYY-MM-DD format (or "today"). 
                      If omitted, defaults to today's date. Only articles published 
                      on this date are returned.

    Returns:
        List of NewsItem models; empty list if key not set or request fails.
    """
    import pytz
    from datetime import datetime
    from dateutil import parser as date_parser

    # Resolve target_dt
    tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
    if not target_date or target_date.lower() == "today":
        target_dt = datetime.now(tz).date()
    else:
        try:
            target_dt = date_parser.parse(target_date).date()
        except Exception as exc:
            logger.warning("Failed to parse target_date '%s': %s. Defaulting to today.", target_date, exc)
            target_dt = datetime.now(tz).date()

    target_date_str = target_dt.strftime("%Y-%m-%d")

    # ── Cache check ──────────────────────────────────────────────────────────
    _cache_key = f"newsapi_{symbol.upper()}_{target_date_str}"
    _ttl = settings.newsapi_cache_ttl_seconds
    if _ttl > 0:
        cached_raw = cache_get(_cache_key, ttl_seconds=_ttl)
        if cached_raw is not None:
            age = cache_age_seconds(_cache_key) or 0
            logger.info(
                "NewsAPI: serving cached response for %s matching date %s (age %.0fm%.0fs / TTL %dh)",
                symbol, target_date_str, age // 60, age % 60, _ttl // 3600,
            )
            # Re-hydrate dicts back to NewsItem models
            return [
                NewsItem(
                    title=a["title"],
                    source=a["source"],
                    published_at=a["published_at"],
                    url=a["url"],
                    description=a.get("description", ""),
                    sentiment=Sentiment(a["sentiment"]),
                )
                for a in cached_raw
            ]

    client = _newsapi_client()
    if client is None:
        return []

    # Company name in quotes gives best relevance for Indian financials
    query = f'"{company_name}" OR "{symbol} share" OR "{symbol} stock"' if company_name else f'"{symbol} share" OR "{symbol} stock"'

    # Query a real lookback window (capped at NewsAPI's free-tier 30-day limit)
    # instead of the single calendar day `target_date` — a single-day query
    # against a handful of Indian domains rarely has ANY match, which starved
    # thinly-covered stocks (e.g. ITC) of both the returned answer and the
    # historical RAG corpus built from everything fetched here.
    today_dt = datetime.now(tz).date()
    from_date_str = (target_dt - timedelta(days=min(settings.news_lookback_days, 30))).strftime("%Y-%m-%d")
    to_date_str = max(target_dt, today_dt).strftime("%Y-%m-%d")

    try:
        response = client.get_everything(
            q=query,
            domains=INDIAN_NEWS_DOMAINS,
            from_param=from_date_str,
            to=to_date_str,
            language="en",
            sort_by="publishedAt",
            page_size=max(settings.news_articles_per_stock * 3, 20),
        )
    except Exception as exc:
        logger.warning("NewsAPI request failed for %s (%s to %s): %s", symbol, from_date_str, to_date_str, exc)
        return []

    all_items: list[NewsItem] = []
    items: list[NewsItem] = []
    for article in response.get("articles", []):
        title = article.get("title") or ""
        description = article.get("description") or ""
        published_at = article.get("publishedAt", "")
        news_item = NewsItem(
            title=title,
            source=article.get("source", {}).get("name", ""),
            published_at=published_at,
            url=article.get("url", ""),
            description=description,
            sentiment=_infer_sentiment(f"{title} {description}"),
        )
        all_items.append(news_item)
        try:
            pub_dt = date_parser.parse(published_at).date() if published_at else None
        except Exception:
            pub_dt = None
        if pub_dt == target_dt:
            items.append(news_item)

    items = items[: settings.news_articles_per_stock]

    logger.info(
        "NewsAPI: fetched %d articles (%s to %s) for %s; %d matching date %s",
        len(all_items), from_date_str, to_date_str, symbol, len(items), target_date_str,
    )

    # Index everything fetched (not just the date-matched subset returned to
    # the caller) so researching a stock builds real historical RAG coverage —
    # previously these premium-source articles only ever lived in the
    # short-TTL disk cache below and were never part of the symbol's
    # historical corpus used by correlation/RAG queries.
    if all_items:
        from src.tools.news_search import _cache_articles_to_qdrant
        _cache_articles_to_qdrant(symbol, all_items)

    # Persist to cache so next call within TTL window skips the HTTP request
    if _ttl > 0 and items:
        cache_set(
            _cache_key,
            [
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
        )
        logger.debug("NewsAPI: response cached under '%s' (TTL %dh)", _cache_key, _ttl // 3600)

    return items


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_newsapi_stock_news(input_str: str, target_date: str = "") -> dict[str, Any]:
    """
    Fetch recent Indian financial news for a stock symbol via NewsAPI.org.

    Covers premium sources: Economic Times, Business Standard, LiveMint,
    Moneycontrol, NDTV Profit, Financial Express.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                  → searches for RELIANCE articles
      "TCS|Tata Consultancy"      → searches for Tata Consultancy OR TCS articles

    Args:
        input_str:   The stock symbol (e.g., "RELIANCE") or symbol and company name.
        target_date: Optional. The target date in YYYY-MM-DD format (or "today"). 
                     If omitted, defaults to today's date. Only articles published 
                     on this date are returned.

    Returns articles with title, source, published_at, url, and per-article sentiment.
    Requires NEWSAPI_KEY in .env (free tier: 100 req/day).
    """
    parts = input_str.strip().split("|")
    symbol = parts[0].strip().upper()
    company_name = parts[1].strip() if len(parts) > 1 else ""

    items = fetch_newsapi_articles(symbol, company_name, target_date)

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
