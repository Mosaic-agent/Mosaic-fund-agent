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
            # Try to resolve the final URL with a short timeout.
            # Some Google News redirects hang indefinitely.
            resp = _requests.head(raw, timeout=5, allow_redirects=True)
            if resp is not None and hasattr(resp, "url"):
                return resp.url
            return raw
        except Exception as e:
            logger.debug("GNews URL expansion failed for %s: %s", raw, e)
            return raw

    _gnews_utils.process_url = _process_url_with_timeout
except Exception:
    pass  # If the patch fails, gnews still works (just potentially slower)


# ── GNews Client ─────────────────────────────────────────────────────────────

def _make_gnews_client(lookback_days: int) -> GNews:
    """Create a GNews client configured for Indian English financial news with a dynamic lookback."""
    return GNews(
        language="en",
        country="IN",
        max_results=min(settings.news_articles_per_stock * 3, 30),
        period=f"{lookback_days}d",
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


def fetch_news_for_symbol(symbol: str, company_name: str = "", target_date: str = "") -> list[NewsItem]:
    """
    Fetch news articles for a given NSE/BSE stock symbol via Google News, filtered by target date.

    Args:
        symbol:       Zerodha trading symbol e.g. 'RELIANCE', 'TCS'
        company_name: Optional full company name for better query results.
        target_date:  Optional target date in YYYY-MM-DD format (or "today"). 
                      If omitted, defaults to today's date. Only articles published 
                      on this date are returned.

    Returns:
        List of NewsItem models.
    """
    import pytz
    from datetime import datetime
    from dateutil import parser as date_parser

    # Resolve target_dt
    if not target_date or target_date.lower() == "today":
        tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
        target_dt = datetime.now(tz).date()
    else:
        try:
            target_dt = date_parser.parse(target_date).date()
        except Exception as exc:
            logger.warning("Failed to parse target_date '%s': %s. Defaulting to today.", target_date, exc)
            tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
            target_dt = datetime.now(tz).date()

    # Calculate dynamic lookback needed to cover the target date
    tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
    today_dt = datetime.now(tz).date()
    days_diff = (today_dt - target_dt).days
    lookback_days = max(settings.news_lookback_days, days_diff + 1)

    client = _make_gnews_client(lookback_days)
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

    filtered_items: list[NewsItem] = []
    for article in articles:
        pub_date_str = article.get("published date", "")
        if not pub_date_str:
            continue
        try:
            pub_date = date_parser.parse(pub_date_str)
            if pub_date.tzinfo is not None:
                pub_date = pub_date.astimezone(tz)
            pub_dt = pub_date.date()
        except Exception:
            continue

        if pub_dt == target_dt:
            title = article.get("title") or ""
            description = article.get("description") or ""
            publisher = article.get("publisher", {})
            source = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)

            filtered_items.append(
                NewsItem(
                    title=title,
                    source=source,
                    published_at=str(article.get("published date", "")),
                    url=article.get("url") or "",
                    description=description,
                    sentiment=_infer_sentiment(f"{title} {description}"),
                )
            )

    items = filtered_items[: settings.news_articles_per_stock]

    logger.info(
        "Fetched %d news articles for %s matching date %s (lookback=%dd)",
        len(items),
        symbol,
        target_dt,
        lookback_days,
    )
    return items


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_stock_news(input_str: str, target_date: str = "") -> dict[str, Any]:
    """
    Fetch the latest Indian financial news for a stock symbol using Google News.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                   → searches for RELIANCE NSE stock news
      "TCS|Tata Consultancy"       → searches for Tata Consultancy NSE stock news

    Args:
        input_str:   The stock symbol (e.g., "RELIANCE") or symbol and company name.
        target_date: Optional. The target date in YYYY-MM-DD format (or "today"). 
                     If omitted, defaults to today's date. Only articles published 
                     on this date are returned.

    Returns a list of news articles with title, source, date, URL,
    sentiment (POSITIVE/NEUTRAL/NEGATIVE), and overall sentiment summary.

    Note: No API key required — powered by Google News RSS via gnews.
    """
    parts = input_str.strip().split("|")
    symbol = parts[0].strip().upper()
    company_name = parts[1].strip() if len(parts) > 1 else ""

    news_items = fetch_news_for_symbol(symbol, company_name, target_date)

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


@tool
def search_financial_news(query: str, max_results: int = 10, target_date: str = "") -> str:
    """
    Free-text financial news search via Google News (GNews).

    Use for broad queries that are not tied to a single stock symbol:
      - "Indian market news today"
      - "ETF news India"
      - "Nifty earnings results"
      - "RBI rate decision"
      - "budget 2026 India"

    Args:
        query:       The query string.
        max_results: Max results to return (default 10).
        target_date: Optional. The target date in YYYY-MM-DD format (or "today"). 
                     If omitted, defaults to today's date. Only articles published 
                     on this date are returned.

    Returns a Markdown table: Title | Source | Date | Sentiment
    """
    import pytz
    from datetime import datetime
    from dateutil import parser as date_parser

    # Resolve target_dt
    if not target_date or target_date.lower() == "today":
        tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
        target_dt = datetime.now(tz).date()
    else:
        try:
            target_dt = date_parser.parse(target_date).date()
        except Exception as exc:
            logger.warning("Failed to parse target_date '%s': %s. Defaulting to today.", target_date, exc)
            tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
            target_dt = datetime.now(tz).date()

    # Calculate dynamic lookback needed to cover the target date
    tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
    today_dt = datetime.now(tz).date()
    days_diff = (today_dt - target_dt).days
    lookback_days = max(settings.news_lookback_days, days_diff + 1)

    try:
        from gnews import GNews
        client = GNews(
            language="en",
            country="IN",
            max_results=min(max_results * 3, 30),
            period=f"{lookback_days}d",
        )
        articles = client.get_news(query)
        if not articles:
            return f"No news found for query: '{query}'"

        filtered_articles = []
        for a in articles:
            pub_date_str = a.get("published date", "")
            if not pub_date_str:
                continue
            try:
                pub_date = date_parser.parse(pub_date_str)
                if pub_date.tzinfo is not None:
                    pub_date = pub_date.astimezone(tz)
                pub_dt = pub_date.date()
            except Exception:
                continue

            if pub_dt == target_dt:
                filtered_articles.append(a)

        if not filtered_articles:
            return f"No news found for query: '{query}' on date: {target_dt}"

        lines = ["| Title | Source | Date | Sentiment |", "|---|---|---|---|"]
        for a in filtered_articles[:max_results]:
            title = (a.get("title") or "")[:90]
            pub   = a.get("publisher", {})
            src   = pub.get("title", "—") if isinstance(pub, dict) else str(pub)
            date  = str(a.get("published date", ""))[:16]
            desc  = a.get("description") or ""
            sent  = _infer_sentiment(f"{title} {desc}").value
            lines.append(f"| {title} | {src} | {date} | {sent} |")

        return "\n".join(lines)
    except Exception as exc:
        return f"Error fetching news for '{query}': {exc}"


@tool
def get_db_news(category: str = "", sentiment: str = "", limit: int = 20) -> str:
    """
    Query saved news articles from the ClickHouse news_articles table.

    Args:
        category:  ETF category filter (e.g. 'gold', 'silver', 'nifty', 'banking') — blank = all
        sentiment: Filter by 'positive', 'negative', 'neutral' — blank = all
        limit:     Max rows to return (default 20)

    Returns a Markdown table of recent saved articles.
    """
    conditions = ["1=1"]
    if category:
        conditions.append(f"lower(category) LIKE '%{category.lower()}%'")
    if sentiment:
        conditions.append(f"lower(sentiment) = '{sentiment.lower()}'")
    where = " AND ".join(conditions)
    sql = (
        f"SELECT fetched_at, category, sentiment, impact_tier, title, source "
        f"FROM market_data.news_articles FINAL "
        f"WHERE {where} "
        f"ORDER BY fetched_at DESC "
        f"LIMIT {min(limit, 100)}"
    )
    try:
        from src.db.pool import query_df
        df = query_df(sql)
        if df.empty:
            return "No saved news found matching the filters."
        return df.to_markdown(index=False)
    except Exception as exc:
        return f"DB news query error: {exc}"


# Convenience list of news tools
NEWS_TOOLS = [get_stock_news]
