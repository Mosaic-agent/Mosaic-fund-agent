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
import random
import time
from typing import Any

from gnews import GNews
from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import NewsItem, Sentiment

logger = logging.getLogger(__name__)

# ── Retry-with-backoff for GNews ───────────────────────────────────────────────
# gnews scrapes Google News RSS with no official API. A blocked/rate-limited
# request comes back as HTTP 503 ("unusual traffic") from Google, but the
# gnews library swallows that internally and just returns an empty list —
# indistinguishable from "genuinely no articles" unless we retry. Retrying
# with backoff recovers short-lived throttling; a longer IP-level block will
# still exhaust retries and fall through to empty (same as today), just after
# a bounded wait instead of failing on the first try.
_GNEWS_MAX_RETRIES = 3
_GNEWS_BASE_DELAY_SECONDS = 3.0


def _gnews_get_news(client: GNews, query: str) -> list:
    """client.get_news(query) with retry-on-empty + exponential backoff + jitter."""
    for attempt in range(_GNEWS_MAX_RETRIES):
        try:
            articles = client.get_news(query)
        except Exception as exc:
            logger.debug(
                "GNews request failed (attempt %d/%d) for %r: %s",
                attempt + 1, _GNEWS_MAX_RETRIES, query, exc,
            )
            articles = None
        if articles:
            return articles
        if attempt < _GNEWS_MAX_RETRIES - 1:
            delay = _GNEWS_BASE_DELAY_SECONDS * (2 ** attempt) + random.uniform(0, 1.0)
            logger.debug(
                "GNews returned no results for %r (attempt %d/%d) — retrying in %.1fs",
                query, attempt + 1, _GNEWS_MAX_RETRIES, delay,
            )
            time.sleep(delay)
    return []

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
        target_date:  Optional target date in YYYY-MM-DD format. 
                      If omitted, returns the most recent articles over the lookback window.
                      If provided, only articles published on this date are returned.

    Returns:
        List of NewsItem models.
    """
    import pytz
    from datetime import datetime
    from dateutil import parser as date_parser

    # Resolve target_dt
    if not target_date or target_date.lower() == "today" or target_date.lower() == "recent":
        target_dt = None
    else:
        try:
            target_dt = date_parser.parse(target_date).date()
        except Exception as exc:
            logger.warning("Failed to parse target_date '%s': %s. Defaulting to recent.", target_date, exc)
            target_dt = None

    # RAG-first: check the Qdrant cache before touching the network. A prior
    # call (this run, an earlier run, or a different sub-agent) may have
    # already fetched and embedded this symbol/date.
    try:
        from src.ml.correlation.news_rag import retrieve_cached_news_for_symbol
        
        # Only use exact-match cache if a specific date was requested
        cached = []
        if target_dt:
            cached = retrieve_cached_news_for_symbol(symbol, target_dt.isoformat())
    except Exception as exc:
        logger.debug("News cache lookup skipped for %s: %s", symbol, exc)
        cached = []

    if cached:
        logger.info("News cache hit for %s/%s: %d article(s) — skipping live fetch", symbol, target_dt, len(cached))
        return [
            NewsItem(
                title=c.get("title", ""),
                source=c.get("source", ""),
                published_at=c.get("published_at", ""),
                url=c.get("url", ""),
                description="",
                sentiment=Sentiment(c.get("sentiment", "NEUTRAL")),
            )
            for c in cached[: settings.news_articles_per_stock]
        ]

    # Calculate dynamic lookback needed to cover the target date if provided
    lookback_days = settings.news_lookback_days
    if target_dt:
        tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
        today_dt = datetime.now(tz).date()
        days_diff = (today_dt - target_dt).days
        lookback_days = max(settings.news_lookback_days, days_diff + 1)

    client = _make_gnews_client(lookback_days)
    query = f"{company_name} NSE stock" if company_name else f"{symbol} NSE stock"

    articles = _gnews_get_news(client, query)

    # Fallback: bare symbol query if primary returned nothing
    if not articles:
        articles = _gnews_get_news(client, symbol)

    filtered_items: list[NewsItem] = []
    all_items: list[NewsItem] = []
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

        title = article.get("title") or ""
        description = article.get("description") or ""
        publisher = article.get("publisher", {})
        source = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)
        news_item = NewsItem(
            title=title,
            source=source,
            published_at=str(article.get("published date", "")),
            url=article.get("url") or "",
            description=description,
            sentiment=_infer_sentiment(f"{title} {description}"),
        )
        # Every fetched article — regardless of date — is a candidate for RAG
        # indexing below, so a stock's Qdrant corpus builds up across its full
        # lookback window instead of only ever capturing "today"'s news.
        all_items.append(news_item)
        if target_dt is None or pub_dt == target_dt:
            filtered_items.append(news_item)

    items = filtered_items[: settings.news_articles_per_stock]

    logger.info(
        "Fetched %d news articles for %s matching date %s (lookback=%dd); %d total fetched for RAG indexing",
        len(items),
        symbol,
        target_dt,
        lookback_days,
        len(all_items),
    )

    # Index everything fetched (not just the today-filtered `items` returned
    # to the caller) so researching a stock builds real historical RAG
    # coverage for it — previously only same-day articles ever got indexed,
    # which starved thinly-researched stocks (e.g. ITC) of any usable corpus.
    _cache_articles_to_qdrant(symbol, all_items)

    return items


def _cache_articles_to_qdrant(symbol: str, items: list[NewsItem]) -> None:
    """Best-effort write-through: embed and upsert fetched articles (each under
    its own actual published date) so future RAG / correlation queries for
    this symbol have real historical coverage instead of re-fetching live."""
    if not items:
        return
    try:
        from dateutil import parser as date_parser

        from src.ml.correlation.news_rag import embed_batch, upsert_to_qdrant

        texts = [f"{item.title}. {item.description}" for item in items]
        vectors = embed_batch(texts)
        if not vectors or all(v == 0.0 for v in vectors[0]):
            return  # embeddings unavailable (e.g. Ollama down) — skip caching silently

        articles = []
        for item, vector in zip(items, vectors):
            try:
                published_date = date_parser.parse(item.published_at).date().isoformat()
            except Exception:
                published_date = ""
            articles.append({
                "title": item.title,
                "source": item.source,
                "url": item.url,
                "published_at": item.published_at,
                "published_date": published_date,
                "category": "stock_news",
                "sentiment": item.sentiment.value,
                "symbol": symbol,
            })
        upsert_to_qdrant(articles, vectors)
    except Exception as exc:
        logger.debug("News cache write skipped for %s: %s", symbol, exc)


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
        target_date: Optional. The target date in YYYY-MM-DD format. 
                     If omitted, returns the most recent news over the lookback window.
                     If provided, only articles published on this date are returned.

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
