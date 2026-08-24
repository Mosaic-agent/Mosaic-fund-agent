"""
src/tools/nse_announcements.py
────────────────────────────────
Fetches official NSE corporate announcements/disclosures — board meeting
outcomes, results, litigation, credit rating changes, M&A, management
changes. This is the ground-truth regulatory disclosure feed for "why did
this stock move", far more authoritative than aggregated news for
company-specific attribution.

Endpoint: https://www.nseindia.com/api/corporate-announcements
No API key required — NSE requires only a warmed-up session (cookies) and a
browser-like User-Agent, same pattern as nse_corporate_actions_fetcher.py.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

_NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporate-announcements"
_NSE_WARMUP = "https://www.nseindia.com/"
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}
_TIMEOUT = 15

# Routine/administrative announcement categories — compliance noise, not
# market-moving events. Excluded by default so they don't dilute correlation
# attribution or clutter the research-agent tool output.
_ROUTINE_CATEGORIES = {
    "copy of newspaper publication",
    "certificate under sebi (depositories and participants) regulations, 2018",
    "trading window",
    "record date",
    "analysts/institutional investor meet/con. call updates",
}


def _load_cached_announcements(symbol: str, from_date: date, to_date: date) -> list[dict[str, Any]]:
    """Check ClickHouse market_data.news_articles for cached announcements before hitting NSE."""
    try:
        from src.db.pool import query_df
        import pandas as pd

        df = query_df(
            """
            SELECT title, category, published_at, url, max(imported_at) as last_imported
            FROM market_data.news_articles FINAL
            WHERE etfs_impacted = {sym:String}
              AND category = 'nse_announcements'
            GROUP BY title, category, published_at, url
            ORDER BY published_at DESC
            """,
            parameters={"sym": symbol.upper()},
        )
        if not df.empty and len(df) >= 3:
            last_imported = df["last_imported"].max()
            if last_imported:
                now_dt = datetime.now()
                last_dt = pd.to_datetime(last_imported).to_pydatetime()
                # If imported within last 12 hours, return cache
                if (now_dt - last_dt).total_seconds() < 43200:
                    logger.info(
                        "Announcements cache hit for %s (%d records) — skipping live NSE fetch",
                        symbol.upper(), len(df),
                    )
                    res = []
                    for _, r in df.iterrows():
                        title = str(r["title"])
                        cat = str(r["category"])
                        if cat == "nse_announcements" or not cat:
                            if ":" in title:
                                cat = title.split(":", 1)[1].strip()
                            else:
                                cat = title
                        res.append({
                            "published_at": str(r["published_at"]),
                            "category": cat,
                            "title": title,
                            "description": title,
                            "url": r["url"],
                            "symbol": symbol.upper(),
                        })
                    return res
    except Exception as exc:
        logger.debug("ClickHouse announcements cache check skipped for %s: %s", symbol, exc)
    return []


def fetch_corporate_announcements(
    symbol: str, from_date: date, to_date: date, include_routine: bool = False
) -> list[dict[str, Any]]:
    """
    Fetch official NSE corporate announcements for a symbol within a date range.

    Returns dicts with: published_at (ISO), category, title, description, url, symbol.
    Routine/administrative categories are dropped by default — pass
    include_routine=True for an unfiltered audit view.
    """
    symbol_upper = symbol.strip().upper()

    # 1. Local ClickHouse / RAG first check — avoid re-fetching if fresh
    cached = _load_cached_announcements(symbol_upper, from_date, to_date)
    if cached:
        return cached

    rows: list[dict[str, Any]] = []
    data = None

    try:
        with httpx.Client(headers=_NSE_HEADERS, follow_redirects=True, timeout=_TIMEOUT) as client:
            # Warm-up: obtain NSE session cookies (required — NSE blocks cookie-less requests)
            client.get(_NSE_WARMUP, timeout=10)
            time.sleep(0.8)

            resp = client.get(
                _NSE_ANNOUNCEMENTS_URL,
                params={
                    "index": "equities",
                    "symbol": symbol_upper,
                    "from_date": from_date.strftime("%d-%m-%Y"),
                    "to_date": to_date.strftime("%d-%m-%Y"),
                },
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("NSE announcements fetch failed for %s: %s", symbol_upper, exc)
        return rows

    if not isinstance(data, list):
        return rows

    for item in data:
        category = str(item.get("desc") or "").strip()
        if not include_routine and category.lower() in _ROUTINE_CATEGORIES:
            continue

        sort_date = str(item.get("sort_date") or "")
        try:
            published_at = datetime.strptime(sort_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        rows.append({
            "published_at": published_at.isoformat(),
            "category": category or "General Updates",
            "title": f"{item.get('sm_name', symbol_upper)}: {category or 'General Updates'}",
            "description": str(item.get("attchmntText") or "").strip(),
            "url": item.get("attchmntFile") or "",
            "symbol": symbol_upper,
        })

    logger.info(
        "NSE announcements: %d material events fetched for %s (%s to %s)",
        len(rows), symbol_upper, from_date, to_date,
    )
    if rows:
        _cache_announcements_to_qdrant(symbol_upper, rows)
    return rows


def _cache_announcements_to_qdrant(symbol: str, rows: list[dict[str, Any]]) -> None:
    """Write-through: embed and upsert all NSE corporate announcements to Qdrant RAG
    and ClickHouse market_data.news_articles so they can be retrieved semantically."""
    if not rows:
        return
    try:
        from dateutil import parser as date_parser
        from src.ml.correlation.news_rag import embed_batch, upsert_to_qdrant
        from src.db.pool import query_df

        # Deduplication check: do NOT re-embed if already in ClickHouse/Qdrant
        existing_urls: set[str] = set()
        existing_titles: set[str] = set()
        try:
            df_existing = query_df(
                "SELECT url, title FROM market_data.news_articles FINAL WHERE etfs_impacted = {sym:String} AND category = 'nse_announcements'",
                parameters={"sym": symbol.upper()},
            )
            if not df_existing.empty:
                existing_urls = set(df_existing["url"].dropna())
                existing_titles = set(df_existing["title"].dropna())
        except Exception:
            pass

        new_rows = [r for r in rows if r.get("url") not in existing_urls and r.get("title") not in existing_titles]
        if not new_rows:
            logger.info("All %d announcements for %s already exist in RAG — skipping re-embedding", len(rows), symbol.upper())
            return

        texts = [f"{r.get('category', '')}: {r.get('title', '')}. {r.get('description', '')[:300]}" for r in new_rows]
        vectors = embed_batch(texts)
        if not vectors or all(v == 0.0 for v in vectors[0]):
            return  # embeddings unavailable

        articles = []
        for r, vector in zip(new_rows, vectors):
            pub_at = r.get("published_at", "")
            try:
                published_date = date_parser.parse(pub_at).date().isoformat()
            except Exception:
                published_date = pub_at[:10] if pub_at else ""

            articles.append({
                "title": r.get("title", ""),
                "source": "NSE Corporate Announcements",
                "url": r.get("url", ""),
                "published_at": pub_at,
                "published_date": published_date,
                "category": "nse_announcements",
                "sentiment": "neutral",
                "symbol": symbol,
                "description": r.get("description", "")[:400],
            })
        upsert_to_qdrant(articles, vectors)
        logger.info("Indexed %d NEW announcements for %s to Qdrant RAG", len(new_rows), symbol.upper())
    except Exception as exc:
        logger.debug("Announcements Qdrant cache write skipped for %s: %s", symbol, exc)

    # ClickHouse write-through
    try:
        from src.data_importer.clickhouse import ClickHouseImporter
        from src.db.pool import get_pool
        pool = get_pool()
        importer = ClickHouseImporter(client=pool.get_client())
        records = []
        for r in rows:
            pub_at = r.get("published_at", "")
            records.append({
                "symbol": symbol,
                "fetched_at": datetime.now(),
                "published_at": pub_at,
                "source_type": "REGULATORY",
                "fetch_source": "NSE",
                "category": "nse_announcements",
                "etfs_impacted": symbol,
                "sentiment": "NEUTRAL",
                "impact_tier": "1",
                "title": r.get("title", ""),
                "source": "NSE Corporate Announcements",
                "url": r.get("url", ""),
                "summary": r.get("description", "")[:500],
                "trade_date": pub_at[:10] if pub_at else date.today().isoformat(),
            })
        if records:
            importer.insert_news_articles(records)
    except Exception as exc:
        logger.debug("Announcements ClickHouse write skipped for %s: %s", symbol, exc)


class FormattedNSEAnnouncements(dict):
    """Dict subclass that renders as a clean Markdown table when stringified."""

    def __str__(self) -> str:
        symbol = self.get("symbol", "")
        count = self.get("total_count", 0)
        highlights = self.get("recent_highlights") or []
        if not highlights:
            return f"No material NSE announcements found for **{symbol}** in the last 365 days."

        lines = [
            f"**Official Disclosures:** `{count}` material events indexed in RAG\n",
            "| Date | Category / Disclosure | Specific Details |",
            "| :--- | :--- | :--- |",
        ]
        for h in highlights[:5]:
            dt = str(h.get("published_at") or "")[:10]
            cat = str(h.get("category") or "").strip()
            title = str(h.get("title") or "").replace("|", "-").strip()
            lines.append(f"| {dt} | {cat} | {title} |")

        return "\n".join(lines)


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_nse_announcements(input_str: str, query: str = "", target_date: str = "") -> dict[str, Any]:
    """
    Fetch official NSE corporate announcements/disclosures for an Indian
    stock over the last 365 days, index them into Qdrant RAG, and return
    a concise summary or semantically matched announcements.

    Input format: "SYMBOL" (e.g. "NUVOCO", "ITC")
    """
    symbol = input_str.strip().upper()
    to_dt = date.today()
    from_dt = to_dt - timedelta(days=365)
    rows = fetch_corporate_announcements(symbol, from_dt, to_dt)

    if not rows:
        return FormattedNSEAnnouncements({
            "symbol": symbol,
            "source": "NSE Corporate Announcements",
            "announcements": [],
            "total_count": 0,
            "recent_highlights": [],
            "note": "No material announcements found in the last 365 days.",
        })

    # Index all announcements into Qdrant & ClickHouse RAG
    _cache_announcements_to_qdrant(symbol, rows)

    # Category breakdown for concise RAG metadata
    cat_counts: dict[str, int] = {}
    for r in rows:
        cat = r.get("category", "General Updates")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    # Return top 5 recent highlights + summary
    highlights = [
        {
            "published_at": r["published_at"][:10],
            "category": r["category"],
            "title": r["title"][:120],
        }
        for r in rows[:5]
    ]

    return FormattedNSEAnnouncements({
        "symbol": symbol,
        "source": "NSE Corporate Announcements",
        "rag_status": f"Indexed {len(rows)} announcements into Qdrant RAG",
        "total_count": len(rows),
        "categories": cat_counts,
        "recent_highlights": highlights,
        "note": "Full corporate disclosures are stored in Qdrant RAG. The LLM can retrieve specific anomaly explanations via semantic search.",
    })
