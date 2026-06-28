"""
src/ml/correlation/event_registry.py
─────────────────────────────────────
Loads candidate events from all sources: corporate actions, hardcoded macro
milestones, dynamic FX shocks, and news (RAG + live GNews fallback).

Each event source is a method — add new sources by subclassing or by adding
methods and calling them from `load_all()`.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from .models import CandidateEvent, EventType

log = logging.getLogger(__name__)


class EventRegistry:
    """Aggregates candidate events from multiple data sources."""

    def load_all(
        self,
        symbol: str,
        df_corp: Optional[pd.DataFrame],
        lookback_days: int = 365,
    ) -> List[CandidateEvent]:
        """Load events from all registered sources."""
        events: List[CandidateEvent] = []
        events.extend(self._from_corporate_actions(df_corp))
        events.extend(self._from_macro_milestones())
        events.extend(self._from_fx_shocks())
        events.extend(self._from_news(symbol, lookback_days))
        return events

    # ── Corporate Actions ─────────────────────────────────────────────────────

    @staticmethod
    def load_corp_actions(symbol: str) -> Optional[pd.DataFrame]:
        """Fetch corporate actions DataFrame from ClickHouse."""
        try:
            from src.db.pool import query_df
            _ca = query_df(
                "SELECT ex_date, action_type, ratio, purpose "
                "FROM market_data.corporate_actions FINAL "
                "WHERE symbol = {sym:String}",
                parameters={"sym": symbol.upper()},
            )
            if not _ca.empty:
                _ca["ex_date"] = pd.to_datetime(_ca["ex_date"])
                return _ca
        except Exception as e:
            log.warning("Failed to load corporate actions for %s: %s", symbol, e)
        return None

    @staticmethod
    def _from_corporate_actions(df_corp: Optional[pd.DataFrame]) -> List[CandidateEvent]:
        """Convert corporate actions DataFrame to CandidateEvents."""
        events: List[CandidateEvent] = []
        if df_corp is None or df_corp.empty:
            return events

        for _, row in df_corp.iterrows():
            ex_date = pd.to_datetime(row["ex_date"]).date()
            action_type = str(row["action_type"])
            ratio = str(row["ratio"])
            purpose = str(row["purpose"])

            events.append(
                CandidateEvent(
                    trade_date=ex_date,
                    event_type=EventType.COMPANY_FILING,
                    label=f"{action_type.upper()} ({ratio})" if ratio else action_type.upper(),
                    description=purpose,
                    metadata={"action_type": action_type, "ratio": ratio},
                )
            )
        return events

    # ── Hardcoded Macro Milestones ────────────────────────────────────────────

    @staticmethod
    def _from_macro_milestones() -> List[CandidateEvent]:
        """Major rate decisions, geopolitical events, and commodity policy shocks."""
        milestones = [
            # Fed decisions
            (date(2025, 9, 18), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-50 bps)", "Fed pivot kicks off policy easing cycle"),
            (date(2025, 11, 7), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-25 bps)", "Fed rate cut following election results"),
            (date(2025, 12, 18), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-25 bps)", "Final Fed easing of 2025"),
            (date(2026, 3, 19), EventType.MACRO_RATE_DECISION, "US Fed Meeting Pause", "Fed holds rates steady amid sticky inflation"),
            # RBI decisions
            (date(2025, 10, 9), EventType.MACRO_RATE_DECISION, "RBI Policy Pause", "RBI holds repo rate at 6.50%"),
            (date(2025, 12, 5), EventType.MACRO_RATE_DECISION, "RBI Repo Rate Cut (-25 bps)", "RBI starts monetary easing cycle"),
            (date(2026, 2, 6), EventType.MACRO_RATE_DECISION, "RBI Policy Pause", "RBI pauses rate cuts to monitor food inflation"),
            # Geopolitical
            (date(2025, 10, 1), EventType.MACRO_GEOPOLITICAL, "Middle East Geopolitical Escalation", "Spike in energy and global risk off sentiment"),
            (date(2026, 1, 12), EventType.MACRO_GEOPOLITICAL, "Global Trade War Tariff Tariffs", "Geopolitical tensions trigger worldwide supply shock"),
            # India commodity policy
            (date(2026, 5, 13), EventType.MACRO_RATE_DECISION, "India Gold Import Duty Hike to 15%", "Government raises gold import duty from 6% to 15%; caps duty-free imports at 100kg per licence. Bearish for gold demand, bullish for domestic gold prices short-term on supply squeeze."),
        ]
        return [
            CandidateEvent(trade_date=dt, event_type=ev_type, label=label, description=desc)
            for dt, ev_type, label, desc in milestones
        ]

    # ── Dynamic FX Shocks ─────────────────────────────────────────────────────

    # Minimum daily USDINR move to qualify as a macro shock candidate.
    # The earlier 0.0075 (0.75%) threshold produced ~30+ events per year, which
    # caused the PostMacroShockStrategy to attach an FX event to nearly every
    # stock anomaly within its ±3-day window. Raised to 1.00% to surface only
    # genuinely market-moving USDINR days.
    _FX_SHOCK_MIN_PCT = 0.01

    @staticmethod
    def _from_fx_shocks() -> List[CandidateEvent]:
        """USDINR daily moves ≥ ``_FX_SHOCK_MIN_PCT`` from ClickHouse fx_rates."""
        events: List[CandidateEvent] = []
        try:
            from src.db.pool import query_df
            df_fx = query_df(
                "SELECT trade_date, toFloat64(close) AS close "
                "FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR' "
                "ORDER BY trade_date ASC"
            )
            if not df_fx.empty:
                df_fx["trade_date"] = pd.to_datetime(df_fx["trade_date"])
                df_fx["pct_change"] = df_fx["close"].pct_change()
                extreme_fx = df_fx[df_fx["pct_change"].abs() >= EventRegistry._FX_SHOCK_MIN_PCT]
                for _, row in extreme_fx.iterrows():
                    fx_date = pd.to_datetime(row["trade_date"]).date()
                    pct = float(row["pct_change"])
                    direction = "Depreciation" if pct > 0 else "Appreciation"
                    events.append(
                        CandidateEvent(
                            trade_date=fx_date,
                            event_type=EventType.MACRO_COMMODITY_SHOCK,
                            label=f"USDINR {direction} ({pct*100:+.2f}%)",
                            description="Significant daily currency volatility shock in INR exchange rates.",
                            metadata={"fx_pct_change": float(pct)},
                        )
                    )
        except Exception as e:
            log.warning("Could not dynamically build USDINR macro events: %s", e)
        return events

    # ── News (RAG + Live Fallback) ────────────────────────────────────────────

    def _from_news(self, symbol: str, lookback_days: int) -> List[CandidateEvent]:
        """
        Semantic RAG retrieval (primary) with live GNews fallback for cold-start.
        Persists live-fetched articles back to ClickHouse for future retrieval.
        """
        from src.utils.symbol_mapper import get_company_name

        company_name = get_company_name(symbol) or symbol
        query_text = f"{symbol} {company_name} price"
        today = date.today()

        events: List[CandidateEvent] = []

        # Primary: semantic retrieval from embedded news_articles
        try:
            from .news_rag import retrieve_articles
            articles = retrieve_articles(
                query=query_text,
                around_date=today,
                days=lookback_days,
                k=20,
            )
            for a in articles:
                pub_str = a.get("published_at", "")
                try:
                    from dateutil import parser as date_parser
                    pub_date = date_parser.parse(pub_str).date() if pub_str else today
                except Exception:
                    pub_date = today

                events.append(
                    CandidateEvent(
                        trade_date=pub_date,
                        event_type=EventType.NEWS_ANNOUNCEMENT,
                        label=a.get("title", ""),
                        description="",
                        metadata={
                            "source": a.get("source", ""),
                            "url": a.get("url", ""),
                            "similarity": a.get("similarity", 0.0),
                        },
                    )
                )
        except Exception as e:
            log.warning("RAG retrieval failed for %s: %s", symbol, e)

        # Check how many retrieved articles actually mention the symbol or company name
        # to distinguish between genuine stock news and general market near-neighbors.
        stock_specific_count = 0
        symbol_lower = symbol.lower()
        company_clean = company_name.lower()
        for noise in ["ltd", "limited", "industries", "group", "india", "corp", "corporation"]:
            company_clean = company_clean.replace(noise, "")
        company_words = {w.strip() for w in company_clean.split() if len(w.strip()) > 3}

        # Specific overrides for common English words
        symbol_overrides = {
            "reliance": ["reliance industries", "ril", "jio", "ambani", "reliance retail", "reliance power", "reliance infra", "reliance share", "reliance stock", "reliance group", "reliance digital"],
            "titan": ["titan company", "titan share", "titan stock", "titan watch", "titan jewellery", "titan eye", "tanishq"],
        }

        import re
        def is_relevant_article(t_lower: str, d_lower: str) -> bool:
            if symbol_lower in symbol_overrides:
                for term in symbol_overrides[symbol_lower]:
                    if len(term) < 5:
                        pattern = rf"\b{re.escape(term)}\b"
                        if re.search(pattern, t_lower) or re.search(pattern, d_lower):
                            return True
                    else:
                        if term in t_lower or term in d_lower:
                            return True
                return False
            else:
                if len(symbol_lower) < 5:
                    pattern = rf"\b{re.escape(symbol_lower)}\b"
                    has_match = bool(re.search(pattern, t_lower) or re.search(pattern, d_lower))
                else:
                    has_match = (symbol_lower in t_lower) or (symbol_lower in d_lower)
                
                if not has_match and company_words:
                    for w in company_words:
                        if len(w) < 5:
                            pattern = rf"\b{re.escape(w)}\b"
                            if re.search(pattern, t_lower) or re.search(pattern, d_lower):
                                return True
                        else:
                            if w in t_lower or w in d_lower:
                                return True
                return has_match

        # Keep ONLY articles that are genuinely relevant to the stock
        filtered_events = []
        for ev in events:
            if is_relevant_article(ev.label.lower(), ev.description.lower()):
                stock_specific_count += 1
                filtered_events.append(ev)

        # Fallback: live news if RAG returned < 3 stock-specific articles
        if stock_specific_count < 3:
            log.info("RAG contains only %d stock-specific articles for %s — triggering live fetch", stock_specific_count, symbol)
            live_events = []
            # 1. Fetch from NewsAPI first (if API key available, up to 30 days)
            live_events.extend(self._fetch_live_newsapi(symbol, lookback_days))
            # 2. Fetch from GNews to cover the entire historical lookback window
            gnews_events = self._fetch_live_gnews(symbol, lookback_days)
            existing_titles = {e.label.lower().strip() for e in live_events}
            for ge in gnews_events:
                if ge.label.lower().strip() not in existing_titles:
                    live_events.append(ge)
            
            # Embed and index/persist in Qdrant (RAG) & ClickHouse
            self._persist_and_index_live_news(live_events, symbol)
            
            for le in live_events:
                if is_relevant_article(le.label.lower(), le.description.lower()):
                    filtered_events.append(le)

        return filtered_events

    @staticmethod
    def _fetch_live_newsapi(symbol: str, lookback_days: int) -> List[CandidateEvent]:
        """Fetch news from NewsAPI.org for the given lookback period (max 30 days for free tier)."""
        from config.settings import settings
        from datetime import timedelta
        if not settings.newsapi_key or "your_" in settings.newsapi_key:
            return []
        
        try:
            from newsapi import NewsApiClient
            from dateutil import parser as date_parser
            from src.utils.symbol_mapper import get_company_name
            import pytz

            client = NewsApiClient(api_key=settings.newsapi_key)
            company_name = get_company_name(symbol)
            query = f'"{company_name}" OR "{symbol} share" OR "{symbol} stock"' if company_name else f'"{symbol} share" OR "{symbol} stock"'
            
            # NewsAPI free tier allows maximum 30 days lookback
            api_lookback = min(lookback_days, 30)
            from_date = (date.today() - timedelta(days=api_lookback)).strftime("%Y-%m-%d")
            to_date = date.today().strftime("%Y-%m-%d")

            response = client.get_everything(
                q=query,
                domains="economictimes.indiatimes.com,business-standard.com,livemint.com,moneycontrol.com,profit.ndtv.com,financialexpress.com,thehindu.com",
                from_param=from_date,
                to=to_date,
                language="en",
                sort_by="publishedAt",
                page_size=30,
            )
            
            events: List[CandidateEvent] = []
            tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
            for a in response.get("articles", []):
                pub_date_str = a.get("publishedAt", "")
                if not pub_date_str:
                    continue
                try:
                    pub_date = date_parser.parse(pub_date_str)
                    if pub_date.tzinfo is not None:
                        pub_date = pub_date.astimezone(tz)
                    pub_dt = pub_date.date()
                except Exception:
                    continue

                title = a.get("title") or ""
                desc = a.get("description") or ""
                source = a.get("source", {}).get("name", "") if isinstance(a.get("source"), dict) else str(a.get("source", ""))

                events.append(
                    CandidateEvent(
                        trade_date=pub_dt,
                        event_type=EventType.NEWS_ANNOUNCEMENT,
                        label=title,
                        description=desc,
                        metadata={
                            "source": source,
                            "url": a.get("url") or "",
                            "published_at": pub_date_str,
                        },
                    )
                )
            log.info("Fetched %d live NewsAPI articles for %s", len(events), symbol)
            return events
        except Exception as e:
            log.warning("Live NewsAPI fetch failed for %s: %s", symbol, e)
            return []

    @staticmethod
    def _fetch_live_gnews(symbol: str, lookback_days: int) -> List[CandidateEvent]:
        """Live GNews fallback fetcher."""
        try:
            from gnews import GNews
            from config.settings import settings
            from dateutil import parser as date_parser
            import pytz
            from src.utils.symbol_mapper import get_company_name

            client = GNews(
                language="en",
                country="IN",
                max_results=100,
                period=f"{lookback_days}d",
            )
            company_name = get_company_name(symbol)
            query = f"{symbol} {company_name}" if company_name else f"{symbol} NSE"
            articles = client.get_news(query)
            if not articles:
                articles = client.get_news(symbol)

            events: List[CandidateEvent] = []
            tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
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

                title = a.get("title") or ""
                desc = a.get("description") or ""
                publisher = a.get("publisher", {})
                source = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)

                events.append(
                    CandidateEvent(
                        trade_date=pub_dt,
                        event_type=EventType.NEWS_ANNOUNCEMENT,
                        label=title,
                        description=desc,
                        metadata={"source": source, "url": a.get("url") or ""},
                    )
                )
            return events
        except Exception as e:
            log.warning("Live GNews fetch failed for %s: %s", symbol, e)
            return []

    @staticmethod
    def _persist_and_index_live_news(events: List[CandidateEvent], symbol: str) -> None:
        """Persist live-fetched news back to ClickHouse and index in Qdrant (RAG) for future retrieval."""
        if not events:
            return
        
        # 1. Persist to ClickHouse
        try:
            from src.importer.clickhouse import ClickHouseImporter
            importer = ClickHouseImporter()
            rows = []
            for ev in events:
                if ev.event_type != EventType.NEWS_ANNOUNCEMENT:
                    continue
                url = ev.metadata.get("url", "")
                is_gnews = "news.google.com" in url if url else False
                rows.append({
                    "fetched_at": datetime.now(),
                    "published_at": str(ev.trade_date),
                    "source_type": "correlation_live",
                    "fetch_source": "gnews" if is_gnews else "newsapi",
                    "category": symbol,
                    "etfs_impacted": symbol,
                    "sentiment": "NEUTRAL",
                    "impact_tier": "",
                    "title": ev.label,
                    "description": ev.description,
                    "source": ev.metadata.get("source", ""),
                    "url": url,
                })
            if rows:
                importer.insert_news_articles(rows)
                log.info("Persisted %d live news articles to ClickHouse for %s", len(rows), symbol)
        except Exception as e:
            log.debug("Could not persist live news to ClickHouse for %s: %s", symbol, e)

        # 2. Embed and Index in Qdrant (RAG)
        try:
            from .news_rag import embed_batch, upsert_to_qdrant
            articles_to_index = []
            texts_to_embed = []
            for ev in events:
                if ev.event_type != EventType.NEWS_ANNOUNCEMENT:
                    continue
                
                pub_at = ev.metadata.get("published_at") or ev.trade_date.strftime("%Y-%m-%d")
                
                articles_to_index.append({
                    "title": ev.label,
                    "source": ev.metadata.get("source", ""),
                    "url": ev.metadata.get("url", ""),
                    "published_at": pub_at,
                    "category": symbol,
                    "sentiment": "NEUTRAL",
                })
                texts_to_embed.append(f"{ev.label} {ev.description}")
            
            if articles_to_index:
                vectors = embed_batch(texts_to_embed)
                if vectors:
                    upsert_to_qdrant(articles_to_index, vectors)
                    log.info("Indexed %d live news articles in Qdrant RAG for %s", len(articles_to_index), symbol)
        except Exception as e:
            log.warning("Could not index live news in Qdrant RAG for %s: %s", symbol, e)
