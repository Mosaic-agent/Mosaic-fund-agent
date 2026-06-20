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
            log.warning("RAG retrieval failed for %s: %s — falling back to live GNews", symbol, e)

        # Fallback: live GNews if retrieval returned <5 results
        if len(events) < 5:
            live_events = self._fetch_live_gnews(symbol, lookback_days)
            self._persist_live_news(live_events, symbol)
            events.extend(live_events)

        return events

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
                max_results=20,
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
    def _persist_live_news(events: List[CandidateEvent], symbol: str) -> None:
        """Persist live-fetched news back to ClickHouse for cache warming."""
        if not events:
            return
        try:
            from src.importer.clickhouse import ClickHouseImporter
            importer = ClickHouseImporter()
            rows = []
            for ev in events:
                if ev.event_type != EventType.NEWS_ANNOUNCEMENT:
                    continue
                rows.append({
                    "fetched_at": datetime.now(),
                    "published_at": str(ev.trade_date),
                    "source_type": "correlation_live",
                    "fetch_source": "gnews",
                    "category": symbol,
                    "etfs_impacted": symbol,
                    "sentiment": "NEUTRAL",
                    "impact_tier": "",
                    "title": ev.label,
                    "description": ev.description,
                    "source": ev.metadata.get("source", ""),
                    "url": ev.metadata.get("url", ""),
                })
            if rows:
                importer.insert_news_articles(rows)
                log.info("Persisted %d live news articles for %s", len(rows), symbol)
        except Exception as e:
            log.debug("Could not persist live news for %s: %s", symbol, e)
