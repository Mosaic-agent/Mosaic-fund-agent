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
import re
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
        df_ohlcv: Optional[pd.DataFrame] = None,
    ) -> List[CandidateEvent]:
        """Load events from all registered sources.

        df_ohlcv (optional): the symbol's price frame. When provided, PELT
        structural breaks in its return distribution are added as REGIME_SHIFT
        candidate events, so an anomaly landing near a regime transition can be
        framed as such (and prompt attribution of the macro/news that drove it).
        """
        events: List[CandidateEvent] = []
        events.extend(self._from_corporate_actions(df_corp))
        events.extend(self._from_nse_announcements(symbol, lookback_days))
        events.extend(self._from_macro_milestones())
        events.extend(self._from_news(symbol, lookback_days))
        if df_ohlcv is not None:
            events.extend(self._from_regime_shifts(df_ohlcv))
        return events

    # ── Regime shifts (PELT structural breaks) ────────────────────────────────
    #
    # Adaptive multi-penalty scan logic now lives in
    # src/ml/anomaly/_changepoint.py::fit_change_points_adaptive() (shared with
    # PeltChangePointStrategy in the anomaly-detection pipeline, which was
    # found to use a single fixed penalty that detected zero breaks on 21/22
    # tested symbols).

    @staticmethod
    def _from_regime_shifts(df_ohlcv: pd.DataFrame) -> List[CandidateEvent]:
        """Detect volatility-regime breaks (PELT on log-returns) as events,
        with an adaptive penalty so stocks and ETFs detect at comparable rates."""
        events: List[CandidateEvent] = []
        try:
            import numpy as np
            from src.ml.anomaly._changepoint import fit_change_points_adaptive

            df = df_ohlcv.copy()
            if "trade_date" not in df.columns or "close" not in df.columns or len(df) < 60:
                return events
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            if "log_return" not in df.columns:
                df["log_return"] = np.log(df["close"] / df["close"].shift(1))

            res = fit_change_points_adaptive(df)
            if not bool(res["is_changepoint"].any()):
                return events          # genuinely no clear regime shift

            for bd in res.loc[res["is_changepoint"], "trade_date"]:
                events.append(
                    CandidateEvent(
                        trade_date=pd.Timestamp(bd).date(),
                        event_type=EventType.REGIME_SHIFT,
                        label="Volatility Regime Shift (PELT structural break)",
                        description=(
                            "The return distribution changed variance/mean regime "
                            "around this date — anomalies here are part of a regime "
                            "transition, not isolated blips."
                        ),
                        metadata={"detector": "pelt"},
                    )
                )
        except Exception as e:
            log.warning("Regime-shift event detection failed: %s", e)
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

    # ── NSE Corporate Announcements (official disclosures) ────────────────────

    # Boilerplate NSE filing-description prefixes that carry no information —
    # stripped so what's left (if anything) is the actual substantive detail.
    _NSE_DESC_BOILERPLATE_RE = re.compile(
        r'^.*?\b(?:has informed the exchange|has submitted to the exchange|'
        r'has submitted the exchange)\b(?:\s+(?:regarding|about|a copy of|the|that))?\s*',
        re.IGNORECASE,
    )
    # A quoted sub-title inside the filing text (e.g. `titled "Press Release on
    # the financial results for the quarter ended June 30, 2026"`) is usually
    # the single most informative fragment NSE provides.
    _NSE_QUOTED_TITLE_RE = re.compile(r'["\u201c]([^"\u201d]{10,150})["\u201d]')

    @staticmethod
    def _informative_filing_label(category: str, raw_description: str) -> str:
        """Build a more informative event label than NSE's bare category
        (e.g. "Press Release", "Resignation", "General Updates").

        NSE's `desc` field is a coarse category bucket; the real substance
        (which press release, whose resignation, what the board decided) sits
        in the free-text `attchmntText`. This pulls that fact out — quoted
        sub-titles first, otherwise the description with generic
        "has informed the Exchange ..." boilerplate stripped — and prefixes
        the category only when the detail doesn't already restate it, so
        category keywords used by materiality weighting stay present.
        Falls back to the bare category when the description adds nothing
        beyond it (e.g. a bare "Credit Rating" filing with no attached text).
        """
        if not raw_description:
            return category

        m = EventRegistry._NSE_QUOTED_TITLE_RE.search(raw_description)
        if m:
            detail = m.group(1).strip()
        else:
            detail = EventRegistry._NSE_DESC_BOILERPLATE_RE.sub("", raw_description)
            detail = detail.strip().lstrip(",:;- ").strip().rstrip(".")

        if not detail or detail.lower() == category.lower():
            return category
        detail = detail[:140]
        if detail and detail[0].islower():
            detail = detail[0].upper() + detail[1:]
        if detail.lower().startswith(category.lower()):
            return detail
        return f"{category}: {detail}"

    @staticmethod
    def _from_nse_announcements(symbol: str, lookback_days: int) -> List[CandidateEvent]:
        """Official NSE announcements — board outcomes, M&A, credit ratings,
        management changes. Ground-truth company-filing events, far more
        authoritative for attribution than aggregated news (which only ever
        found 0-2 articles per stock in practice)."""
        events: List[CandidateEvent] = []
        try:
            from datetime import timedelta

            from src.tools.nse_announcements import fetch_corporate_announcements

            to_dt = date.today()
            from_dt = to_dt - timedelta(days=lookback_days)
            rows = fetch_corporate_announcements(symbol, from_dt, to_dt)
            for r in rows:
                try:
                    pub_dt = datetime.fromisoformat(r["published_at"]).date()
                except Exception:
                    continue
                cat = str(r.get("category") or "").strip()
                title = str(r.get("title") or "").strip()
                if not cat or cat.lower() in ("nse_announcements", "general updates", "updates"):
                    if ":" in title:
                        cat = title.split(":", 1)[1].strip()
                    else:
                        cat = title or "Corporate Disclosure"

                raw_desc = str(r.get("description") or "").strip()
                desc = raw_desc
                if not desc or desc.lower() == "nse_announcements":
                    desc = title or cat
                    raw_desc = ""  # placeholder fallback text carries no real detail

                events.append(
                    CandidateEvent(
                        trade_date=pub_dt,
                        event_type=EventType.COMPANY_FILING,
                        label=EventRegistry._informative_filing_label(cat, raw_desc),
                        description=desc,
                        metadata={"source": "nse", "url": r.get("url", ""), "category": cat},
                    )
                )
        except Exception as e:
            log.warning("NSE announcements event source failed for %s: %s", symbol, e)
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
            (date(2026, 1, 12), EventType.MACRO_GEOPOLITICAL, "Global Trade War Tariffs", "Geopolitical tensions trigger worldwide supply shock"),
            # India commodity policy
            (date(2026, 5, 13), EventType.MACRO_RATE_DECISION, "India Gold Import Duty Hike to 15%", "Government raises gold import duty from 6% to 15%; caps duty-free imports at 100kg per licence. Bearish for gold demand, bullish for domestic gold prices short-term on supply squeeze."),
        ]
        return [
            CandidateEvent(trade_date=dt, event_type=ev_type, label=label, description=desc)
            for dt, ev_type, label, desc in milestones
        ]

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
                symbol=symbol,
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
                    existing_titles.add(ge.label.lower().strip())
            # 3. Fetch from Yahoo Finance — not rate-limited/blocked like GNews
            # scraping, and often covers lower-profile stocks the other two miss.
            yahoo_events = self._fetch_live_yahoo(symbol)
            for ye in yahoo_events:
                if ye.label.lower().strip() not in existing_titles:
                    live_events.append(ye)
                    existing_titles.add(ye.label.lower().strip())

            # Embed and index/persist in Qdrant (RAG) & ClickHouse
            self._persist_and_index_live_news(live_events, symbol)
            
            for le in live_events:
                if is_relevant_article(le.label.lower(), le.description.lower()):
                    filtered_events.append(le)

        return self._dedupe_same_day_news(filtered_events)

    # Near-duplicate news are treated as the SAME candidate when their semantic
    # similarity is >= this cutoff. Empirically calibrated on GOLDBEES: same-day
    # syndicated "gold rate today" recaps from different publishers (identical
    # information, different wording/cities) scored 0.82-0.91 cosine similarity
    # against each other; genuinely distinct same-day stories (e.g. "ETF inflows
    # hit 5-month high" vs "Central banks snapping up gold") scored 0.60-0.78.
    # 0.82 sits just below the recap cluster and above the distinct-story band.
    _NEWS_DEDUPE_SIM_THRESHOLD = 0.82

    @staticmethod
    def _dedupe_same_day_news(events: List[CandidateEvent]) -> List[CandidateEvent]:
        """Collapse near-duplicate same-day news candidates before scoring.

        Syndicated outlets (FXStreet, Economic Times, Business Standard, ...)
        often republish the exact same routine fact (e.g. "gold rate today: X
        in Mumbai/Delhi/...") independently on the same date. These are
        semantically near-identical but textually distinct, so exact
        title/url dedup in retrieve_articles() doesn't catch them — they
        become separate CandidateEvents, wasting retrieval budget and
        correlation-strategy compute (they're near-universally filtered out
        downstream by news quality scoring anyway, but crowd out genuinely
        distinct same-day stories from the fixed top-k retrieval).

        Greedy single-linkage clustering within each trade_date: for each
        event (in original relevance-ranked order), keep it only if its
        embedding similarity to every already-kept event on that date is
        below the threshold; otherwise it's folded into an existing cluster
        and dropped as redundant.
        """
        if len(events) < 2:
            return events

        by_date: dict = {}
        for ev in events:
            by_date.setdefault(ev.trade_date, []).append(ev)

        # Only dates with 2+ candidates need embedding/clustering.
        multi_date_events = [ev for evs in by_date.values() if len(evs) > 1 for ev in evs]
        if not multi_date_events:
            return events

        try:
            from .news_rag import embed_batch
            import numpy as np

            labels = [ev.label for ev in multi_date_events]
            vecs = np.array(embed_batch(labels), dtype=np.float32)
            norms = np.linalg.norm(vecs, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            vecs_n = vecs / norms
            vec_by_id = {id(ev): vecs_n[i] for i, ev in enumerate(multi_date_events)}
        except Exception as e:
            log.debug("News dedup embedding failed, skipping clustering: %s", e)
            return events

        deduped: List[CandidateEvent] = []
        for trade_date, evs in by_date.items():
            if len(evs) == 1:
                deduped.append(evs[0])
                continue
            kept: List[CandidateEvent] = []
            for ev in evs:
                v = vec_by_id.get(id(ev))
                if v is None:
                    kept.append(ev)
                    continue
                is_dup = any(
                    float(np.dot(v, vec_by_id[id(k)])) >= EventRegistry._NEWS_DEDUPE_SIM_THRESHOLD
                    for k in kept
                    if vec_by_id.get(id(k)) is not None
                )
                if not is_dup:
                    kept.append(ev)
            if len(kept) < len(evs):
                log.debug(
                    "News dedup on %s: %d candidates → %d after collapsing near-duplicates",
                    trade_date, len(evs), len(kept),
                )
            deduped.extend(kept)

        return deduped

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
            from src.tools.news_search import _gnews_get_news

            company_name = get_company_name(symbol)
            query = f"{symbol} {company_name}" if company_name else f"{symbol} NSE"
            articles = _gnews_get_news(client, query)
            if not articles:
                articles = _gnews_get_news(client, symbol)

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
    def _fetch_live_yahoo(symbol: str) -> List[CandidateEvent]:
        """Live Yahoo Finance news fetcher — not rate-limited/blocked like
        GNews scraping, and often covers lower-profile stocks the other two
        sources miss. Yahoo's `.news` only returns recent items (no lookback
        param), so this is a coverage top-up, not a full historical source."""
        try:
            import yfinance as yf
            from dateutil import parser as date_parser
            from src.tools.yahoo_finance import _build_yf_symbol

            yf_symbol = _build_yf_symbol(symbol, "NSE")
            raw_news = yf.Ticker(yf_symbol).news or []

            events: List[CandidateEvent] = []
            for n in raw_news:
                c = n.get("content") or {}
                title = c.get("title") or ""
                if not title:
                    continue
                pub_date_str = c.get("pubDate") or c.get("displayTime") or ""
                if not pub_date_str:
                    continue
                try:
                    pub_dt = date_parser.parse(pub_date_str).date()
                except Exception:
                    continue

                desc = c.get("summary") or c.get("description") or ""
                provider = c.get("provider") or {}
                source = provider.get("displayName", "") if isinstance(provider, dict) else str(provider)
                url = (
                    (c.get("canonicalUrl") or {}).get("url")
                    or (c.get("clickThroughUrl") or {}).get("url")
                    or ""
                )

                events.append(
                    CandidateEvent(
                        trade_date=pub_dt,
                        event_type=EventType.NEWS_ANNOUNCEMENT,
                        label=title,
                        description=desc,
                        metadata={"source": source, "url": url},
                    )
                )
            return events
        except Exception as e:
            log.warning("Live Yahoo news fetch failed for %s: %s", symbol, e)
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
                    # category is a consistent kind label ("stock_news"); the ticker
                    # goes in the dedicated `symbol` field so a symbol pre-filter can
                    # find this live-fetched news later (previously category=symbol
                    # and symbol was unset → stored as "", making it unfindable and
                    # forcing the live fetch to re-fire on every run).
                    "category": "stock_news",
                    "symbol": symbol,
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
