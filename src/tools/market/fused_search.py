"""Cross-collection fused search across Qdrant vector collections.

Combines results from market_anomalies, mf_holdings, and news_articles
collections to provide a 360° view of signals for a single symbol.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from langchain_core.tools import tool

log = logging.getLogger(__name__)


@tool
def fused_multi_signal_search(
    symbol: str,
    days_back: int = 30,
) -> str:
    """Search across anomalies, MF holdings, and news for a single symbol.

    Fuses results from market_anomalies, mf_holdings, and news_articles
    Qdrant collections to give a 360° view of recent signals for a
    stock or ETF.  Use when the user asks for a holistic overview of
    what is happening with a particular asset.

    Args:
        symbol: The ticker symbol to search (e.g., 'GOLDBEES', 'WELCORP').
        days_back: How many days back to search for anomalies and news.
    """
    sections: list[str] = []
    cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()
    symbol_upper = symbol.upper().strip()

    # ── 1. Anomalies ──────────────────────────────────────────────────────
    try:
        from src.db.anomaly_vector import retrieve_similar_anomalies

        anomalies = retrieve_similar_anomalies(
            symbol=symbol_upper,
            regime="",
            trade_date=date.today(),
            k=8,
            same_asset_only=True,
        )
        recent = [a for a in anomalies if a.get("trade_date", "") >= cutoff_date]
        if recent:
            sections.append("### 🔴 Recent Anomalies")
            for a in recent:
                attr = a.get("attributed_event_type", "")
                attr_tag = f" → {attr}" if attr else ""
                sections.append(
                    f"- **{a['trade_date']}** | {a.get('regime', '?')} | "
                    f"z={a.get('final_z', 0):.2f} "
                    f"ret={a.get('daily_return', 0):.2f}%{attr_tag}"
                )
    except Exception as e:
        log.debug("Fused search anomaly leg failed: %s", e)

    # ── 2. MF Holdings ────────────────────────────────────────────────────
    try:
        from src.db.mf_vector import find_funds_holding_security

        holdings = find_funds_holding_security(query=symbol_upper, k=10)
        if holdings:
            sections.append("\n### 🏦 Institutional Holdings (Qdrant)")
            for h in holdings:
                sections.append(
                    f"- **{h.get('fund_name', '?')}** | "
                    f"{h.get('pct_of_nav', 0):.2f}% NAV | "
                    f"₹{h.get('market_value_cr', 0):.1f} Cr | "
                    f"as of {h.get('as_of_month', '?')}"
                )
    except Exception as e:
        log.debug("Fused search MF leg failed: %s", e)

    # ── 3. News ───────────────────────────────────────────────────────────
    try:
        from src.ml.correlation.news_rag import retrieve_articles

        news = retrieve_articles(symbol=symbol_upper, limit=5)
        if news:
            sections.append("\n### 📰 Recent News")
            for n in news:
                title = n.get("title", n.get("headline", "?"))
                pub = n.get("published_date", n.get("published_at", "?"))
                sent = n.get("sentiment", "?")
                sections.append(f"- [{pub}] {title} ({sent})")
    except Exception as e:
        log.debug("Fused search news leg failed: %s", e)

    if not sections:
        return f"No cross-collection signals found for {symbol_upper} in the last {days_back} days."

    return f"## 360° Signal Fusion: {symbol_upper}\n\n" + "\n".join(sections)


FUSED_SEARCH_TOOLS = [fused_multi_signal_search]
