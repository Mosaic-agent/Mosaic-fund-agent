from __future__ import annotations

from collections import defaultdict
from datetime import date

from ..base import DossierSection
from ..context import DossierContext


class NewsSection(DossierSection):
    """Single news path: the Qdrant RAG two-pass symbol→semantic-fallback
    retrieval (src.ml.correlation.news_rag.retrieve_articles), replacing the
    three parallel paths (GNews, NewsAPI, and a second bespoke raw-scroll
    Qdrant query) the deleted scripts ran for the same symbol."""

    key = "news"
    title = "Recent News (Qdrant RAG)"

    def fetch(self, ctx: DossierContext) -> str:
        from src.ml.correlation.news_rag import retrieve_articles

        articles = retrieve_articles(
            query=f"{ctx.company_name} {ctx.symbol} stock news",
            around_date=date.today(),
            days=30,
            k=30,
            symbol=ctx.symbol,
        )
        if not articles:
            return f"*No articles found for {ctx.symbol} in the last 30 days.*"

        clusters: dict[str, list[str]] = defaultdict(list)
        sentiments: dict[str, list[str]] = defaultdict(list)
        for a in articles:
            cat = str(a.get("category") or "General").strip() or "General"
            title = str(a.get("title") or "").strip()
            if title:
                clusters[cat].append(title)
            sentiments[cat].append(str(a.get("sentiment") or "NEUTRAL").upper())

        lines = [f"**{len(articles)} articles found**\n"]
        for cat, titles in sorted(clusters.items(), key=lambda x: -len(x[1]))[:6]:
            cat_sent = sentiments.get(cat, [])
            bullish = cat_sent.count("POSITIVE") + cat_sent.count("BULLISH")
            bearish = cat_sent.count("NEGATIVE") + cat_sent.count("BEARISH")
            label = "🟢 Bullish" if bullish > bearish else "🔴 Bearish" if bearish > bullish else "⚪ Neutral"
            lines.append(f"**{cat}** ({len(titles)} articles) — {label}")
            for t in titles[:3]:
                lines.append(f"  • {t}")
            if len(titles) > 3:
                lines.append(f"  *...and {len(titles) - 3} more*")
            lines.append("")
        return "\n".join(lines)
