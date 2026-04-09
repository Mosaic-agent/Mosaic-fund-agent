"""
src/tools/etf_news_scanner.py
──────────────────────────────
Free ETF-impact news scanner.

Fetches news that can move specific Indian ETF categories using:
  1. Google News RSS (via gnews  — no key, no quota)
  2. Yahoo Finance   (via yfinance — no key, already a dependency)

Each news item is tagged with the ETFs it can impact, a sentiment
score, and an impact-tier (HIGH / MEDIUM / LOW).

Public API
──────────
    scan_etf_news(categories=None, max_per_topic=5) -> ETFNewsReport
    print_etf_news_report(report)                  -> None  (Rich console)

No new dependencies — uses gnews + yfinance which are already in requirements.txt.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

# ── ETF → news topics mapping ────────────────────────────────────────────────
# Each entry:  category_name, etf_symbols, search_queries, impact_tier
# impact_tier: HIGH = directly prices the ETF, MEDIUM = sector driver, LOW = macro

ETF_NEWS_TOPICS: list[dict] = [
    # ── Gold / Precious Metals ────────────────────────────────────────────────
    {
        "category": "Gold ETFs",
        "etfs": ["GOLDBEES", "SILVERBEES"],
        "queries": [
            "gold price today India",
            "COMEX gold futures",
            "US dollar index DXY gold",
            "US Federal Reserve interest rate gold",
            "central bank gold reserves buying",
            "India RBI gold import duty",
            "gold ETF AUM outflow inflow",
        ],
        "yf_symbols": ["GC=F", "SI=F", "GLD", "IAU"],  # Yahoo tickers for news
        "impact": "HIGH",
    },
    # ── Broad Equity / Nifty 50 ───────────────────────────────────────────────
    {
        "category": "Nifty ETFs",
        "etfs": ["NIFTYBEES", "SETFNIF50", "HDFCNIFTY", "MONIFTY500"],
        "queries": [
            "Nifty 50 outlook India market",
            "FII DII India equity flows",
            "RBI monetary policy rate India",
            "India GDP growth forecast",
            "MSCI India index rebalance",
        ],
        "yf_symbols": ["^NSEI"],
        "impact": "HIGH",
    },
    # ── Banking ───────────────────────────────────────────────────────────────
    {
        "category": "Bank ETFs",
        "etfs": ["BANKBEES", "PSUBNKBEES"],
        "queries": [
            "RBI repo rate decision India bank",
            "India banking sector NPA credit growth",
            "PSU bank divestment privatisation",
            "India bank quarterly results",
            "RBI banking regulation NBFC",
        ],
        "yf_symbols": ["^NSEBANK"],
        "impact": "HIGH",
    },
    # ── IT / Technology ───────────────────────────────────────────────────────
    {
        "category": "IT ETFs",
        "etfs": ["ITBEES"],
        "queries": [
            "India IT sector TCS Infosys results",
            "US dollar rupee USDINR IT exports",
            "Nasdaq US tech outlook",
            "H1B visa US tech India IT",
            "India IT hiring demand revenue",
        ],
        "yf_symbols": ["^NDX", "INFY", "TCS.NS"],
        "impact": "HIGH",
    },
    # ── PSU / CPSE ────────────────────────────────────────────────────────────
    {
        "category": "PSU ETFs",
        "etfs": ["CPSEETF", "ICICIB22"],
        "queries": [
            "India PSU divestment government",
            "India CPSE ETF NAV",
            "India infrastructure government capex",
            "India budget fiscal deficit PSU",
        ],
        "yf_symbols": [],
        "impact": "MEDIUM",
    },
    # ── Mid & Small Cap ───────────────────────────────────────────────────────
    {
        "category": "Mid/Small Cap ETFs",
        "etfs": ["JUNIORBEES", "MID150BEES", "SMALL250"],
        "queries": [
            "Nifty midcap smallcap India rally",
            "India small cap mutual fund SIP",
            "India mid cap earnings results",
        ],
        "yf_symbols": ["^NSMIDCP"],
        "impact": "MEDIUM",
    },
    # ── Pharma ────────────────────────────────────────────────────────────────
    {
        "category": "Pharma ETFs",
        "etfs": ["PHARMABEES"],
        "queries": [
            "India pharma USFDA warning letter approval",
            "Sun Pharma Dr Reddy Cipla results",
            "India generic drug exports US",
        ],
        "yf_symbols": [],
        "impact": "HIGH",
    },
    # ── International ETFs ────────────────────────────────────────────────────
    {
        "category": "International ETFs",
        "etfs": ["MON100", "MAFANG", "HNGSNGBEES", "MAHKTECH", "MASPTOP50"],
        "queries": [
            "US Federal Reserve rate decision",
            "Nasdaq FAANG tech stock outlook",
            "Hang Seng Hong Kong market",
            "China tech regulation Alibaba Tencent",
            "S&P 500 correction rally",
        ],
        "yf_symbols": ["^NDX", "^HSI", "^GSPC"],
        "impact": "HIGH",
    },
    # ── Debt / Liquid ─────────────────────────────────────────────────────────
    {
        "category": "Debt / Liquid ETFs",
        "etfs": ["LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES"],
        "queries": [
            "RBI repo rate liquidity India",
            "India 10 year government bond yield",
            "India inflation CPI WPI RBI",
            "India g-sec gilt bond market",
        ],
        "yf_symbols": [],
        "impact": "HIGH",
    },
    # ── Auto ─────────────────────────────────────────────────────────────────
    {
        "category": "Auto ETFs",
        "etfs": ["AUTOBEES"],
        "queries": [
            "India auto sector sales EV",
            "Maruti Tata Motors results",
            "India electric vehicle policy subsidy",
        ],
        "yf_symbols": [],
        "impact": "MEDIUM",
    },
]

# ── Sentiment keywords ────────────────────────────────────────────────────────
_POS = {
    "surge", "rally", "gain", "profit", "record", "growth", "beat", "strong",
    "upgrade", "buy", "bullish", "outperform", "dividend", "expansion", "robust",
    "soar", "rise", "high", "positive", "boom", "inflow", "accumulate", "cut rate",
    "easing", "approval", "win", "award", "recovery", "rebound",
}
_NEG = {
    "fall", "drop", "loss", "crash", "decline", "miss", "weak", "sell", "bearish",
    "underperform", "cut", "downgrade", "risk", "concern", "fraud", "penalty",
    "regulatory", "debt", "pressure", "plunge", "slowdown", "warning", "default",
    "lawsuit", "outflow", "hike rate", "tightening", "sanction", "ban", "rejection",
}


def _sentiment(text: str) -> str:
    words = set(text.lower().split())
    pos = len(words & _POS)
    neg = len(words & _NEG)
    if pos > neg:
        return "POSITIVE"
    if neg > pos:
        return "NEGATIVE"
    return "NEUTRAL"


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ETFNewsItem:
    title: str
    source: str
    published_at: str
    url: str
    sentiment: str          # POSITIVE / NEGATIVE / NEUTRAL
    etfs_impacted: list[str]
    category: str
    impact_tier: str        # HIGH / MEDIUM / LOW
    fetch_source: str       # "gnews" | "yfinance"


@dataclass
class ETFNewsReport:
    as_of: str
    items: list[ETFNewsItem] = field(default_factory=list)
    categories_scanned: list[str] = field(default_factory=list)
    negative_count: int = 0
    positive_count: int = 0
    neutral_count: int = 0


# ── Fetchers ──────────────────────────────────────────────────────────────────

def _fetch_gnews(query: str, max_results: int = 5) -> list[dict]:
    """Fetch articles from Google News RSS via gnews (no key needed)."""
    try:
        from gnews import GNews
        client = GNews(language="en", country="IN", max_results=max_results, period="2d")
        return client.get_news(query) or []
    except Exception as exc:
        log.debug("gnews failed for '%s': %s", query, exc)
        return []


def _fetch_yfinance_news(symbol: str, max_results: int = 5) -> list[dict]:
    """Fetch news from Yahoo Finance for a given ticker (no key needed)."""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        raw = ticker.news or []
        results = []
        for item in raw[:max_results]:
            content = item.get("content", {})
            title   = content.get("title", "") or item.get("title", "")
            if not title:
                continue
            pub = content.get("pubDate", "") or item.get("providerPublishTime", "")
            if isinstance(pub, int):
                pub = datetime.fromtimestamp(pub).strftime("%Y-%m-%d %H:%M")
            provider = content.get("provider", {})
            source   = provider.get("displayName", "") if isinstance(provider, dict) else ""
            url_info = content.get("canonicalUrl", {})
            url      = url_info.get("url", "") if isinstance(url_info, dict) else ""
            results.append({
                "title":          title,
                "source":         source or symbol,
                "published date": str(pub),
                "url":            url,
                "description":    "",
            })
        return results
    except Exception as exc:
        log.debug("yfinance news failed for '%s': %s", symbol, exc)
        return []


def _deduplicate(items: list[ETFNewsItem]) -> list[ETFNewsItem]:
    """Remove near-duplicate titles (first 60 chars key)."""
    seen: set[str] = set()
    out: list[ETFNewsItem] = []
    for item in items:
        key = item.title[:60].lower().strip()
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


# ── Main scanner ──────────────────────────────────────────────────────────────

def scan_etf_news(
    categories: Optional[list[str]] = None,
    max_per_topic: int = 4,
) -> ETFNewsReport:
    """
    Fetch ETF-impacting news for all (or selected) ETF categories.

    Parameters
    ----------
    categories    : list of category names to scan; None = all categories
    max_per_topic : max articles to fetch per search query / YF symbol

    Returns
    -------
    ETFNewsReport with deduplicated, tagged, sentiment-scored articles.
    """
    report = ETFNewsReport(as_of=datetime.now().strftime("%Y-%m-%d %H:%M IST"))
    raw_items: list[ETFNewsItem] = []

    topics = ETF_NEWS_TOPICS
    if categories:
        cat_lower = {c.lower() for c in categories}
        topics = [t for t in topics if t["category"].lower() in cat_lower]

    for topic in topics:
        cat      = topic["category"]
        etfs     = topic["etfs"]
        impact   = topic["impact"]
        report.categories_scanned.append(cat)

        # ── Google News ───────────────────────────────────────────────────────
        for query in topic["queries"]:
            articles = _fetch_gnews(query, max_results=max_per_topic)
            for art in articles:
                title = art.get("title", "")
                if not title:
                    continue
                desc      = art.get("description", "") or ""
                publisher = art.get("publisher", {})
                source    = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)
                pub_date  = str(art.get("published date", ""))
                url       = art.get("url", "")
                raw_items.append(ETFNewsItem(
                    title=title,
                    source=source,
                    published_at=pub_date,
                    url=url,
                    sentiment=_sentiment(f"{title} {desc}"),
                    etfs_impacted=etfs,
                    category=cat,
                    impact_tier=impact,
                    fetch_source="gnews",
                ))

        # ── Yahoo Finance news ────────────────────────────────────────────────
        for yf_sym in topic.get("yf_symbols", []):
            articles = _fetch_yfinance_news(yf_sym, max_results=max_per_topic)
            for art in articles:
                title = art.get("title", "")
                if not title:
                    continue
                raw_items.append(ETFNewsItem(
                    title=title,
                    source=art.get("source", yf_sym),
                    published_at=art.get("published date", ""),
                    url=art.get("url", ""),
                    sentiment=_sentiment(title),
                    etfs_impacted=etfs,
                    category=cat,
                    impact_tier=impact,
                    fetch_source="yfinance",
                ))

    # Deduplicate and count sentiment
    report.items = _deduplicate(raw_items)
    report.positive_count = sum(1 for i in report.items if i.sentiment == "POSITIVE")
    report.negative_count = sum(1 for i in report.items if i.sentiment == "NEGATIVE")
    report.neutral_count  = sum(1 for i in report.items if i.sentiment == "NEUTRAL")

    log.info(
        "ETF news scan: %d articles across %d categories (pos=%d neg=%d neu=%d)",
        len(report.items), len(report.categories_scanned),
        report.positive_count, report.negative_count, report.neutral_count,
    )
    return report


# ── Rich console printer ──────────────────────────────────────────────────────

def print_etf_news_report(report: ETFNewsReport) -> None:
    """Print a formatted ETF news report to the Rich console."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich import box

    console = Console()

    # Header
    console.print(Panel(
        f"[bold cyan]ETF-Impact News Scanner[/bold cyan]\n"
        f"[dim]As of {report.as_of}  •  "
        f"{len(report.items)} articles  •  "
        f"[green]↑{report.positive_count}[/green] "
        f"[red]↓{report.negative_count}[/red] "
        f"[dim]→{report.neutral_count}[/dim]\n"
        f"Sources: Google News RSS + Yahoo Finance  (no API key)[/dim]",
        border_style="cyan",
    ))

    # Group by category
    from collections import defaultdict
    by_cat: dict[str, list[ETFNewsItem]] = defaultdict(list)
    for item in report.items:
        by_cat[item.category].append(item)

    _SENT_COLOR = {"POSITIVE": "green", "NEGATIVE": "red", "NEUTRAL": "dim"}
    _IMPACT_COLOR = {"HIGH": "bold red", "MEDIUM": "yellow", "LOW": "dim"}

    for cat, items in by_cat.items():
        # Category header
        etfs_str = " ".join(f"[cyan]{e}[/cyan]" for e in items[0].etfs_impacted)
        impact   = items[0].impact_tier
        console.print(f"\n[bold white]{cat}[/bold white]  [{_IMPACT_COLOR[impact]}]{impact} IMPACT[/]  {etfs_str}")

        tbl = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        tbl.add_column("sent",    width=2)
        tbl.add_column("title",   style="white", no_wrap=False, ratio=7)
        tbl.add_column("source",  style="dim",   ratio=2)
        tbl.add_column("date",    style="dim",   ratio=2)

        for item in items[:8]:   # max 8 per category
            sent_icon = {"POSITIVE": "🟢", "NEGATIVE": "🔴", "NEUTRAL": "⚪"}[item.sentiment]
            tbl.add_row(sent_icon, item.title, item.source, item.published_at[:16])

        console.print(tbl)


# ── CLI smoke test ────────────────────────────────────────────────────────────

def save_etf_news_to_db(report: ETFNewsReport, ch_client) -> int:
    """
    Persist an ETFNewsReport to market_data.news_articles in ClickHouse.

    Parameters
    ----------
    report    : ETFNewsReport returned by scan_etf_news()
    ch_client : clickhouse_connect client (already connected)

    Returns number of rows inserted.
    """
    from datetime import datetime
    fetched_at = datetime.now()
    rows = [
        {
            "fetched_at":    fetched_at,
            "published_at":  item.published_at,
            "source_type":   "etf_news",
            "category":      item.category,
            "etfs_impacted": ",".join(item.etfs_impacted),
            "sentiment":     item.sentiment,
            "impact_tier":   item.impact_tier,
            "title":         item.title,
            "source":        item.source,
            "url":           item.url,
        }
        for item in report.items
    ]
    if not rows:
        return 0
    n = ch_client.insert_news_articles(rows)
    log.info("Saved %d ETF news articles to ClickHouse", n)
    return n


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    logging.basicConfig(level=logging.INFO)

    cats = sys.argv[1:] if len(sys.argv) > 1 else None
    report = scan_etf_news(categories=cats, max_per_topic=4)
    print_etf_news_report(report)
