"""
src/tools/yahoo_finance.py
──────────────────────────
LangChain tool wrapper around yfinance for Indian equity market data.

Provides end-of-day prices, financial metrics, sector info, and
52-week range for NSE/BSE listed stocks using Yahoo Finance's
free API (no API key required).

Symbol conventions:
  • NSE stocks: RELIANCE.NS, TCS.NS, INFY.NS
  • BSE stocks: RELIANCE.BO, TCS.BO
  • Indian ETFs: NIFTYBEES.NS, GOLDBEES.NS
"""

from __future__ import annotations

import logging
import time
from typing import Any

import yfinance as yf
from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import NewsItem, Sentiment, YahooFinanceData
from src.data_importer.tool_fetchers.news_search import _infer_sentiment

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_yf_symbol(symbol: str, exchange: str = "NSE") -> str:
    """
    Convert a Zerodha trading symbol to Yahoo Finance ticker.

    Examples:
      RELIANCE + NSE  →  RELIANCE.NS
      RELIANCE + BSE  →  RELIANCE.BO
      ADSK + US      →  ADSK
    """
    # Already has suffix, starts with caret, contains a dot/equals, or ends with =F – return as-is
    if (
        symbol.endswith(".NS")
        or symbol.endswith(".BO")
        or symbol.startswith("^")
        or symbol.endswith("=F")
        or "." in symbol
        or "=" in symbol
    ):
        return symbol

    if exchange.upper() in ("US", "NASDAQ", "NYSE"):
        return symbol

    suffix = settings.bse_suffix if exchange.upper() == "BSE" else settings.nse_suffix
    return f"{symbol}{suffix}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely coerce a value to float, returning default on failure."""
    try:
        if value is None:
            return default
        f = float(value)
        return f if not (f != f) else default  # NaN guard
    except (TypeError, ValueError):
        return default


def fetch_yahoo_data(symbol: str, exchange: str = "NSE") -> YahooFinanceData:
    """
    Fetch financial data for a single Indian stock from Yahoo Finance.

    Args:
        symbol:   Zerodha trading symbol e.g. 'RELIANCE'
        exchange: 'NSE' (default) or 'BSE'

    Returns:
        YahooFinanceData model populated from Yahoo Finance info dict.
    """
    yf_symbol = _build_yf_symbol(symbol, exchange)
    logger.info("Fetching Yahoo Finance data for %s", yf_symbol)

    try:
        ticker = yf.Ticker(yf_symbol)
        info: dict[str, Any] = ticker.info or {}

        return YahooFinanceData(
            symbol=yf_symbol,
            sector=info.get("sector", ""),
            industry=info.get("industry", ""),
            # Market cap: Yahoo returns in USD for Indian stocks sometimes;
            # we keep as-is since it's comparative rather than absolute here
            market_cap=_safe_float(info.get("marketCap")),
            pe_ratio=_safe_float(info.get("trailingPE")),
            pb_ratio=_safe_float(info.get("priceToBook")),
            # yfinance now returns dividendYield already as a percent (e.g. 5.52, not 0.0552)
            dividend_yield=_safe_float(info.get("dividendYield")),
            fifty_two_week_high=_safe_float(info.get("fiftyTwoWeekHigh")),
            fifty_two_week_low=_safe_float(info.get("fiftyTwoWeekLow")),
            current_price=_safe_float(
                info.get("currentPrice") or info.get("regularMarketPrice")
            ),
            description=info.get("longBusinessSummary", ""),
        )

    except Exception as exc:
        logger.warning("Yahoo Finance fetch failed for %s: %s", yf_symbol, exc)
        return YahooFinanceData(symbol=yf_symbol)


def fetch_price_history(
    symbol: str,
    exchange: str = "NSE",
    period: str = "3mo",
    start_date: str = "",
    end_date: str = "",
) -> list[dict[str, Any]]:
    """
    Fetch historical OHLCV data for an Indian stock.

    Args:
        symbol:     Zerodha trading symbol e.g. 'RELIANCE'
        exchange:   'NSE' (default) or 'BSE'
        period:     yfinance period string: '1mo', '3mo', '6mo', '1y'
        start_date: Optional start date in YYYY-MM-DD format (e.g. '2019-01-01')
        end_date:   Optional end date in YYYY-MM-DD format (e.g. '2019-12-31')

    Returns:
        List of dicts with date, open, high, low, close, volume.
    """
    yf_symbol = _build_yf_symbol(symbol, exchange)
    try:
        ticker = yf.Ticker(yf_symbol)
        if start_date:
            hist = ticker.history(start=start_date, end=end_date or None)
        else:
            hist = ticker.history(period=period)
        if hist.empty:
            return []
        records = []
        for date, row in hist.iterrows():
            records.append(
                {
                    "date": str(date.date()),
                    "open": round(_safe_float(row.get("Open")), 2),
                    "high": round(_safe_float(row.get("High")), 2),
                    "low": round(_safe_float(row.get("Low")), 2),
                    "close": round(_safe_float(row.get("Close")), 2),
                    "volume": int(_safe_float(row.get("Volume"))),
                }
            )
        return records
    except Exception as exc:
        logger.warning("Price history fetch failed for %s: %s", yf_symbol, exc)
        return []


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_yahoo_finance_data(symbol: str = "", input_str: str = "") -> dict[str, Any]:
    """
    Fetch financial metrics and company overview for an Indian stock
    from Yahoo Finance (free, no API key required).

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"
    Examples:
      "RELIANCE"        → fetches RELIANCE.NS from NSE
      "RELIANCE:BSE"    → fetches RELIANCE.BO from BSE
      "NIFTYBEES:NSE"   → fetches NIFTYBEES.NS (ETF)

    Returns a dict with sector, industry, P/E, P/B, 52-week range,
    market cap, dividend yield, current price, YoY price change %,
    YoY market cap change %, and company description.
    """
    # Parse input
    raw_input = (symbol or input_str).strip()
    parts = raw_input.split(":")
    sym = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    # Small delay to be polite to Yahoo Finance
    time.sleep(0.5)

    data = fetch_yahoo_data(sym, exchange)

    # Fetch 1y history to compute YoY change
    hist = fetch_price_history(sym, exchange, period="1y")
    yoy_pct = None
    if len(hist) >= 2:
        latest = hist[-1]["close"]
        prev1y = hist[0]["close"]
        yoy_pct = round(((latest - prev1y) / prev1y * 100), 2) if prev1y else 0.0

    # Format Market Cap and determine Cap Category
    is_indian = (
        data.symbol.endswith((".NS", ".BO"))
        or exchange.upper() in ("NSE", "BSE")
    )
    amfi_rank = None
    amfi_avg_mcap = None
    if is_indian:
        mc_crore = data.market_cap / 1e7
        market_cap_formatted = f"₹{mc_crore:,.2f} Cr"
        category = _classify_indian(mc_crore)
        try:
            from src.db.pool import get_pool
            from src.db.repository import MarketDataRepository
            repo = MarketDataRepository(get_pool())
            amfi_data = repo.amfi_market_cap_lookup(sym)
            if amfi_data:
                amfi_rank = amfi_data.get("rank")
                category = amfi_data.get("cap_category", category)
                amfi_avg_mcap = amfi_data.get("avg_mcap_cr")
        except Exception as exc:
            logger.debug("ClickHouse AMFI lookup skipped for %s: %s", sym, exc)
    else:
        mc_billion = data.market_cap / 1e9
        market_cap_formatted = f"${mc_billion:,.2f} B"
        category = _classify_us(data.market_cap)

    res = {
        "symbol": data.symbol,
        "sector": data.sector,
        "industry": data.industry,
        "cap_category": category,
        "market_cap": data.market_cap,
        "market_cap_formatted": market_cap_formatted,
        "pe_ratio": data.pe_ratio,
        "pb_ratio": data.pb_ratio,
        "dividend_yield_pct": data.dividend_yield,
        "52_week_high": data.fifty_two_week_high,
        "52_week_low": data.fifty_two_week_low,
        "current_price_inr": data.current_price,
        "price_yoy_change_pct": yoy_pct,
        "market_cap_yoy_change_pct": yoy_pct, # assuming constant outstanding shares
        "description": data.description[:500] if data.description else "",
    }
    if amfi_rank is not None:
        res["amfi_rank"] = amfi_rank
    if amfi_avg_mcap is not None:
        res["amfi_6m_avg_mcap_cr"] = amfi_avg_mcap
    return res


@tool
def get_price_momentum(symbol: str = "", input_str: str = "") -> dict[str, Any]:
    """
    Fetch 1-year price history and compute momentum signals for an
    Indian stock using Yahoo Finance (free).

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"

    Returns recent close prices, 30-day return %, 90-day return %,
    1-year return % (YoY), and a simple momentum signal (BULLISH / BEARISH / NEUTRAL).
    """
    raw_input = (symbol or input_str).strip()
    parts = raw_input.split(":")
    sym = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    time.sleep(0.5)
    history = fetch_price_history(sym, exchange, period="1y")

    if len(history) < 2:
        return {"symbol": symbol, "error": "Insufficient price history"}

    latest_close = history[-1]["close"]
    oldest_close = history[0]["close"]

    # 30-day return (approx last 22 trading days)
    idx_30d = max(0, len(history) - 22)
    close_30d_ago = history[idx_30d]["close"]

    # 90-day return (approx last 66 trading days)
    idx_90d = max(0, len(history) - 66)
    close_90d_ago = history[idx_90d]["close"]

    ret_30d = ((latest_close - close_30d_ago) / close_30d_ago * 100) if close_30d_ago else 0
    ret_90d = ((latest_close - close_90d_ago) / close_90d_ago * 100) if close_90d_ago else 0
    ret_1y = ((latest_close - oldest_close) / oldest_close * 100) if oldest_close else 0

    if ret_30d > 5:
        signal = "BULLISH"
    elif ret_30d < -5:
        signal = "BEARISH"
    else:
        signal = "NEUTRAL"

    return {
        "symbol": symbol,
        "current_price": latest_close,
        "return_30d_pct": round(ret_30d, 2),
        "return_90d_pct": round(ret_90d, 2),
        "return_1y_pct": round(ret_1y, 2),
        "momentum_signal": signal,
        "data_points": len(history),
    }


@tool
def get_yahoo_stock_news(symbol: str = "", input_str: str = "") -> dict[str, Any]:
    """
    Fetch recent news for an Indian stock directly from Yahoo Finance.

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"
    Examples:
      "RELIANCE"      → RELIANCE.NS
      "ITC:NSE"       → ITC.NS

    Complements get_stock_news (GNews) and get_newsapi_stock_news (NewsAPI.org)
    — Yahoo's own news feed isn't rate-limited the way scraped GNews is, and
    often covers lower-profile stocks the other two sources miss. Every
    fetched article is also indexed into the Qdrant RAG news corpus, so
    researching a stock here builds its historical coverage for later
    correlation/RAG queries.

    Returns articles with title, source, published_at, url, and sentiment.
    """
    raw_input = (symbol or input_str).strip()
    parts = raw_input.split(":")
    sym = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"
    yf_symbol = _build_yf_symbol(sym, exchange)

    try:
        raw_news = yf.Ticker(yf_symbol).news or []
    except Exception as exc:
        logger.warning("Yahoo news fetch failed for %s: %s", yf_symbol, exc)
        raw_news = []

    items: list[NewsItem] = []
    for n in raw_news:
        c = n.get("content") or {}
        title = c.get("title") or ""
        if not title:
            continue
        description = c.get("summary") or c.get("description") or ""
        published_at = c.get("pubDate") or c.get("displayTime") or ""
        provider = c.get("provider") or {}
        source = provider.get("displayName", "") if isinstance(provider, dict) else str(provider)
        url = (
            (c.get("canonicalUrl") or {}).get("url")
            or (c.get("clickThroughUrl") or {}).get("url")
            or ""
        )
        items.append(NewsItem(
            title=title,
            source=source,
            published_at=published_at,
            url=url,
            description=description,
            sentiment=_infer_sentiment(f"{title} {description}"),
        ))

    # Index into Qdrant RAG — same historical-corpus write-through as
    # get_stock_news / get_newsapi_stock_news.
    if items:
        from src.data_importer.tool_fetchers.news_search import _cache_articles_to_qdrant
        _cache_articles_to_qdrant(symbol, items)

    if not items:
        return {
            "symbol": symbol,
            "source": "Yahoo Finance",
            "articles": [],
            "overall_sentiment": "NEUTRAL",
            "note": "No articles found.",
        }

    sentiments = [i.sentiment for i in items]
    pos = sentiments.count(Sentiment.POSITIVE)
    neg = sentiments.count(Sentiment.NEGATIVE)
    overall = "POSITIVE" if pos > neg else "NEGATIVE" if neg > pos else "NEUTRAL"

    return {
        "symbol": symbol,
        "source": "Yahoo Finance",
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





# ── SEBI / AMFI Cap Classification Thresholds (INR Crore) ────────────────────
# Source: SEBI circular SEBI/HO/IMD/DF3/CIR/P/2017/114
# Official list: AMFI Average Market Capitalization (6 months ended 30 June 2026).
_LARGE_CAP_CR = 106_346   # Rank 1–100   (Cut-off at Rank 100 GAIL: ₹1,06,346 Cr)
_MID_CAP_CR   =  33_664   # Rank 101–250 (Cut-off at Rank 250 GODREJIND: ₹33,664 Cr)
_SMALL_CAP_CR =   2_000   # Rank 251+    (Rank 251 NAVINFLUOR: ₹33,441 Cr down to ~₹2,000 Cr)
# below ₹2,000 Cr         = Micro Cap


def _classify_indian(mc_crore: float) -> str:
    """Return SEBI/AMFI cap category for an Indian stock (INR Crore input)."""
    if mc_crore <= 0:
        return "Unknown"
    if mc_crore >= _LARGE_CAP_CR:
        return "Large Cap"
    if mc_crore >= _MID_CAP_CR:
        return "Mid Cap"
    if mc_crore >= _SMALL_CAP_CR:
        return "Small Cap"
    return "Micro Cap"


def _classify_us(mc_usd: float) -> str:
    """Return cap category for a US-listed stock (USD raw input)."""
    if mc_usd <= 0:
        return "Unknown"
    b = mc_usd / 1e9   # convert to USD billions
    if b >= 200:
        return "Mega Cap"
    if b >= 10:
        return "Large Cap"
    if b >= 2:
        return "Mid Cap"
    if b >= 0.3:
        return "Small Cap"
    return "Micro Cap"


@tool
def get_market_cap_category(symbol: str = "", input_str: str = "") -> dict[str, Any]:
    """
    Classify an Indian (NSE/BSE) or US stock into its market-cap tier and
    fetch its current market capitalisation via Yahoo Finance.

    Classification standards applied:
      • Indian stocks (NSE/BSE, ₹ Crore):
          Large Cap : Market Cap ≥ ₹20,000 Cr  (approx top ~100)
          Mid Cap   : ₹5,000 Cr ≤ Market Cap < ₹20,000 Cr  (approx 101–250)
          Small Cap : ₹500 Cr ≤ Market Cap < ₹5,000 Cr  (approx 251–500)
          Micro Cap : Market Cap < ₹500 Cr
          (aligned with SEBI circular SEBI/HO/IMD/DF3/CIR/P/2017/114)

      • US stocks (NASDAQ/NYSE, USD):
          Mega Cap  : Market Cap ≥ $200 Billion
          Large Cap : $10 Billion ≤ Market Cap < $200 Billion
          Mid Cap   : $2 Billion ≤ Market Cap < $10 Billion
          Small Cap : $300 Million ≤ Market Cap < $2 Billion
          Micro Cap : Market Cap < $300 Million

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"
    Examples:
      "RELIANCE"        → fetches RELIANCE.NS, classifies as Large Cap (₹ Cr)
      "TATASTEEL:NSE"   → fetches TATASTEEL.NS, classifies as Large Cap
      "MAPMYINDIA"      → fetches MAPMYINDIA.NS, classifies as Mid/Small Cap
      "AAPL:US"         → fetches AAPL, classifies as Mega Cap ($ B)
      "ADSK:NASDAQ"     → fetches ADSK, classifies as Large Cap ($ B)

    Returns a dict with:
        symbol               – Yahoo Finance ticker used (e.g. "RELIANCE.NS")
        market_cap_raw       – Raw market cap (INR or USD, as returned by Yahoo)
        market_cap_crore     – Market cap in ₹ Crore (Indian stocks only)
        market_cap_usd_bn    – Market cap in USD billions (US stocks only)
        market_cap_formatted – Human-readable e.g. "₹2,30,456.78 Cr"
        cap_category         – "Micro Cap" | "Small Cap" | "Mid Cap" | "Large Cap"
                               (or "Mega Cap" for large US stocks)
        classification_basis – The threshold set applied
        exchange             – "NSE" | "BSE" | "US"
        sector               – Sector from Yahoo Finance
        industry             – Industry from Yahoo Finance
    """
    raw_input = (symbol or input_str).strip()
    parts    = raw_input.split(":")
    sym      = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    time.sleep(0.3)   # be polite to Yahoo Finance

    yf_symbol = _build_yf_symbol(sym, exchange)
    try:
        info: dict[str, Any] = yf.Ticker(yf_symbol).info or {}
    except Exception as exc:
        logger.warning("get_market_cap_category: fetch failed for %s: %s", yf_symbol, exc)
        return {
            "symbol":       yf_symbol,
            "error":        str(exc),
            "cap_category": "Unknown",
            "exchange":     exchange,
        }

    mc_raw = _safe_float(info.get("marketCap"))
    if mc_raw <= 0:
        return {
            "symbol":       yf_symbol,
            "market_cap_raw": 0,
            "cap_category": "Unknown",
            "exchange":     exchange,
            "error":        "Market cap not available from Yahoo Finance",
        }

    is_indian = (
        exchange.upper() in ("NSE", "BSE")
        or yf_symbol.endswith((".NS", ".BO"))
    )

    if is_indian:
        mc_crore  = mc_raw / 1e7
        category  = _classify_indian(mc_crore)
        basis     = "SEBI / AMFI Category Thresholds"

        # Check ClickHouse AMFI official ranking
        amfi_rank = None
        amfi_avg_mcap = None
        try:
            from src.db.pool import get_pool
            from src.db.repository import MarketDataRepository
            repo = MarketDataRepository(get_pool())
            amfi_data = repo.amfi_market_cap_lookup(sym)
            if amfi_data:
                amfi_rank = amfi_data.get("rank")
                category = amfi_data.get("cap_category", category)
                amfi_avg_mcap = amfi_data.get("avg_mcap_cr")
                basis = (
                    f"Official AMFI Statutory Ranking (Rank #{amfi_rank}, "
                    f"6M Avg Market Cap: ₹{amfi_avg_mcap:,.2f} Cr as of {amfi_data.get('period_end_date')})"
                )
        except Exception as exc:
            logger.debug("ClickHouse AMFI lookup skipped for %s: %s", sym, exc)

        formatted = f"₹{mc_crore:,.2f} Cr"
        res_dict = {
            "symbol":               yf_symbol,
            "market_cap_raw":       mc_raw,
            "market_cap_crore":     round(mc_crore, 2),
            "market_cap_formatted": formatted,
            "cap_category":         category,
            "classification_basis": basis,
            "exchange":             exchange,
            "sector":               info.get("sector", ""),
            "industry":             info.get("industry", ""),
        }
        if amfi_rank:
            res_dict["amfi_rank"] = amfi_rank
        if amfi_avg_mcap:
            res_dict["amfi_6m_avg_mcap_cr"] = amfi_avg_mcap
        return res_dict
    else:
        b         = mc_raw / 1e9
        category  = _classify_us(mc_raw)
        formatted = f"${b:,.2f} B"
        basis     = "US convention: Mega≥$200B | Large $10–200B | Mid $2–10B | Small $0.3–2B | Micro<$0.3B"
        return {
            "symbol":               yf_symbol,
            "market_cap_raw":       mc_raw,
            "market_cap_usd_bn":    round(b, 2),
            "market_cap_formatted": formatted,
            "cap_category":         category,
            "classification_basis": basis,
            "exchange":             exchange,
            "sector":               info.get("sector", ""),
            "industry":             info.get("industry", ""),
        }


# Convenience list of Yahoo Finance tools
YAHOO_TOOLS = [get_yahoo_finance_data, get_price_momentum, get_market_cap_category]
YAHOO_NEWS_TOOLS = [get_yahoo_stock_news]
