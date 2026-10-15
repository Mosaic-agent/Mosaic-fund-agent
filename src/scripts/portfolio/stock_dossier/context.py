"""
src/scripts/portfolio/stock_dossier/context.py
─────────────────────────────────────────────────
Builds the DossierContext once per run: symbol resolution, freshness check,
canonical price history (ClickHouse-first, NSE/Shoonya), and a single shared
yfinance snapshot. Every DossierSection reads from this instead of re-fetching
the same data — e.g. the DCF section used to open its own yfinance.Ticker and
re-query ClickHouse for the latest close; it now reuses ctx.yf_info/yf_ticker()
and ctx.cur_price.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd

from src.db.pool import get_pool
from src.tools.company_resolver import resolve_company_info
from src.tools.agent_tools import check_and_refresh_symbol_data


@dataclass
class DossierContext:
    symbol: str
    company_name: str
    exchange: str
    yf_symbol: str
    days: int
    live: bool

    price_df: pd.DataFrame
    price_source: str
    summary_hdr: str
    cur_price: float
    low_price: float
    high_price: float

    yf_info: dict[str, Any] = field(default_factory=dict)
    mcap_cr: float = 0.0

    _yf_ticker: Any = None

    def yf_ticker(self):
        """Lazily-cached yfinance Ticker, shared across sections that need
        statements beyond `.info` (e.g. DCF's cashflow fallback)."""
        if self._yf_ticker is None:
            import yfinance as yf
            self._yf_ticker = yf.Ticker(self.yf_symbol)
        return self._yf_ticker


def _fetch_price_history(symbol: str, days: int) -> tuple[pd.DataFrame, str]:
    """ClickHouse-first price history with UNVERIFIED/staleness banners.
    data_importer/CLAUDE.md: 'Indian equity/ETF prices must use Shoonya or
    NSE. Never use Yahoo Finance ... for secondary Indian ETF prices.'"""
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT trade_date, open, high, low, close, volume
        FROM market_data.daily_prices FINAL
        WHERE symbol IN ('{symbol}', '{symbol}.NS')
          AND trade_date >= today() - {days}
        ORDER BY trade_date ASC
    """)

    price_source = "market_data.daily_prices FINAL (NSE/Shoonya)"

    if df.empty:
        price_source = "yfinance / Yahoo — UNVERIFIED"
        print(
            "\n" + "!" * 85
            + f"\n⚠️  PRICE AUTHORITY WARNING — '{symbol}' has NO rows in"
              " market_data.daily_prices.\n"
              "    Falling back to yfinance (Yahoo). This BREAKS the NSE/Shoonya\n"
              "    price-authority rule. Yahoo OHLC for Indian small caps is routinely\n"
              "    stale, split-unadjusted or simply wrong.\n"
              "    Treat EVERY number below as UNVERIFIED. Import the symbol first:\n"
              f"       ./mosaic.sh import --category stocks --source nse\n"
            + "!" * 85 + "\n"
        )
        import yfinance as yf
        start = (date.today() - timedelta(days=days)).isoformat()
        t = yf.Ticker(f"{symbol}.NS")
        df = t.history(start=start).reset_index()
        df = df.rename(columns={
            "Date": "trade_date", "Open": "open", "High": "high",
            "Low": "low", "Close": "close", "Volume": "volume",
        })

    if df.empty:
        return df, price_source

    latest_bar = pd.to_datetime(df["trade_date"]).max().date()
    lag_days = (date.today() - latest_bar).days
    if lag_days > 5:
        print(
            f"\n⚠️  STALE PRICE WARNING: latest bar for '{symbol}' is {latest_bar}"
            f" ({lag_days} calendar days old).\n"
            "    Every price, return and drawdown below is as-of that date, NOT today.\n"
            "    Refresh before acting:  ./mosaic.sh import --category stocks --source nse\n"
        )
        price_source += f" [STALE: as-of {latest_bar}, {lag_days}d lag]"

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values("trade_date").reset_index(drop=True), price_source


def build_context(query: str, days: int = 120, live: bool = False) -> DossierContext:
    print(f"\n🚀 Resolving company info for: {query}...")
    info = resolve_company_info(query, auto_import=False)
    symbol = info.get("symbol") or query.upper().strip()
    yf_symbol = info.get("yf_symbol") or f"{symbol}.NS"
    company_name = info.get("company_name") or symbol
    exchange = info.get("exchange") or "NSE"
    print(f"Resolved: {symbol} ({company_name}) on {exchange}")

    print("Checking database baseline freshness...")
    check_and_refresh_symbol_data.invoke({"symbol": symbol, "auto_import": True})

    price_df, price_source = _fetch_price_history(symbol, days)
    if price_df.empty:
        raise RuntimeError(f"No price data available for '{symbol}' from any source.")

    cur_price = float(price_df["close"].iloc[-1])
    prev_close = float(price_df["close"].iloc[-2]) if len(price_df) > 1 else cur_price
    chg = cur_price - prev_close
    pct_chg = (chg / prev_close) * 100 if prev_close else 0.0
    low_price, high_price = float(price_df["low"].min()), float(price_df["high"].max())
    summary_hdr = (
        f"Latest Close: ₹{cur_price:,.2f} ({chg:+.2f} / {pct_chg:+.2f}%) | "
        f"{days}d Range: ₹{low_price:,.2f} – ₹{high_price:,.2f}"
    )
    print(summary_hdr)

    import yfinance as yf
    yf_info = yf.Ticker(yf_symbol).info or {}
    mcap_cr = (yf_info.get("marketCap") or 0) / 1e7

    return DossierContext(
        symbol=symbol, company_name=company_name, exchange=exchange, yf_symbol=yf_symbol,
        days=days, live=live,
        price_df=price_df, price_source=price_source, summary_hdr=summary_hdr,
        cur_price=cur_price, low_price=low_price, high_price=high_price,
        yf_info=yf_info, mcap_cr=mcap_cr,
    )
