"""
src/models/portfolio.py
───────────────────────
Pydantic data models for Zerodha portfolio holdings, positions,
and enriched analysis results.

All monetary values are in INR (₹).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enumerations ──────────────────────────────────────────────────────────────

class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    MCX = "MCX"
    NFO = "NFO"
    CDS = "CDS"


class InstrumentType(str, Enum):
    STOCK = "STOCK"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    UNKNOWN = "UNKNOWN"


class Sentiment(str, Enum):
    POSITIVE = "POSITIVE"
    NEUTRAL = "NEUTRAL"
    NEGATIVE = "NEGATIVE"


# ── Zerodha MCP Raw Models ─────────────────────────────────────────────────────

class Holding(BaseModel):
    """Represents a single holding returned by Zerodha get_holdings()."""

    tradingsymbol: str = Field(..., description="NSE/BSE trading symbol e.g. RELIANCE")
    exchange: str = Field(default="NSE", description="Exchange: NSE or BSE")
    isin: str = Field(default="", description="ISIN code e.g. INE002A01018")
    quantity: int = Field(..., description="Total quantity held")
    t1_quantity: int = Field(default=0, description="T1 settlement quantity")
    average_price: float = Field(..., description="Average buy price in INR")
    last_price: float = Field(default=0.0, description="Last traded price in INR")
    close_price: float = Field(default=0.0, description="Previous close price in INR")
    pnl: float = Field(default=0.0, description="Unrealised P&L in INR")
    day_change: float = Field(default=0.0, description="Day change in INR")
    day_change_percentage: float = Field(default=0.0, description="Day change %")
    instrument_type: InstrumentType = Field(
        default=InstrumentType.STOCK,
        description="Type of instrument",
    )

    @property
    def current_value(self) -> float:
        """Current market value of this holding in INR."""
        return self.quantity * self.last_price

    @property
    def invested_value(self) -> float:
        """Total invested amount in INR."""
        return self.quantity * self.average_price

    @property
    def pnl_percent(self) -> float:
        """Unrealised P&L as percentage."""
        if self.invested_value == 0:
            return 0.0
        return ((self.current_value - self.invested_value) / self.invested_value) * 100

    @property
    def yahoo_symbol(self) -> str:
        """Returns Yahoo Finance ticker symbol e.g. RELIANCE.NS or RELIANCE.BO."""
        suffix = ".BO" if self.exchange.upper() == "BSE" else ".NS"
        return f"{self.tradingsymbol}{suffix}"


class Position(BaseModel):
    """Represents an open position (intraday or short-term) from get_positions()."""

    tradingsymbol: str
    exchange: str = "NSE"
    product: str = Field(default="", description="CNC, MIS, NRML, CO, BO")
    quantity: int = Field(default=0)
    average_price: float = Field(default=0.0)
    last_price: float = Field(default=0.0)
    pnl: float = Field(default=0.0)
    day_change_percentage: float = Field(default=0.0)


class Portfolio(BaseModel):
    """Aggregated portfolio fetched from Zerodha Kite MCP."""

    holdings: list[Holding] = Field(default_factory=list)
    positions: list[Position] = Field(default_factory=list)
    profile_name: str = Field(default="", description="Kite user name")

    @property
    def total_invested(self) -> float:
        return sum(h.invested_value for h in self.holdings)

    @property
    def total_current_value(self) -> float:
        return sum(h.current_value for h in self.holdings)

    @property
    def total_pnl(self) -> float:
        return sum(h.pnl for h in self.holdings)

    @property
    def total_pnl_percent(self) -> float:
        if self.total_invested == 0:
            return 0.0
        return (self.total_pnl / self.total_invested) * 100


# ── Enrichment / Analysis Models ──────────────────────────────────────────────

class NewsItem(BaseModel):
    """A single news article fetched for a stock."""

    title: str
    source: str = ""
    published_at: str = ""
    url: str = ""
    description: str = ""
    sentiment: Sentiment = Sentiment.NEUTRAL


class QuarterlyResult(BaseModel):
    """Latest quarterly financial results for a company."""

    period: str = Field(default="", description="e.g. Q3 FY2025")
    revenue_cr: float = Field(default=0.0, description="Revenue in Crore INR")
    net_profit_cr: float = Field(default=0.0, description="Net Profit in Crore INR")
    eps: float = Field(default=0.0, description="Earnings Per Share in INR")
    revenue_yoy_pct: float = Field(default=0.0, description="Revenue growth YoY %")
    profit_yoy_pct: float = Field(default=0.0, description="Profit growth YoY %")
    guidance: str = Field(default="", description="Management guidance / commentary")
    source_url: str = Field(default="", description="Source URL for the results")


class YahooFinanceData(BaseModel):
    """Financial metrics fetched from Yahoo Finance for a symbol."""

    symbol: str
    sector: str = ""
    industry: str = ""
    market_cap: float = Field(default=0.0, description="Market cap in INR")
    pe_ratio: float = Field(default=0.0, description="Trailing P/E ratio")
    pb_ratio: float = Field(default=0.0, description="Price-to-Book ratio")
    dividend_yield: float = Field(default=0.0, description="Dividend yield %")
    fifty_two_week_high: float = Field(default=0.0)
    fifty_two_week_low: float = Field(default=0.0)
    current_price: float = Field(default=0.0, description="Latest close price in INR")
    description: str = Field(default="", description="Company business summary")


class AssetAnalysis(BaseModel):
    """Full analysis result for a single holding."""

    symbol: str
    exchange: str = "NSE"
    instrument_type: InstrumentType = InstrumentType.STOCK
    quantity: int = 0
    average_buy_price: float = 0.0
    current_price: float = 0.0
    invested_value: float = 0.0
    current_value: float = 0.0
    pnl_percent: float = 0.0

    # Enrichment
    yahoo_data: YahooFinanceData | None = None
    news_items: list[NewsItem] = Field(default_factory=list)
    quarterly_result: QuarterlyResult | None = None
    inav_data: dict | None = Field(
        default=None,
        description="ETF iNAV data — iNAV, market price, premium/discount. None for stocks.",
    )
    historic_inav_data: dict | None = Field(
        default=None,
        description="30-day historic iNAV from AMFI + Yahoo Finance. Includes sparkline, trend, avg P/D. None for stocks.",
    )

    # AI-generated scores
    sentiment_score: float = Field(
        default=0.0,
        description="Sentiment score from -1 (very negative) to +1 (very positive)",
    )
    risk_score: float = Field(
        default=5.0,
        description="Risk score from 1 (very low risk) to 10 (very high risk)",
    )
    summary: str = Field(default="", description="5-bullet AI investment insight summary")
    key_insights: list[str] = Field(default_factory=list, description="Bullet insight points")
    risk_signals: list[str] = Field(default_factory=list, description="Identified risk signals")


# ── Final Report Models ────────────────────────────────────────────────────────

class PortfolioSummary(BaseModel):
    total_value: str = ""
    total_invested: str = ""
    total_pnl: str = ""
    total_pnl_percent: str = ""
    health_score: float = Field(default=0.0, description="Portfolio health 0–100")
    diversification_score: float = Field(default=0.0, description="Diversification 0–100")
    num_holdings: int = 0
    etf_count: int = 0
    stock_count: int = 0
    etf_allocation_pct: float = 0.0
    direct_equity_allocation_pct: float = 0.0


class PortfolioReport(BaseModel):
    """Final structured report – matches the required output JSON schema."""

    generated_at: str = ""
    portfolio_summary: PortfolioSummary = Field(default_factory=PortfolioSummary)
    holdings_analysis: list[dict[str, Any]] = Field(default_factory=list)
    sector_allocation: dict[str, float] = Field(
        default_factory=dict,
        description="Sector → % of portfolio",
    )
    portfolio_risks: list[str] = Field(default_factory=list)
    actionable_insights: list[str] = Field(default_factory=list)
