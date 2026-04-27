"""
src/deepdive/models.py
──────────────────────
Pydantic v2 models for the company deep-dive dataset.

All monetary values are in USD millions unless noted otherwise.
Follows the exact patterns from src/models/portfolio.py:
  - Pydantic v2, Optional[float] = None, Field(default_factory=list)
  - No custom validators — keep models as plain data containers
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SegmentRevenue(BaseModel):
    """Revenue breakdown for a single business segment."""

    name: str
    revenue_usd_m: Optional[float] = None
    yoy_growth_pct: Optional[float] = None
    # File path + anchor where this row was sourced
    source: str = ""


class AnnualFinancials(BaseModel):
    """Key P&L and cash-flow metrics for one fiscal year."""

    fiscal_year: str                              # e.g. "FY2025"
    revenue_usd_m: float
    gross_profit_usd_m: Optional[float] = None
    operating_income_usd_m: Optional[float] = None
    net_income_usd_m: Optional[float] = None
    free_cash_flow_usd_m: Optional[float] = None
    rd_expense_usd_m: Optional[float] = None
    capex_usd_m: Optional[float] = None
    gross_margin_pct: Optional[float] = None
    operating_margin_pct: Optional[float] = None


class ValuationSnapshot(BaseModel):
    """Market valuation metrics as of a single date, plus peer medians."""

    as_of_date: str                               # ISO date string
    pe_trailing: Optional[float] = None
    pe_forward: Optional[float] = None
    ev_revenue: Optional[float] = None
    ev_ebitda: Optional[float] = None
    fcf_yield_pct: Optional[float] = None
    market_cap_usd_b: Optional[float] = None      # billions
    peer_pe_median: Optional[float] = None
    peer_ev_ebitda_median: Optional[float] = None
    peer_ev_revenue_median: Optional[float] = None


class HeadcountData(BaseModel):
    """Headcount snapshot for one reporting period."""

    period: str                                   # e.g. "FY2025"
    total_headcount: Optional[int] = None
    yoy_change_pct: Optional[float] = None
    notes: str = ""


class FilingRefs(BaseModel):
    """URLs / accession numbers for the fetched SEC filings."""

    annual_10k_url: str = ""
    annual_10k_filed: str = ""                    # ISO date
    quarterly_10q_urls: list[str] = Field(default_factory=list)
    proxy_def14a_url: str = ""
    accession_no_10k: str = ""


class FilingExcerpts(BaseModel):
    """Raw text / HTML excerpts extracted from SEC filings."""

    segment_table_text: str = ""       # MD&A section 7 HTML — contains segment table
    competition_section_text: str = "" # Business section 1 text — competition paragraph
    rd_section_text: str = ""          # R&D discussion from MD&A
    headcount_notes_text: str = ""     # Headcount disclosure from section 1 text


class CompanyDataset(BaseModel):
    """
    Top-level dataset assembled from all sources.
    Serialised to output/deepdive/<TICKER>/<DATE>/dataset.json.
    """

    ticker: str
    company_name: str
    report_date: str                              # ISO date
    fiscal_year_end: str                          # e.g. "FY2025"
    cik: str = ""
    sic: str = ""
    exchange: str = ""

    filing_refs: FilingRefs = Field(default_factory=FilingRefs)
    filing_excerpts: FilingExcerpts = Field(default_factory=FilingExcerpts)

    segments: list[SegmentRevenue] = Field(default_factory=list)
    financials: list[AnnualFinancials] = Field(default_factory=list)   # sorted asc by fiscal_year
    valuation: Optional[ValuationSnapshot] = None
    headcount: list[HeadcountData] = Field(default_factory=list)

    rd_spend_usd_m: Optional[float] = None
    rd_pct_of_revenue: Optional[float] = None
    capex_usd_m: Optional[float] = None
    guidance_revenue_usd_m: Optional[float] = None
    guidance_eps: Optional[float] = None

    competitors_named: list[str] = Field(default_factory=list)
    jobs: dict[str, dict[str, int]] = Field(default_factory=dict)

    # One entry per raw source file used to build this dataset
    sources: list[dict] = Field(default_factory=list)
