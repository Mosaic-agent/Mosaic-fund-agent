"""
VLRT v3 — point-in-time data loading, with integrity gates.

Five defects in the underlying data were found during the build and are corrected here.
They are documented because each one silently invalidates a backtest if missed:

1. ``LIQUIDBEES.close`` is unusable as a cash return series. It is a constant-NAV
   daily-dividend fund: 898 of 907 days in 2023+ sit at exactly 1000.00, with nine
   stray values producing ±10% phantom moves. ``mf_nav`` carries the same flat 1000.
   Cash is therefore taken from a liquid-fund **growth** NAV (a real total-return
   series), never from LIQUIDBEES.
2. ``NIFTYBEES.close`` contains an un-reversed 10:1 split (effective ~2017-08).
   ``repair_price_glitches`` unwinds it on load; independently cross-checked against
   NSE's own unadjusted series (nselib) — NSE's last pre-split close of 997.78 on
   2017-08-11 matches the repaired value (99.778) exactly. Once repaired, the full
   series is clean back to GOLDBEES's own start (2011-05-03), which is the real
   binding constraint, not NIFTYBEES.
3. ``asset_type='gold'`` in ``mf_holdings`` is *precious metals* — gold **and silver**.
   The sleeve is named ``pm`` for that reason.
4. ``asset_type='bond'`` is ~70% TREPS overnight repo, so the sleeve is named ``cash``
   and no duration story should be told about it.
5. ``mf_holdings`` carries first-of-month rows from a second scraper with an
   incompatible schema (totals of exactly 100.00). Only true month-end rows are kept.

Deliberately NOT used (verified empty or corrupt over the window): ``fii_dii_flows``
(starts 2025-10-01), ``stock_valuation`` (40 snapshot dates, all 2026-04+),
``amfi_category_flows`` (category_name corrupted to roman numerals), ``nse_delivery``
(2026-07+), ``etf_aum`` (2026-04+).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from src.db.pool import get_pool

PRICE_SYMBOLS: tuple[str, ...] = (
    "NIFTY50", "NIFTYMID", "INDIAVIX", "DXY", "US10Y", "SP500",
    "NIFTYBEES", "GOLDBEES", "JUNIORBEES", "SILVERBEES",
)

#: Instrument backing each portfolio sleeve. ``cash`` is synthesised (see module docs).
SLEEVES: dict[str, str] = {"equity": "NIFTYBEES", "pm": "GOLDBEES", "cash": "CASH"}

#: Nippon India Liquid Fund - Regular Plan - Growth Option (AMFI scheme code).
#: Verified 2016+: 4.81% p.a., 0.186% annualised vol, max daily move 0.33%.
CASH_SCHEME_CODE = "100851"
_NAV_CACHE_DIR = Path("output/.cache")

#: The calendar is taken from this symbol alone — never from a union across symbols
#: (SP500/DXY/US10Y trade on NSE holidays) and never from ``pd.date_range``.
CALENDAR_SYMBOL = "NIFTYBEES"

#: Any sleeve daily move beyond this is a data-integrity error, not a market event.
MAX_PLAUSIBLE_DAILY_MOVE = 0.15

COT_MAX_STALE_DAYS = 21
DEFAULT_START = "2011-01-01"
#: First date on which every sleeve series is glitch-free. Bounded by GOLDBEES's own
#: daily_prices start (2011-05-03), not by NIFTYBEES — verified via check_sleeve_integrity
#: over the full repaired history, and cross-checked against NSE's unadjusted series.
CLEAN_START = "2011-05-03"

FUND_MULTI_ASSET = "QUANT_MULTI_ASSET"
FUND_DYNAMIC_AA = "QUANT_DYNAMIC_ASSET_ALLOCATION"
SCHEME_CODES = {FUND_MULTI_ASSET: "120821", FUND_DYNAMIC_AA: "120833"}

_HOUSE_EXCLUDE = (
    FUND_MULTI_ASSET, FUND_DYNAMIC_AA,
    "QUANT_LIQUID", "QUANT_OVERNIGHT", "QUANT_GILT", "QUANT_ARBITRAGE",
)


class DataIntegrityError(RuntimeError):
    """Raised when a sleeve series fails a plausibility gate."""


@dataclass
class VLRTData:
    daily: pd.DataFrame           # signal inputs, on the NSE calendar
    monthly: pd.DataFrame         # month-end trade dates only
    sleeve_px: pd.DataFrame       # total-return index per sleeve
    notes: list[str] = field(default_factory=list)

    @property
    def month_end_dates(self) -> pd.DatetimeIndex:
        return self.monthly.index


def _q(sql: str) -> pd.DataFrame:
    return get_pool().query_df(sql)


def month_end_trade_dates(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Last *actual trading day* of each month in ``idx``."""
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby(idx.to_period("M")).max().values)


def repair_price_glitches(s: pd.Series, tol: float = 0.05) -> pd.Series:
    """
    Reverse un-adjusted decimal/split jumps.

    A jump whose ratio to the prior close is within ``tol`` of a power of ten is
    treated as a scaling error and unwound for all subsequent observations.
    """
    v = s.to_numpy(dtype=float)
    adj = np.ones(len(v))
    factor = 1.0
    for i in range(1, len(v)):
        if np.isfinite(v[i]) and np.isfinite(v[i - 1]) and v[i - 1] > 0:
            ratio = v[i] / v[i - 1]
            for k in (-2, -1, 1, 2):
                if abs(ratio / (10.0**k) - 1.0) < tol:
                    factor /= 10.0**k
                    break
        adj[i] = factor
    return pd.Series(v * adj, index=s.index, name=s.name)


def load_prices(start: str = DEFAULT_START) -> pd.DataFrame:
    syms = ", ".join(f"'{s}'" for s in PRICE_SYMBOLS)
    df = _q(
        f"""SELECT symbol, trade_date, close
            FROM market_data.daily_prices FINAL
            WHERE symbol IN ({syms}) AND trade_date >= '{start}'
            ORDER BY trade_date"""
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    wide = df.pivot_table(index="trade_date", columns="symbol", values="close", aggfunc="last")
    return wide.sort_index()


def load_fx(start: str = DEFAULT_START) -> pd.Series:
    df = _q(
        f"""SELECT trade_date, close FROM market_data.fx_rates FINAL
            WHERE symbol = 'USDINR' AND trade_date >= '{start}' ORDER BY trade_date"""
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date")["close"].rename("USDINR").sort_index()


def load_cot(start: str = DEFAULT_START) -> pd.DataFrame:
    df = _q(
        f"""SELECT report_date, mm_net, open_interest
            FROM market_data.cot_gold FINAL
            WHERE report_date >= '{start}' ORDER BY report_date"""
    )
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["cot_pct_oi"] = df["mm_net"] / df["open_interest"].clip(lower=1)
    return df.set_index("report_date")[["cot_pct_oi"]].sort_index()


def _load_nav_series(scheme_code: str, cache_name: str, months: int = 250) -> pd.Series:
    """
    Shared cached loader for any AMFI scheme NAV, fetched live from mfapi.in (no
    ClickHouse table carries fund NAV) since it backs both the cash proxy and the
    fund-NAV benchmark.
    """
    cache_path = _NAV_CACHE_DIR / cache_name
    if cache_path.exists():
        c = pd.read_csv(cache_path, parse_dates=["date"])
        return pd.Series(c["nav"].to_numpy(float), index=pd.DatetimeIndex(c["date"])).sort_index()

    from src.scripts.portfolio.fund_mom_returns import fetch_nav_history

    _, df = fetch_nav_history(scheme_code, months=months)
    s = pd.Series(df["nav"].astype(float).to_numpy(), index=pd.to_datetime(df["date"])).sort_index()
    s = repair_price_glitches(s)  # unwinds unit-split / decimal-scale artifacts
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    s.rename("nav").rename_axis("date").reset_index().to_csv(cache_path, index=False)
    return s


def load_cash_nav() -> pd.Series:
    """
    Liquid-fund growth NAV as the cash total-return series.

    ``LIQUIDBEES.close`` is unusable — see module docstring, defect 1.
    """
    return _load_nav_series(CASH_SCHEME_CODE, "vlrt_cash_nav.csv")


def load_fund_nav_returns(scheme_code: str = SCHEME_CODES[FUND_MULTI_ASSET]) -> pd.Series:
    """
    Daily simple returns of a disclosed fund's own NAV — a real, tradeable benchmark,
    but not one the model or its cost/turnover pipeline ever touches. It is net of the
    fund's TER, may hold silver the model does not, and (per ``mf_holdings``) the fund
    runs a short single-stock-futures overlay in some months — not directly comparable,
    only a reference point.
    """
    s = _load_nav_series(scheme_code, f"vlrt_fund_nav_{scheme_code}.csv")
    return s.pct_change().dropna()


def load_fund_alloc(fund_name: str) -> pd.DataFrame:
    """
    Disclosed allocation by asset_type, month-end rows only.

    Weights are returned raw plus a ``classified`` total, so callers renormalise
    explicitly. ``other`` is included in the total because it is not a residual —
    it holds equities, CDs, g-secs and short single-stock futures.
    """
    df = _q(
        f"""SELECT as_of_month, asset_type, sum(pct_of_nav) AS pct
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}'
            GROUP BY as_of_month, asset_type
            ORDER BY as_of_month"""
    )
    if df.empty:
        return df
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    # Drop the second scraper's first-of-month rows (defect 5).
    df = df[df["as_of_month"] == df["as_of_month"] + pd.offsets.MonthEnd(0)]
    wide = df.pivot_table(index="as_of_month", columns="asset_type", values="pct", aggfunc="sum")
    for col in ("equity", "gold", "bond", "cash", "other"):
        if col not in wide.columns:
            wide[col] = 0.0
    wide = wide.fillna(0.0).sort_index()
    wide.index = wide.index.to_period("M")
    wide["classified"] = wide[["equity", "gold", "bond", "cash", "other"]].sum(axis=1)
    return wide


def load_pm_split(fund_name: str) -> pd.DataFrame:
    """Gold vs silver inside the precious-metals bucket (defect 3)."""
    df = _q(
        f"""SELECT as_of_month,
              sumIf(pct_of_nav, positionCaseInsensitive(security_name, 'silver') > 0) AS silver,
              sumIf(pct_of_nav, positionCaseInsensitive(security_name, 'silver') = 0) AS gold_only
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}' AND asset_type = 'gold'
            GROUP BY as_of_month ORDER BY as_of_month"""
    )
    if df.empty:
        return df
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df = df[df["as_of_month"] == df["as_of_month"] + pd.offsets.MonthEnd(0)]
    df.index = df.pop("as_of_month").dt.to_period("M")
    return df


def load_house_equity() -> pd.DataFrame:
    """Mean equity% across Quant's other equity funds — the replication ceiling."""
    excl = ", ".join(f"'{f}'" for f in _HOUSE_EXCLUDE)
    df = _q(
        f"""SELECT as_of_month, avg(eq) AS house_equity_pct, count() AS n_funds FROM (
                SELECT fund_name, as_of_month, sumIf(pct_of_nav, asset_type = 'equity') AS eq
                FROM market_data.mf_holdings FINAL
                WHERE fund_name LIKE 'QUANT%' AND fund_name NOT IN ({excl})
                GROUP BY fund_name, as_of_month)
            GROUP BY as_of_month ORDER BY as_of_month"""
    )
    if df.empty:
        return df
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df = df[df["as_of_month"] == df["as_of_month"] + pd.offsets.MonthEnd(0)]
    df.index = df.pop("as_of_month").dt.to_period("M")
    return df[["house_equity_pct", "n_funds"]]


def check_sleeve_integrity(sleeve_px: pd.DataFrame) -> None:
    """Hard gate: implausible daily moves mean broken data, not a market event."""
    bad = {}
    for col in sleeve_px.columns:
        r = sleeve_px[col].pct_change()
        hits = r[r.abs() > MAX_PLAUSIBLE_DAILY_MOVE]
        if len(hits):
            bad[col] = [(str(d.date()), round(float(x) * 100, 1)) for d, x in hits.head(5).items()]
    if bad:
        raise DataIntegrityError(f"Implausible sleeve returns (>|{MAX_PLAUSIBLE_DAILY_MOVE:.0%}| daily): {bad}")


def load_all(start: str = DEFAULT_START, clean_start: str = CLEAN_START) -> VLRTData:
    notes: list[str] = []

    px = load_prices(start)
    for col in px.columns:
        px[col] = repair_price_glitches(px[col])

    trading_days = px[CALENDAR_SYMBOL].dropna().index
    daily = px.reindex(trading_days).ffill(limit=5)
    daily["USDINR"] = load_fx(start).reindex(trading_days).ffill(limit=5)

    cot = load_cot(start)
    cot_daily = cot.reindex(trading_days.union(cot.index)).ffill(limit=COT_MAX_STALE_DAYS)
    daily["cot_pct_oi"] = cot_daily.reindex(trading_days)["cot_pct_oi"]

    stale_days = (trading_days.max() - cot.index.max()).days
    if stale_days > COT_MAX_STALE_DAYS:
        notes.append(
            f"COT gold stale: latest report {cot.index.max().date()} is {stale_days}d behind "
            f"the last trade date; suppressed beyond the {COT_MAX_STALE_DAYS}d cap."
        )

    cash = load_cash_nav().reindex(trading_days).ffill(limit=5)
    sleeve_px = pd.DataFrame(
        {"equity": daily[SLEEVES["equity"]], "pm": daily[SLEEVES["pm"]], "cash": cash},
        index=trading_days,
    )

    # Only the *tradable* series is restricted to the glitch-free window. Signal
    # inputs keep their full history: NIFTY50 is clean back to 2011, and truncating
    # it would starve the 60-month valuation lookback for no reason.
    sleeve_px = sleeve_px.loc[clean_start:]
    check_sleeve_integrity(sleeve_px)
    notes.append(
        f"Signals from {daily.index.min().date()}; tradable sleeves from {clean_start} "
        "(NIFTYBEES split glitches before that)."
    )

    monthly = daily.loc[month_end_trade_dates(daily.index)]
    return VLRTData(daily=daily, monthly=monthly, sleeve_px=sleeve_px, notes=notes)
