from __future__ import annotations

import numpy as np
import pandas as pd

from ..base import DossierSection
from ..context import DossierContext
from ..utils import detect_amc


class WhaleOwnershipSection(DossierSection):
    """ISIN-keyed active-fund cross-ownership. NEVER match the ticker against
    security_name: mf_holdings stores company names ("Nuvama Wealth Management
    Limited"), and both exact and loose ILIKE name matching were measured to
    produce false negatives (0 rows for a stock held by 29 funds) and false
    positives (pulling bonds of affiliated entities) respectively. ISIN is the
    only safe key."""

    key = "whale"
    title = "Institutional Whale & Mutual Fund Ownership"

    def fetch(self, ctx: DossierContext) -> str:
        from src.db.pool import get_pool

        pool = get_pool()
        isin_df = pool.query_df(f"""
            SELECT isin FROM market_data.amfi_market_cap FINAL
            WHERE nse_symbol = '{ctx.symbol}'
              AND period_end_date = (SELECT max(period_end_date) FROM market_data.amfi_market_cap FINAL)
            LIMIT 1
        """)
        if isin_df.empty:
            isin_df = pool.query_df(f"""
                SELECT isin FROM market_data.security_symbol_map FINAL
                WHERE symbol = '{ctx.symbol}' AND isin != '' LIMIT 1
            """)

        resolved_isin = str(isin_df["isin"].iloc[0]) if not isin_df.empty else ""
        if not resolved_isin:
            return (
                f"⚠️ [UNRESOLVED_SECURITY: symbol={ctx.symbol}] no ISIN found in "
                "amfi_market_cap or security_symbol_map. Ownership was NOT checked — "
                "this is a lookup gap, not an absence of holders."
            )

        # Each fund at ITS OWN latest disclosed month, never a single global
        # max(as_of_month) for the ISIN — AMC filing calendars are ragged.
        mf_df = pool.query_df(f"""
            SELECT fund_name,
                   argMax(as_of_month, as_of_month) AS latest_month,
                   round(argMax(market_value_cr, as_of_month), 2) AS val_cr,
                   round(argMax(pct_of_nav, as_of_month), 2) AS pct_nav
            FROM market_data.mf_holdings FINAL
            WHERE isin = '{resolved_isin}'
              AND lower(asset_type) = 'equity'
              AND lower(fund_name) NOT LIKE '%index%'
              AND lower(fund_name) NOT LIKE '%etf%'
              AND lower(fund_name) NOT LIKE '%arbitrage%'
            GROUP BY fund_name
            ORDER BY val_cr DESC
            LIMIT 25
        """)

        if mf_df.empty:
            return (
                f"ℹ️ ISIN {resolved_isin} resolved, but zero ACTIVE-fund equity holdings on "
                "the latest disclosed month (index / ETF / arbitrage funds excluded)."
            )

        mf_df["latest_month"] = pd.to_datetime(mf_df["latest_month"])
        newest, oldest = mf_df["latest_month"].max(), mf_df["latest_month"].min()
        # Funds whose last filing lags the newest by >62d may have exited
        # rather than simply not filed yet.
        mf_df["stale"] = np.where((newest - mf_df["latest_month"]).dt.days > 62, "<- STALE/possible exit", "")
        mf_df["amc"] = mf_df["fund_name"].apply(detect_amc)
        mf_df["latest_month"] = mf_df["latest_month"].dt.date

        lines = [
            f"**{resolved_isin}** | {len(mf_df)} active funds | total ₹{mf_df['val_cr'].sum():,.1f} Cr "
            f"| disclosures {oldest.date()} .. {newest.date()}\n"
        ]
        if oldest != newest:
            lines.append(
                "⚠️ RAGGED DISCLOSURE: AMC filing months differ across these funds, so the "
                "total is NOT a single-date figure. Each row is that fund's own latest filing.\n"
            )
        n_stale = int((mf_df["stale"] != "").sum())
        if n_stale:
            lines.append(
                f"⚠️ {n_stale} fund(s) last filed >62d before the newest disclosure and may "
                "have EXITED — verify before citing as current holders.\n"
            )
        lines.append("```text")
        lines.append(mf_df[["amc", "fund_name", "latest_month", "val_cr", "pct_nav", "stale"]].to_string(index=False))
        lines.append("```")
        return "\n".join(lines)
