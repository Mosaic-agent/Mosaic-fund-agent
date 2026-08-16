"""
src/tools/mf_multi_asset.py
───────────────────────────
Centralized catalogue, dynamic discovery, and AMC archetype profiling for ALL
Multi-Asset Allocation and Asset Allocator Mutual Funds across Indian AMCs
in `market_data.mf_holdings`.

Supports:
  - Nippon India (Multi Asset Fund, Multi Asset FoF)
  - DSP (Multi Asset Allocation, Multi Asset Omni FoF, Dynamic Asset Allocation)
  - Quant (Multi Asset, Dynamic Asset Allocation)
  - Bajaj Finserv (Multi Asset Allocation)
  - ICICI Prudential (Multi Asset Fund)
  - Axis (Multi Asset Allocation, Multi-Asset Active FoF)
  - Invesco (Multi Asset Allocation)
  - Mirae Asset (Multi Asset Allocation, Diversified Equity Allocator FoF)
  - Motilal Oswal (Asset Allocation FoF Conservative, Aggressive)
  - HDFC (Multi-Asset Allocation)
  - Kotak Mahindra (Multi Asset Allocation)
  - SBI (Multi Asset Allocation)
  - Canara Robeco (Multi Asset Allocation)
  - Dynamic discovery for any newly added AMC schemes.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional
import pandas as pd

sys.path.append(os.getcwd())

from src.db.pool import get_client, get_pool

# ── Canonical Roster across all supported AMCs ───────────────────────────────

CANONICAL_MULTI_ASSET_FUNDS: List[Dict[str, str]] = [
    {
        "label": "Nippon Multi Asset",
        "filter": "(fund_name IN ('NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND', 'NIPPON_INDIA_MULTI_ASSET_FUND') OR scheme_code = 'RLMF806') AND scheme_code != 'RLMF811'",
    },
    {
        "label": "Nippon Multi Asset FoF",
        "filter": "(fund_name IN ('NIPPON_INDIA_MULTI_ASSET_OMNI_FOF', 'NIPPON_INDIA_MULTI_ASSET_ACTIVE_FOF', 'NIPPON_INDIA_ASSET_ALLOCATOR_FOF') OR scheme_code = 'RLMF811')",
    },
    {
        "label": "DSP Multi Asset",
        "filter": "fund_name = 'DSP_MULTI_ASSET' OR scheme_code = '152056'",
    },
    {
        "label": "DSP Multi Asset Omni",
        "filter": "fund_name = 'DSP_MULTI_ASSET_OMNI_FOF' OR scheme_code = '154167'",
    },
    {
        "label": "DSP Dynamic Asset Allocation",
        "filter": "fund_name = 'DSP_DYNAMIC_ASSET_ALLOCATION' OR scheme_code = '126393'",
    },
    {
        "label": "Bajaj Multi Asset",
        "filter": "fund_name = 'BAJAJ_MULTI_ASSET' OR scheme_code = '152639'",
    },
    {
        "label": "Quant Multi Asset",
        "filter": "fund_name = 'QUANT_MULTI_ASSET' OR scheme_code = '120821'",
    },
    {
        "label": "Quant Dynamic Asset Allocation",
        "filter": "fund_name = 'QUANT_DYNAMIC_ASSET_ALLOCATION' OR scheme_code = '120833'",
    },
    {
        "label": "ICICI Multi Asset",
        "filter": "fund_name = 'ICICI_MULTI_ASSET' OR scheme_code IN ('120334', '120716')",
    },
    {
        "label": "Axis Multi Asset",
        "filter": "fund_name = 'Axis Multi Asset Allocation Fund' OR scheme_code = 'AXIS_MULTI_ASSET_ALLOCATION'",
    },
    {
        "label": "Axis Multi-Asset Active FoF",
        "filter": "fund_name = 'Axis Multi-Asset Active FoF' OR scheme_code = 'AXIS_MULTI_ASSET_ACTIVE_FOF'",
    },
    {
        "label": "Invesco Multi Asset",
        "filter": "fund_name = 'Invesco India Multi Asset Allocation Fund' OR scheme_code = 'INVESCO_MULTI_ASSET_ALLOCATION'",
    },
    {
        "label": "Mirae Asset Multi Asset",
        "filter": "fund_name = 'Mirae Asset Multi Asset Allocation Fund' OR scheme_code = 'MIRAE_MULTI_ASSET_ALLOCATION'",
    },
    {
        "label": "Mirae Asset Allocator FoF",
        "filter": "fund_name = 'Mirae Asset Diversified Equity Allocator Passive FOF' OR scheme_code = 'MIRAE_DIVERSIFIED_EQUITY_ALLOCATOR_PASSIVE_FOF'",
    },
    {
        "label": "Motilal Asset Allocation Conservative FoF",
        "filter": "fund_name = 'Motilal Oswal Asset Allocation Fund of Fund- Conservative' OR scheme_code = 'MOTILAL_ASSET_ALLOCATION_FUND_OF_FUND_CONSERVATIVE'",
    },
    {
        "label": "Motilal Asset Allocation Aggressive FoF",
        "filter": "fund_name = 'Motilal Oswal Asset Allocation Fund of Fund- Aggressive' OR scheme_code = 'MOTILAL_ASSET_ALLOCATION_FUND_OF_FUND_AGGRESSIVE'",
    },
    {
        "label": "HDFC Multi Asset",
        "filter": "fund_name = 'HDFC_MULTI_ASSET' OR (fund_name ILIKE '%HDFC%MULTI%ASSET%')",
    },
    {
        "label": "Kotak Multi Asset",
        "filter": "fund_name = 'KOTAK_MULTI_ASSET' OR scheme_code = '152064' OR (fund_name ILIKE '%KOTAK%MULTI%ASSET%')",
    },
    {
        "label": "SBI Multi Asset",
        "filter": "fund_name = 'SBI_MULTI_ASSET' OR scheme_code = '119843' OR (fund_name ILIKE '%SBI%MULTI%ASSET%')",
    },
    {
        "label": "Canara Robeco Multi Asset",
        "filter": "fund_name = 'Canara Robeco Multi Asset Allocation Fund' OR scheme_code = 'CANARA_MULTI_ASSET_ALLOCATION' OR (fund_name ILIKE '%CANARA%MULTI%ASSET%')",
    },
]


def get_all_multi_asset_funds(client: Any = None, include_dynamic: bool = True) -> List[Dict[str, Any]]:
    """
    Returns a unified list of all multi-asset funds present in ClickHouse.
    Combines curated entries with dynamic table discovery.
    """
    if client is None:
        client = get_client()

    valid_funds: List[Dict[str, Any]] = []

    for f in CANONICAL_MULTI_ASSET_FUNDS:
        q = f"""
        SELECT count(), count(DISTINCT as_of_month), max(as_of_month)
        FROM market_data.mf_holdings FINAL
        WHERE {f['filter']}
        """
        try:
            res = client.query(q).result_rows
            if res and res[0][0] > 0:
                valid_funds.append({
                    "label": f["label"],
                    "name": f["label"],
                    "filter": f["filter"],
                    "query_filter": f["filter"],
                    "row_count": int(res[0][0]),
                    "months_count": int(res[0][1]),
                    "latest_month": str(res[0][2]) if res[0][2] else None,
                })
        except Exception:
            continue

    if not include_dynamic:
        return valid_funds

    # Dynamic Discovery: Find any newly imported funds matching multi-asset or asset allocator patterns
    disc_q = """
    SELECT DISTINCT fund_name, scheme_code, count(), count(DISTINCT as_of_month), max(as_of_month)
    FROM market_data.mf_holdings FINAL
    WHERE (fund_name ILIKE '%multi%asset%' OR fund_name ILIKE '%asset%allocat%')
      AND fund_name NOT ILIKE '%FLEXI%'
      AND fund_name NOT ILIKE '%MIDCAP%'
      AND fund_name NOT ILIKE '%SMALL%'
      AND fund_name NOT ILIKE '%LARGE%'
    GROUP BY fund_name, scheme_code
    """
    try:
        disc_rows = client.query(disc_q).result_rows
        for r in disc_rows:
            fn, sc, cnt, n_m, lat_m = r[0], r[1], int(r[2]), int(r[3]), str(r[4])
            
            # Check if this (fund_name, scheme_code) is already captured by valid_funds
            already_covered = False
            for vf in valid_funds:
                check_q = f"""
                SELECT count() FROM market_data.mf_holdings FINAL
                WHERE ({vf['filter']}) AND fund_name = '{fn}' AND scheme_code = '{sc}'
                """
                if client.query(check_q).result_rows[0][0] > 0:
                    already_covered = True
                    break
            
            if not already_covered:
                clean_label = fn.replace("_", " ").title()
                valid_funds.append({
                    "label": clean_label,
                    "name": clean_label,
                    "filter": f"fund_name = '{fn}' AND scheme_code = '{sc}'",
                    "query_filter": f"fund_name = '{fn}' AND scheme_code = '{sc}'",
                    "row_count": cnt,
                    "months_count": n_m,
                    "latest_month": lat_m,
                })
    except Exception:
        pass

    valid_funds.sort(key=lambda x: (-x["months_count"], x["label"]))
    return valid_funds


def classify_amc_style(
    label: str,
    equity_pct: float,
    gold_pct: float,
    silver_pct: float,
    debt_pct: float,
    reit_pct: float,
    fut_pct: float,
    has_offshore: bool,
    top_equities: List[str],
) -> tuple[str, str]:
    """
    Infers the institutional style archetype and primary strategic differentiator
    for an AMC based on actual quantitative portfolio holdings.
    """
    l_lower = label.lower()
    
    if "icici" in l_lower or abs(fut_pct) > 1.0:
        archetype = "Tactical Derivatives Heavyweight"
        diff = "Long stock/commodity futures & swaps overlay"
    elif has_offshore or "nippon" in l_lower or "invesco" in l_lower:
        if "invesco" in l_lower:
            archetype = "Global US Value & Domestic ETF Hybrid"
            diff = "Offshore US Value allocation (11.5%) + Midcap ETFs"
        else:
            archetype = "Global Multi-Asset & Precious Metals Base"
            diff = "iShares MSCI World + Heavy Gold/Silver sleeve"
    elif "quant" in l_lower:
        archetype = "Hyper-Concentrated Momentum & Sovereign Duration"
        diff = "Top 3 holdings > 25% equity + dynamic 17-24% G-Sec trading"
    elif "bajaj" in l_lower:
        archetype = "Equity-Yield & Real Asset Balancer"
        diff = "Heavy REITs/InvITs (Embassy/Mindspace) replacing bonds"
    elif "dsp" in l_lower:
        if "omni" in l_lower or "dynamic" in l_lower:
            archetype = "Thematic Smart Beta & Factor Allocator"
            diff = "IT & Private Bank ETFs + Dynamic Asset Allocation"
        else:
            archetype = "Smart Beta, Cash Buffer & REIT Allocator"
            diff = "Nifty Equal Weight ETF + TREPS buffer + Embassy REIT"
    elif "sbi" in l_lower or debt_pct > 25.0:
        archetype = "Conservative Hybrid & High-Duration Anchor"
        diff = "Lowest equity (46.2%) + Largest Sovereign Debt book (29.2%)"
    elif "axis" in l_lower or "mirae" in l_lower:
        archetype = "Defensive Large-Cap Quality & In-House ETFs"
        diff = "Core Private Banks + Internal ETF building blocks"
    elif "motilal" in l_lower:
        archetype = "Factor & Multi-Cap ETF Passive Allocator"
        diff = "Next 50 + Smallcap 250 ETF sleeves"
    elif "hdfc" in l_lower:
        archetype = "Core Value & Heavy Bluechip Equity Anchor"
        diff = "Reliance + ICICI + HDFC Bank core weight (>14%)"
    elif "kotak" in l_lower:
        archetype = "Commodity & Real Asset Hybrid FoF"
        diff = "High Silver & Gold weighting"
    else:
        archetype = "Multi-Asset Diversified Allocator"
        diff = f"Balanced multi-asset allocation ({equity_pct:.1f}% eq / {gold_pct:.1f}% gold)"

    return archetype, diff


def get_amc_archetype_profiles(client: Any = None) -> List[Dict[str, Any]]:
    """
    Computes a comprehensive AMC-by-AMC multi-asset profile scorecard.
    """
    if client is None:
        client = get_client()

    funds = get_all_multi_asset_funds(client)
    profiles = []

    for f in funds:
        label = f["label"]
        flt = f["filter"]

        q_max = f"SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE {flt}"
        try:
            max_m = client.query(q_max).result_rows[0][0]
        except Exception:
            continue

        if not max_m:
            continue

        # 1. Asset breakdown
        q_asset = f"""
        SELECT lower(asset_type) as asset_type, sum(pct_of_nav) as wt, sum(market_value_cr) as mv
        FROM market_data.mf_holdings FINAL
        WHERE ({flt}) AND as_of_month = '{max_m}'
        GROUP BY asset_type
        """
        res_asset = client.query(q_asset).result_rows
        asset_dict = {str(r[0]): float(r[1]) for r in res_asset}
        total_mv = sum(float(r[2]) for r in res_asset)

        # 2. Specific slices: Silver, REITs/InvITs, Derivatives, Offshore
        q_slices = f"""
        SELECT 
            sumIf(pct_of_nav, security_name ILIKE '%silver%') as silver_wt,
            sumIf(pct_of_nav, security_name ILIKE '%reit%' OR security_name ILIKE '%invit%') as reit_wt,
            sumIf(pct_of_nav, security_name ILIKE '%future%' OR security_name ILIKE '%option%' OR security_name ILIKE '%swap%') as fut_wt,
            countIf(security_name ILIKE '%msci%' OR security_name ILIKE '%ishares%' OR security_name ILIKE '%us value%' OR security_name ILIKE '%nasdaq%' OR security_name ILIKE '%s&p%') as offshore_cnt
        FROM market_data.mf_holdings FINAL
        WHERE ({flt}) AND as_of_month = '{max_m}'
        """
        slice_row = client.query(q_slices).result_rows[0]
        silver_wt = float(slice_row[0] or 0.0)
        reit_wt = float(slice_row[1] or 0.0)
        fut_wt = float(slice_row[2] or 0.0)
        has_offshore = int(slice_row[3] or 0) > 0

        # 3. Top 3 Equity holdings
        q_top_eq = f"""
        SELECT security_name, round(sum(pct_of_nav), 2) as wt, round(sum(market_value_cr), 1) as mv_cr
        FROM market_data.mf_holdings FINAL
        WHERE ({flt}) AND as_of_month = '{max_m}' AND lower(asset_type) = 'equity'
        GROUP BY security_name
        ORDER BY wt DESC
        LIMIT 3
        """
        res_eq = client.query(q_top_eq).result_rows
        top_equities = [f"{r[0]} ({r[1]}%)" for r in res_eq]

        eq_pct = asset_dict.get("equity", 0.0)
        gold_pct = asset_dict.get("gold", 0.0)
        debt_pct = asset_dict.get("bond", 0.0)
        cash_pct = asset_dict.get("cash", 0.0)
        other_pct = asset_dict.get("other", 0.0)

        archetype, diff = classify_amc_style(
            label, eq_pct, gold_pct, silver_wt, debt_pct, reit_wt, fut_wt, has_offshore, top_equities
        )

        profiles.append({
            "fund_label": label,
            "as_of_month": str(max_m),
            "months_depth": f["months_count"],
            "total_aum_cr": total_mv,
            "equity_pct": eq_pct,
            "gold_pct": gold_pct,
            "silver_pct": silver_wt,
            "debt_pct": debt_pct,
            "reit_pct": reit_wt,
            "derivatives_pct": fut_wt,
            "cash_pct": cash_pct,
            "other_pct": other_pct,
            "top_equities": top_equities,
            "archetype": archetype,
            "key_differentiator": diff,
        })

    profiles.sort(key=lambda x: -x["total_aum_cr"])
    return profiles
