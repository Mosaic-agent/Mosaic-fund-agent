"""
src/tools/market/mf_tools.py
─────────────────────────────
LangChain agent tools for Qdrant-backed mutual fund analysis.

Tools
─────
find_funds_holding       — which funds hold a stock / ISIN / asset type?
find_similar_funds       — find multi-asset funds with similar portfolio composition
search_mf_exposure       — find funds with significant exposure to a category
                           (commodity, gold, equity, bond, cash)
"""

from __future__ import annotations

import logging

from langchain_core.tools import tool

log = logging.getLogger(__name__)


@tool
def find_funds_holding(
    query: str,
    asset_type: str = "",
    top_k: int = 10,
) -> str:
    """
    Find which mutual funds hold a specific stock, ISIN, or asset class
    by searching the Qdrant mf_holdings collection.

    Use when the user asks:
      - "Which funds hold HDFC Bank?"
      - "Which multi-asset funds have gold ETF exposure?"
      - "Which AMC funds own Reliance Industries?"
      - "Which funds hold INE040A01034?"
      - "Which DSP or ICICI funds have silver exposure?"

    Args:
        query:      Stock name, ISIN, sector, or keyword
                    (e.g. "HDFC Bank", "GOLDBEES", "gold ETF", "INE040A01034")
        asset_type: Optional narrow filter: equity | gold | bond | cash | other
        top_k:      Max results to return (default 10)
    """
    from src.db.mf_vector import find_funds_holding_security

    results = find_funds_holding_security(query=query, k=top_k, asset_type=asset_type)

    if not results:
        return (
            f"No fund holdings found matching **{query}**"
            + (f" with asset_type={asset_type}" if asset_type else "")
            + ".\n\nThe `mf_holdings` Qdrant collection may be empty. "
            "Import fund holdings first: `import --category mf_holdings` "
            "or run `dsp`, `icici`, `nippon` importers."
        )

    lines = [
        f"## 📦 Funds Holding: {query}",
        f"{len(results)} match(es) found\n",
        "| Fund | Security | Asset Type | % NAV | Mkt Value (₹Cr) | Month | Similarity |",
        "| :--- | :--- | :--- | ---: | ---: | :--- | ---: |",
    ]
    for r in results:
        lines.append(
            f"| {r['fund_name']} | {r['security_name']} | `{r['asset_type']}` "
            f"| {r['pct_of_nav']:.2f}% | {r['market_value_cr']:.1f} "
            f"| {r['as_of_month']} | {r['similarity']:.3f} |"
        )

    # Group by fund for a quick summary
    fund_totals: dict[str, float] = {}
    for r in results:
        fund_totals[r["fund_name"]] = fund_totals.get(r["fund_name"], 0.0) + r["pct_of_nav"]

    if len(fund_totals) > 1:
        lines.append("\n**Summary by fund (total % NAV across matched holdings):**")
        for fn, pct in sorted(fund_totals.items(), key=lambda x: -x[1]):
            lines.append(f"- {fn}: {pct:.2f}%")

    return "\n".join(lines)


@tool
def find_similar_funds(
    fund_name: str,
    as_of_month: str = "",
    top_k: int = 5,
) -> str:
    """
    Find mutual funds with a similar multi-asset portfolio composition.
    Searches the Qdrant mf_fund_profiles collection by embedding the target
    fund's equity/gold/bond/cash allocation fingerprint.

    Use when the user asks:
      - "Which funds are similar to DSP_MULTI_ASSET?"
      - "Find multi-asset funds like ICICI_MULTI_ASSET"
      - "What other funds have a similar gold + equity mix to Quant Multi Asset?"
      - "Find funds with comparable asset allocation to Bajaj Multi Asset"

    Args:
        fund_name:   Fund name key (e.g. DSP_MULTI_ASSET, ICICI_MULTI_ASSET,
                     QUANT_MULTI_ASSET, BAJAJ_MULTI_ASSET)
        as_of_month: YYYY-MM reference month (blank = latest)
        top_k:       Number of similar funds to return (default 5)
    """
    from src.db.mf_vector import find_similar_fund_profiles

    results = find_similar_fund_profiles(
        fund_name=fund_name.upper(), as_of_month=as_of_month, k=top_k
    )

    if not results:
        return (
            f"No fund profiles found similar to **{fund_name}**.\n\n"
            "The `mf_fund_profiles` Qdrant collection may be empty. "
            "Import fund holdings first so profiles are built automatically."
        )

    lines = [
        f"## 🔍 Funds Similar to {fund_name.upper()}",
        f"Sorted by portfolio composition similarity | top {len(results)}\n",
        "| Fund | Month | Equity % | Gold % | Bond % | Cash % | Primary | Similarity |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | :--- | ---: |",
    ]
    for r in results:
        lines.append(
            f"| {r['fund_name']} | {r['as_of_month']} "
            f"| {r['equity_pct']:.1f}% | {r['gold_pct']:.1f}% "
            f"| {r['bond_pct']:.1f}% | {r['cash_pct']:.1f}% "
            f"| `{r['primary']}` | {r['similarity']:.3f} |"
        )
        if r.get("lead_fund_manager"):
            lines.append(f"   Fund Manager: {r['lead_fund_manager']}")
    lines.append("\n**Top holdings of most similar fund:**")
    lines.append(f"> {results[0]['top5_text']}")

    return "\n".join(lines)


@tool
def search_mf_exposure(
    category: str,
    query: str = "",
    top_k: int = 8,
) -> str:
    """
    Find mutual funds with significant exposure to a specific asset category
    (commodity, gold, equity, bond, debt, cash) by searching mf_fund_profiles.

    Use when the user asks:
      - "Which multi-asset funds have the most gold allocation?"
      - "Find funds with commodity exposure"
      - "Which funds are heavily invested in equities?"
      - "Show me debt-heavy multi-asset funds"
      - "Which AMC funds have silver or precious metals?"

    Args:
        category:  Asset category — gold | equity | bond | cash | other
                   Also accepts aliases: commodity → gold, debt → bond
        query:     Additional context
                   (e.g. "precious metal", "large-cap", "government bonds")
        top_k:     Max results (default 8)
    """
    from src.db.mf_vector import find_funds_by_category

    # Normalise aliases
    _aliases = {"commodity": "gold", "precious metal": "gold",
                 "debt": "bond", "fixed income": "bond", "liquid": "cash"}
    cat_norm = _aliases.get(category.lower().strip(), category.lower().strip())

    results = find_funds_by_category(asset_type=cat_norm, query=query, k=top_k)

    if not results:
        return (
            f"No fund profiles found with primary exposure to **{category}**"
            + (f" ({query})" if query else "")
            + ".\n\nThe `mf_fund_profiles` Qdrant collection may be empty."
        )

    lines = [
        f"## 🏷️ Funds with {category.title()} Exposure",
        f"Primary asset type: `{cat_norm}`"
        + (f" | context: {query}" if query else "")
        + f" | {len(results)} fund(s)\n",
        "| Fund | Month | Gold % | Equity % | Bond % | Cash % | Top Holdings | Score |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | :--- | ---: |",
    ]
    for r in results:
        lines.append(
            f"| {r['fund_name']} | {r['as_of_month']} "
            f"| {r['gold_pct']:.1f}% | {r['equity_pct']:.1f}% "
            f"| {r['bond_pct']:.1f}% | {r['cash_pct']:.1f}% "
            f"| {r['top5_text'][:60]}… | {r['similarity']:.3f} |"
        )

    return "\n".join(lines)


@tool
def get_mf_holdings_by_cap_category(
    cap_category: str = "Small Cap",
    fund_filter: str = "multi_asset",
    min_funds: int = 1,
    top_n: int = 25,
) -> str:
    """
    Query mutual fund equity holdings filtered by official statutory SEBI/AMFI
    market cap category (Small Cap, Mid Cap, or Large Cap).

    Joins market_data.mf_holdings with market_data.amfi_market_cap on ISIN.

    Args:
        cap_category: 'Small Cap' | 'Mid Cap' | 'Large Cap' (default: 'Small Cap')
        fund_filter:  'multi_asset' (default) | 'dsp' | 'all' | or AMC name substring
        min_funds:    Minimum number of distinct funds holding the stock (default: 1)
        top_n:        Maximum rows to return (default: 25)
    """
    from src.db.pool import get_pool
    from src.db.repository import MarketDataRepository

    repo = MarketDataRepository(get_pool())

    # Normalize category
    cat_clean = "Small Cap"
    if "mid" in cap_category.lower():
        cat_clean = "Mid Cap"
    elif "large" in cap_category.lower():
        cat_clean = "Large Cap"

    # Build fund condition
    ff = fund_filter.lower().strip()
    if ff in ("multi_asset", "multi asset", "multi-asset"):
        fund_cond = "(fund_name ILIKE '%Multi%Asset%' OR fund_name ILIKE '%Dynamic Asset%')"
    elif ff == "dsp":
        fund_cond = "fund_name ILIKE 'DSP%'"
    elif ff == "all" or not ff:
        fund_cond = "1=1"
    else:
        fund_cond = f"fund_name ILIKE '%{fund_filter}%'"

    sql = f"""
    WITH latest_holdings AS (
        SELECT fund_name, isin, security_name, market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month >= today() - 90
          AND asset_type = 'equity'
          AND {fund_cond}
    )
    SELECT 
        c.rank AS amfi_rank,
        c.company_name,
        c.nse_symbol,
        c.avg_mcap_cr AS amfi_6m_avg_mcap_cr,
        count(DISTINCT h.fund_name) AS fund_count,
        groupArray(DISTINCT h.fund_name) AS holding_funds,
        round(sum(h.market_value_cr), 2) AS total_market_val_cr,
        round(avg(h.pct_of_nav), 2) AS avg_pct_of_nav
    FROM latest_holdings h
    INNER JOIN market_data.amfi_market_cap c FINAL
        ON h.isin = c.isin
    WHERE c.cap_category = '{cat_clean}'
    GROUP BY c.rank, c.company_name, c.nse_symbol, c.avg_mcap_cr
    HAVING fund_count >= {min_funds}
    ORDER BY fund_count DESC, total_market_val_cr DESC
    LIMIT {top_n}
    """

    try:
        df = repo._qdf(sql)
    except Exception as exc:
        log.error("Failed to query MF holdings by cap category: %s", exc)
        return f"Error querying MF holdings by cap category: {exc}"

    if df.empty:
        return f"No {cat_clean} holdings found matching fund_filter='{fund_filter}' in recent disclosures."

    lines = [
        f"## 🏛️ Top {cat_clean} Holdings in {fund_filter.title()} Funds",
        f"Official AMFI Statutory Classification (Joined on `isin`) | Total Matches: {len(df)}\n",
        "| Rank | Company Name | Symbol | 6M Avg Mcap (₹ Cr) | # Funds | Total Value (₹ Cr) | Avg % NAV | Holding Funds |",
        "| :---: | :--- | :---: | ---: | :---: | ---: | ---: | :--- |",
    ]

    for _, row in df.iterrows():
        rank = row["amfi_rank"]
        cname = row["company_name"]
        sym = row["nse_symbol"] or "-"
        mcap = f"₹{row['amfi_6m_avg_mcap_cr']:,.2f}"
        fcount = row["fund_count"]
        val = f"₹{row['total_market_val_cr']:,.2f}"
        pnav = f"{row['avg_pct_of_nav']:.2f}%"
        funds = ", ".join(row["holding_funds"][:3])
        if len(row["holding_funds"]) > 3:
            funds += f" (+{len(row['holding_funds']) - 3} more)"

        lines.append(f"| #{rank} | {cname} | `{sym}` | {mcap} | {fcount} | {val} | {pnav} | {funds} |")

    return "\n".join(lines)


MF_QDRANT_TOOLS = [find_funds_holding, find_similar_funds, search_mf_exposure, get_mf_holdings_by_cap_category]

