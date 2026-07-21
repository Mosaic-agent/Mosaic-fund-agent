"""
src/tools/whale_tools.py
─────────────────────────
LangChain @tool wrappers for the Cross-AMC Whale Accumulation Scanner.
Exposes two agent-accessible tools:

  scan_whale_accumulation(amc, lookback_months, min_amcs)
    → Scans all AMC holdings for consensus accumulation signals.

  get_whale_consensus(symbol)
    → Looks up a single stock across all AMCs: who holds it, trend, and conviction.
"""

from __future__ import annotations

import logging

from langchain_core.tools import tool

log = logging.getLogger(__name__)


@tool
def scan_whale_accumulation(
    amc: str = "all",
    lookback_months: int = 3,
    min_amcs: int = 2,
) -> str:
    """
    Scan mutual fund holdings for stocks where multiple independent AMCs
    are simultaneously building positions (cross-AMC consensus signal).

    The consensus_score = num_amcs × avg_weight_delta_pp — a higher score means
    more AMC groups are each adding weight in the same stock.

    Use when the user asks:
      - "What are institutions accumulating?"
      - "Which stocks are multiple AMCs buying together?"
      - "Show me cross-AMC consensus buys"
      - "Whale accumulation scan"
      - "New stock entries by active funds"

    Args:
        amc:             AMC filter — "all", "dsp", "nippon", "bajaj", "icici", "quant"
        lookback_months: MoM comparison window in months (default 3)
        min_amcs:        Minimum distinct AMC groups to qualify as consensus (default 2)

    Returns a formatted Markdown report with:
      - Top Accumulators (sorted by consensus_score)
      - Zero-to-Hero fresh entries (stocks newly entered by min_amcs+ AMCs)
      - Largest institutional bets by ₹ value
    """
    from src.scripts.portfolio.whale_accumulation_scanner import run_whale_scan

    results = run_whale_scan(
        amc=amc,
        lookback_months=lookback_months,
        min_amcs=min_amcs,
    )

    if "error" in results:
        return f"❌ Scan failed: {results['error']}"

    as_of = results["as_of"]
    prev  = results["prev_month"]
    lines = [
        f"## 🐋 Whale Accumulation Scan  (as of {as_of} vs {prev})",
        f"AMC filter: **{results['amc_filter'].upper()}**  ·  "
        f"min_amcs: **{results['min_amcs']}**  ·  "
        f"lookback: **{results['lookback_months']}M**",
        "",
    ]

    # Top accumulators
    acc = results["top_accumulators"]
    if acc:
        lines += [
            "### 🔺 Top Consensus Accumulators",
            "",
            "| Security | Score | AMCs | AMC Groups | Avg Δ (pp) | Total (₹ Cr) |",
            "| :--- | ---: | ---: | :--- | ---: | ---: |",
        ]
        for r in acc[:15]:
            lines.append(
                f"| {r['security_name']} | {r['consensus_score']:.3f} "
                f"| {r['num_amcs']} | {', '.join(r['amcs'])} "
                f"| {r['avg_delta_pp']:+.3f} | ₹{r['total_value_cr']:,.2f} |"
            )
        lines.append("")

    # Fresh entries
    fresh = results["fresh_entries"]
    if fresh:
        lines += [
            f"### 🆕 Zero-to-Hero: Fresh Entries (last {results['lookback_months']}M)",
            "",
            "| Security | AMCs Entered | # | Total (₹ Cr) | Combined Wt % |",
            "| :--- | :--- | ---: | ---: | ---: |",
        ]
        for r in fresh[:12]:
            lines.append(
                f"| {r['security_name']} | {', '.join(r['amcs_entered'])} "
                f"| {r['num_amcs']} | ₹{r['total_value_cr']:,.2f} | {r['total_pct']:.3f}% |"
            )
        lines.append("")

    if not acc and not fresh:
        lines.append(
            "_No consensus accumulation found. Try `min_amcs=1` or a longer `lookback_months`._"
        )

    return "\n".join(lines)


@tool
def get_whale_consensus(symbol: str) -> str:
    """
    Look up a single NSE stock across ALL active AMC mutual funds to show:
      - Which AMC groups currently hold it and their weight (% of NAV)
      - Month-over-month delta (accumulating or trimming)
      - Total institutional market value
      - How many months each fund has held it (conviction tenure)

    Use when the user asks:
      - "What is the institutional consensus on SANSERA?"
      - "Which AMCs hold RELIANCE?"
      - "Is DSP or Nippon buying TATAELXSI?"
      - "Show whale holdings for CGPOWER"

    Args:
        symbol: NSE trading symbol (e.g. SANSERA, RELIANCE, CGPOWER) OR
                a partial company name (e.g. "Sansera Engineering").
    """
    from src.db.pool import query_df
    import pandas as pd

    sym_upper = symbol.strip().upper()

    # ── Fetch holdings where security_name matches symbol ────────────────────
    try:
        df = query_df(
            """
            SELECT fund_name, as_of_month,
                   security_name,
                   toFloat64(pct_of_nav) AS pct_of_nav,
                   toFloat64(market_value_cr) AS market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE (
                security_name ILIKE {sym:String}
                OR security_name ILIKE {sym_like:String}
            )
            AND asset_type IN ('Equity', 'equity', 'EQUITY')
            ORDER BY fund_name, as_of_month ASC
            """,
            parameters={"sym": sym_upper, "sym_like": f"%{sym_upper}%"},
        )
    except Exception as exc:
        # Fallback: try without parametrised query for broader match
        try:
            df = query_df(
                f"""
                SELECT fund_name, as_of_month, security_name,
                       toFloat64(pct_of_nav) AS pct_of_nav,
                       toFloat64(market_value_cr) AS market_value_cr
                FROM market_data.mf_holdings FINAL
                WHERE security_name ILIKE '%{sym_upper}%'
                ORDER BY fund_name, as_of_month ASC
                """
            )
        except Exception as exc2:
            return f"❌ Query failed: {exc2}"

    if df.empty:
        return (
            f"No mutual fund holdings found for **{symbol}** in ClickHouse. "
            "The stock may not be held by any tracked active AMC, or the name "
            "spelling may differ (try NSE symbol, e.g. 'SANSERA')."
        )

    # Filter passives
    from src.scripts.portfolio.whale_accumulation_scanner import _is_passive, _get_amc_group
    df = df[~df["fund_name"].apply(_is_passive)].copy()
    if df.empty:
        return f"No **active** fund holdings found for **{symbol}** (only passive/index funds hold it)."

    df["amc_group"] = df["fund_name"].apply(_get_amc_group)
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])

    # Resolve actual security name from data
    security_name = df["security_name"].value_counts().index[0]

    # Latest 2 months
    months = sorted(df["as_of_month"].unique())
    latest = months[-1]
    prev   = months[-2] if len(months) >= 2 else None

    df_lat = df[df["as_of_month"] == latest]
    df_prv = df[df["as_of_month"] == prev] if prev is not None else pd.DataFrame()

    lat_agg = df_lat.groupby(["fund_name", "amc_group"]).agg(
        pct=("pct_of_nav", "sum"),
        value_cr=("market_value_cr", "sum"),
    ).reset_index()

    if not df_prv.empty:
        prv_agg = df_prv.groupby("fund_name").agg(prev_pct=("pct_of_nav", "sum")).reset_index()
        lat_agg = lat_agg.merge(prv_agg, on="fund_name", how="left")
        lat_agg["prev_pct"] = lat_agg["prev_pct"].fillna(0.0)
        lat_agg["delta_pp"] = lat_agg["pct"] - lat_agg["prev_pct"]
    else:
        lat_agg["prev_pct"] = 0.0
        lat_agg["delta_pp"] = lat_agg["pct"]

    # Months held per fund
    tenure = df.groupby("fund_name")["as_of_month"].nunique().rename("months_held")
    lat_agg = lat_agg.merge(tenure, on="fund_name", how="left")

    # Sort by value desc
    lat_agg = lat_agg.sort_values("value_cr", ascending=False)

    total_value = float(lat_agg["value_cr"].sum())
    num_funds   = len(lat_agg)
    amcs        = sorted(lat_agg["amc_group"].unique().tolist())

    lines = [
        f"## 🐋 Institutional Consensus: {security_name}",
        f"**As of:** {latest.strftime('%B %Y')}  ·  "
        f"**Funds holding:** {num_funds}  ·  "
        f"**AMC groups:** {', '.join(amcs)}  ·  "
        f"**Total AUM:** ₹{total_value:,.2f} Cr",
        "",
        "| Fund | AMC | Wt % NAV | Prev Wt % | Δ (pp) | Value (₹ Cr) | Months Held |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for _, r in lat_agg.iterrows():
        delta_str = f"{r['delta_pp']:+.3f}" if not pd.isna(r["delta_pp"]) else "—"
        prev_str  = f"{r['prev_pct']:.3f}" if r["prev_pct"] > 0 else "0.000"
        trend_icon = "↑" if r["delta_pp"] > 0 else ("↓" if r["delta_pp"] < 0 else "→")
        lines.append(
            f"| {r['fund_name']} | {r['amc_group']} "
            f"| {r['pct']:.3f} | {prev_str} "
            f"| {trend_icon} {delta_str} "
            f"| ₹{r['value_cr']:,.2f} | {int(r.get('months_held', 1))} |"
        )

    lines.append("")

    # Summary conviction
    adding = lat_agg[lat_agg["delta_pp"] > 0]
    trimming = lat_agg[lat_agg["delta_pp"] < 0]
    if len(adding) > len(trimming):
        lines.append(
            f"**Trend:** {len(adding)}/{num_funds} funds **accumulating** (+{adding['delta_pp'].sum():+.2f}pp combined). "
            f"Strong institutional conviction."
        )
    elif len(trimming) > len(adding):
        lines.append(
            f"**Trend:** {len(trimming)}/{num_funds} funds **trimming** ({trimming['delta_pp'].sum():.2f}pp combined). "
            f"Institutions reducing exposure."
        )
    else:
        lines.append(f"**Trend:** Mixed — {len(adding)} adding, {len(trimming)} trimming.")

    return "\n".join(lines)


WHALE_TOOLS = [scan_whale_accumulation, get_whale_consensus]
