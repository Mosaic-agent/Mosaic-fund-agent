"""
src/workflows/consolidated_mf_report.py
────────────────────────────────────────
Workflow to compile a consolidated Multi-Asset holdings, consensus, and RAG report.
"""

from datetime import date
from typing import Dict, Any
import pandas as pd
import numpy as np

from src.db.pool import get_pool
from src.tools.report_publisher import publish_consolidated_pdf
from src.tools.news_search import get_stock_news
from src.scripts.portfolio.multi_asset_consensus import cross_fund_consensus

def _style_axes(ax, bg: str = "#0d0d1a") -> None:
    """Style matplotlib axes with consistent dark-theme styling."""
    ax.set_facecolor(bg)
    ax.tick_params(colors="#cccccc", labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")
    ax.yaxis.label.set_color("#cccccc")
    ax.xaxis.label.set_color("#cccccc")
    ax.title.set_color("#e0e0ff")
    ax.grid(color="#333355", linewidth=0.4, alpha=0.6, axis='y')

def _render_allocation_chart(df_matrix: pd.DataFrame) -> str:
    """Render a stacked bar chart of asset allocations by fund, returning base64 string."""
    import matplotlib.pyplot as plt
    import io
    import base64
    
    categories = ['Equity', 'Gold', 'Bond', 'Cash', 'Other']
    plot_df = df_matrix.copy()
    for cat in categories:
        plot_df[cat] = plot_df[cat].str.rstrip('%').astype(float)
        
    BG = "#0d0d1a"
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
    _style_axes(ax, BG)
    
    funds = plot_df["Fund"].tolist()
    bottoms = np.zeros(len(funds))
    
    colors = {
        'Equity': '#4da6ff',
        'Gold': '#ffd700',
        'Bond': '#22c55e',
        'Cash': '#a855f7',
        'Other': '#6b7280'
    }
    
    for cat in categories:
        vals = plot_df[cat].values
        ax.bar(funds, vals, bottom=bottoms, label=cat, color=colors[cat], width=0.4)
        bottoms += vals
        
    ax.set_ylabel("Allocation Percentage (%)")
    ax.set_title("Fund-level Asset Allocation Comparison", fontsize=12, pad=10)
    ax.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333355", labelcolor="#cccccc", loc="upper left", bbox_to_anchor=(1.02, 1))
    plt.tight_layout(pad=1.0)
    
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=BG)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return b64

def _render_shifts_chart(adds_df: pd.DataFrame, trims_df: pd.DataFrame) -> str:
    """Render consensus adds/trims shifts as a horizontal bar chart, returning base64 string."""
    import matplotlib.pyplot as plt
    import io
    import base64
    
    a_subset = adds_df.head(5).copy()
    t_subset = trims_df.head(5).copy()
    
    items = []
    colors = []
    
    for idx, row in t_subset.iterrows():
        name = row["canonical_name"]
        if len(name) > 30:
            name = name[:27] + "..."
        items.append((name, row["avg_delta"]))
        colors.append("#ef4444")
        
    for idx, row in a_subset.iterrows():
        name = row["canonical_name"]
        if len(name) > 30:
            name = name[:27] + "..."
        items.append((name, row["avg_delta"]))
        colors.append("#22c55e")
        
    if not items:
        return ""
        
    items.reverse()
    colors.reverse()
    
    names = [x[0] for x in items]
    deltas = [x[1] for x in items]
    
    BG = "#0d0d1a"
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
    _style_axes(ax, BG)
    
    ax.grid(color="#333355", linewidth=0.4, alpha=0.6, axis='x')
    
    y_pos = np.arange(len(names))
    ax.barh(y_pos, deltas, color=colors, height=0.5)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=8)
    ax.axvline(0, color="#ffffff", linewidth=0.8, linestyle="--", alpha=0.5)
    
    ax.set_xlabel("Average Weight Delta (Avg % NAV Change)")
    ax.set_title("Consensus Active Shifts (Top Adds vs Top Trims)", fontsize=12, pad=10)
    plt.tight_layout(pad=1.0)
    
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=BG)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return b64

def _get_news_symbol(name: str) -> str:
    name_lower = name.lower()
    if "gold bees" in name_lower or "gold etf" in name_lower:
        return "GOLDBEES"
    if "silver etf" in name_lower or "silver bees" in name_lower:
        return "SILVERBEES"
    if "hdfc bank" in name_lower:
        return "HDFCBANK"
    if "icici bank" in name_lower:
        return "ICICIBANK"
    if "reliance" in name_lower:
        return "RELIANCE"
    if "towers" in name_lower:
        return "INDUSTOWER"
    if "ntpc" in name_lower:
        return "NTPC"
    if "muthoot" in name_lower:
        return "MUTHOOTFIN"
    if "unilever" in name_lower:
        return "HINDUNILVR"
    # Fallback: take first 2 words
    words = name.split()
    return " ".join(words[:2])

def _format_news(news_dict: Dict[str, Any]) -> str:
    articles = news_dict.get("articles", [])
    if not articles:
        return "*No recent articles found in Qdrant news cache.*"
    lines = []
    for a in articles[:3]:
        lines.append(f"- **{a.get('title')}** ({a.get('source')})")
        if a.get('url'):
            lines.append(f"  [Read article]({a.get('url')})")
    return "\n".join(lines)

def build_consolidated_report(output_pdf_path: str = "output/consolidated_multi_asset_report.pdf") -> str:
    pool = get_pool()
    
    # ── 1. Side-by-Side Asset Allocation Matrix ──────────────────────────────
    funds = [
        ("Nippon Multi Asset",     "scheme_code = 'RLMF806'"),
        ("Nippon FoF",             "scheme_code = 'RLMF811'"),
        ("DSP Multi Asset",        "scheme_code = '152056'"),
        ("DSP Omni FoF",           "scheme_code = '154167'"),
        ("Bajaj Multi Asset",      "scheme_code = '152639'"),
        ("Quant Multi Asset",      "scheme_code = '120821'"),
        ("ICICI Multi Asset",      "scheme_code = '120334'"),
    ]
    
    alloc_data = []
    for label, flt in funds:
        latest_month_row = pool.query_df(f"SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE {flt}").to_dict("records")
        if not latest_month_row or not latest_month_row[0]['max(as_of_month)']:
            continue
        latest_month = latest_month_row[0]['max(as_of_month)']
        latest_month_str = str(latest_month)[:10]
        
        alloc_rows = pool.query_df(f'''
            SELECT asset_type, sum(if(isNaN(pct_of_nav), 0, pct_of_nav)) AS pct
            FROM market_data.mf_holdings FINAL
            WHERE {flt} AND as_of_month = '{latest_month_str}'
            GROUP BY asset_type
        ''').to_dict("records")
        
        row_dict = {"Fund": label, "Month": latest_month_str[:7]}
        for a in alloc_rows:
            row_dict[a["asset_type"].capitalize()] = f"{a['pct']:.2f}%"
        alloc_data.append(row_dict)
        
    df_matrix = pd.DataFrame(alloc_data).fillna("0.00%")
    # Ensure correct columns order
    cols_order = ["Fund", "Month", "Equity", "Gold", "Bond", "Cash", "Other"]
    for c in cols_order:
        if c not in df_matrix.columns:
            df_matrix[c] = "0.00%"
    df_matrix = df_matrix[cols_order]
    
    alloc_table_md = df_matrix.to_markdown(index=False)
    alloc_chart_b64 = _render_allocation_chart(df_matrix)
    
    # ── 2. Consensus Adds & Trims ────────────────────────────────────────────
    df_shifts, _, _, _ = cross_fund_consensus("mom", None, 0.10)
    
    adds_table_md = "*No consensus adds.*"
    trims_table_md = "*No consensus trims.*"
    
    top_add_sym = ""
    top_trim_sym = ""
    shifts_chart_b64 = ""
    
    if not df_shifts.empty:
        adds = df_shifts[df_shifts['n_funds_add'] >= 2].sort_values(['n_funds_add', 'avg_delta'], ascending=[False, False])
        if not adds.empty:
            adds_table_md = adds[['canonical_name', 'asset_type', 'n_funds_add', 'avg_delta', 'funds_moving']].rename(
                columns={"canonical_name": "Security", "asset_type": "Asset", "n_funds_add": "# Funds", "avg_delta": "Avg Δ", "funds_moving": "Funds Adding"}
            ).head(10).to_markdown(index=False)
            top_add_sym = _get_news_symbol(adds.iloc[0]['canonical_name'])
            
        trims = df_shifts[df_shifts['n_funds_trim'] >= 2].sort_values(['n_funds_trim', 'avg_delta'], ascending=[False, True])
        if not trims.empty:
            trims_table_md = trims[['canonical_name', 'asset_type', 'n_funds_trim', 'avg_delta', 'funds_moving']].rename(
                columns={"canonical_name": "Security", "asset_type": "Asset", "n_funds_trim": "# Funds", "avg_delta": "Avg Δ", "funds_moving": "Funds Trimming"}
            ).head(10).to_markdown(index=False)
            top_trim_sym = _get_news_symbol(trims.iloc[0]['canonical_name'])
            
        if not adds.empty or not trims.empty:
            shifts_chart_b64 = _render_shifts_chart(adds, trims)

    # ── 3. RAG Breaking News ─────────────────────────────────────────────────
    news_add_content = "*No symbol selected.*"
    news_trim_content = "*No symbol selected.*"
    
    if top_add_sym:
        news_add = get_stock_news.invoke({"input_str": top_add_sym})
        news_add_content = _format_news(news_add)
        
    if top_trim_sym:
        news_trim = get_stock_news.invoke({"input_str": top_trim_sym})
        news_trim_content = _format_news(news_trim)

    # ── 4. Compile Markdown Content ──────────────────────────────────────────
    alloc_chart_html = f'<div class="chart-inline"><img src="data:image/png;base64,{alloc_chart_b64}" alt="Allocation Comparison Chart"/></div>' if alloc_chart_b64 else ""
    shifts_chart_html = f'<div class="chart-inline"><img src="data:image/png;base64,{shifts_chart_b64}" alt="Consensus Shifts Chart"/></div>' if shifts_chart_b64 else ""

    md_content = f"""# Consolidated Multi-Asset Fund Report
*Generated on: {date.today().strftime('%Y-%m-%d')}*

This report aggregates EOD holdings disclosures across active multi-asset allocation funds and enriches them with vector-search RAG news context to explain macro drivers.

---

## 1. Asset Allocation Overview
The table and chart below present a side-by-side comparison of the latest asset allocations (% NAV) across all tracked multi-asset funds.

{alloc_chart_html}

{alloc_table_md}

---

## 2. Consensus Shifts (Adds & Trims)
The following chart and tables summarize active portfolio rebalancing consensus (where \\ge 2 funds adjusted weights in the same direction).

{shifts_chart_html}

### Consensus ADDS (MoM)
{adds_table_md}

### Consensus TRIMS (MoM)
{trims_table_md}

---

## 3. RAG Context: Breaking News & Macro Drivers
Below is the RAG-retrieved financial news context for the top rotated holdings.

### Top Accumulation Target: **{top_add_sym or 'None'}**
{news_add_content}

### Top Trimming Target: **{top_trim_sym or 'None'}**
{news_trim_content}

---
"""
    
    # ── 5. Publish to PDF ────────────────────────────────────────────────────
    import os
    stem = os.path.splitext(os.path.basename(output_pdf_path))[0]
    
    # Pass empty symbols to suppress stock price charts at the top,
    # showing only our custom fund asset allocation & shifts charts.
    pdf_path = publish_consolidated_pdf.invoke({
        "report_markdown": md_content,
        "symbols": "",
        "title": "Consolidated Multi-Asset Institutional Report",
        "filename": stem
    })
    return pdf_path
