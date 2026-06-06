"""
src/tools/market/correlation_tools.py
──────────────────────────────────────
LangChain `@tool` integration for mapping stock price anomalies to corporate actions
filings and global macro events.
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from langchain_core.tools import tool

from src.db.pool import query_df
from src.ml.correlation import CorrelationService, EventType
from src.tools.visualization.correlation_chart import (
    render_correlation_timeline_png,
    render_lead_lag_grid_png,
)

log = logging.getLogger(__name__)


@tool
def find_anomaly_correlations(symbol: str, lookback_days: int = 365) -> str:
    """
    Scans historical stock price anomalies and maps them to qualitative event triggers
    (insider leaks, corporate actions, regulatory filings, and global macro shocks).
    Saves visual timeline and lead-lag scatter charts to the output directory and
    returns a text-based summary of mapped correlations.
    """
    sym = symbol.strip().upper()
    log.info("Finding anomaly correlations for %s (lookback: %d days)", sym, lookback_days)

    # 1. Fetch OHLCV data for target symbol
    try:
        df_ohlcv = query_df(
            """
            SELECT trade_date,
                   toFloat64(argMax(open,   imported_at)) AS open,
                   toFloat64(argMax(high,   imported_at)) AS high,
                   toFloat64(argMax(low,    imported_at)) AS low,
                   toFloat64(argMax(close,  imported_at)) AS close,
                   toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = {sym:String}
            GROUP BY trade_date ORDER BY trade_date ASC
            """,
            parameters={"sym": sym},
        )
    except Exception as e:
        return f"❌ Failed to fetch price history for {sym} from ClickHouse: {e}"

    if df_ohlcv.empty:
        return f"❌ No price data found in ClickHouse for symbol '{sym}'"

    # Filter to lookback window
    df_ohlcv["trade_date"] = pd.to_datetime(df_ohlcv["trade_date"])
    cutoff = datetime.now() - pd.Timedelta(days=lookback_days)
    df_ohlcv = df_ohlcv[df_ohlcv["trade_date"] >= cutoff].copy()

    if len(df_ohlcv) < 5:
        return f"❌ Too few rows ({len(df_ohlcv)}) in the lookback window for {sym}."

    # 2. Fetch benchmark prices (NIFTYBEES)
    df_benchmark = None
    try:
        df_benchmark = query_df(
            """
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices FINAL
            WHERE symbol = 'NIFTYBEES'
            GROUP BY trade_date ORDER BY trade_date ASC
            """
        )
        if not df_benchmark.empty:
            df_benchmark["trade_date"] = pd.to_datetime(df_benchmark["trade_date"])
    except Exception as e:
        log.warning("Could not fetch NIFTYBEES benchmark data: %s", e)

    # 2b. Fetch USDINR rates for statistical validation
    df_usdinr = pd.DataFrame()
    try:
        df_usdinr = query_df(
            """
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.fx_rates FINAL
            WHERE symbol = 'USDINR'
            GROUP BY trade_date ORDER BY trade_date ASC
            """
        )
        if not df_usdinr.empty:
            df_usdinr["trade_date"] = pd.to_datetime(df_usdinr["trade_date"])
    except Exception as e:
        log.warning("Could not fetch USDINR rates for statistical validation: %s", e)

    # 3. Execute Correlation Service
    service = CorrelationService()
    findings = service.find_correlations(sym, df_ohlcv, df_benchmark, lookback_days)

    # Pre-calculate daily returns and historical percentiles for findings
    df_stock = df_ohlcv.sort_values("trade_date").copy()
    df_stock["stock_return"] = df_stock["close"].pct_change()
    all_abs_returns = df_stock["stock_return"].abs().dropna()

    date_to_stats = {}
    for idx, row in df_stock.iterrows():
        t_date = row["trade_date"].date()
        ret_val = row["stock_return"]
        if pd.isna(ret_val):
            ret_val = 0.0
        abs_ret = abs(ret_val)
        if len(all_abs_returns) > 0:
            percentile = (all_abs_returns < abs_ret).mean() * 100.0
        else:
            percentile = 0.0
        date_to_stats[t_date] = (ret_val, percentile)

    # Calculate FX Statistical Validation (USDINR)
    validation_block = ""
    correlation = 0.0
    beta_fx = 0.0
    r_squared = 0.0
    p_value = 1.0
    if not df_usdinr.empty and not df_ohlcv.empty:
        df_fx_sorted = df_usdinr.sort_values("trade_date").copy()
        df_fx_sorted["fx_return"] = df_fx_sorted["close"].pct_change()

        df_merged = pd.merge(
            df_stock[["trade_date", "stock_return"]],
            df_fx_sorted[["trade_date", "fx_return"]],
            on="trade_date",
        ).dropna()

        if len(df_merged) >= 10:
            try:
                import scipy.stats as stats
                correlation, p_value = stats.pearsonr(df_merged["stock_return"], df_merged["fx_return"])
            except Exception as e:
                log.warning("scipy.stats.pearsonr failed, falling back: %s", e)
                correlation = df_merged["stock_return"].corr(df_merged["fx_return"])
                p_value = 1.0

            r_squared = correlation ** 2
            cov = df_merged["stock_return"].cov(df_merged["fx_return"])
            var_fx = df_merged["fx_return"].var()
            beta_fx = cov / var_fx if var_fx > 0 else 0.0

            significance = "Weak / Insignificant"
            if abs(correlation) >= 0.4:
                significance = "Strong"
            elif abs(correlation) >= 0.2:
                significance = "Moderate"

            direction = "positive" if beta_fx > 0 else "inverse (negative)"
            sig_text = "statistically significant" if p_value < 0.05 else "not statistically significant"
            interpretation = (
                f"USDINR explains ~{r_squared*100:.1f}% of {sym} return variance. "
                f"The relationship is {sig_text} (p-value = {p_value:.4f})."
            )

            validation_block = (
                "\n### 💱 FX Statistical Validation (USDINR)\n"
                f"• **Correlation Coefficient ($r$):** `{correlation:.4f}` ({significance} linear correlation)\n"
                f"• **Beta ($\\beta_{{FX}}$) relative to USDINR:** `{beta_fx:.4f}`\n"
                f"• **R² (Coefficient of Determination):** `{r_squared:.4f}`\n"
                f"• **p-value:** `{p_value:.4f}`\n"
                f"• **Interpretation:** {interpretation}\n"
            )

    # Calculate Root Cause Attribution & Strength summary statistics
    total_anomalies = 0
    try:
        from src.ml.anomaly import run_composite_anomaly
        df_corp = service._load_corp_actions(sym)
        df_anomaly_res, _, _ = run_composite_anomaly(df_ohlcv, df_corp_actions=df_corp)
        if not df_anomaly_res.empty and "is_anomaly" in df_anomaly_res.columns:
            total_anomalies = int(df_anomaly_res["is_anomaly"].sum())
    except Exception as e:
        log.warning("Could not calculate total anomalies: %s", e)

    matched_dates = {f.anomaly_date for f in findings}
    unknown_anomalies = max(0, total_anomalies - len(matched_dates))

    driver_stats = {
        "Company News & Filings": {"count": 0, "total_score": 0.0},
        "USDINR Volatility": {"count": 0, "total_score": 0.0},
        "Global Rate Decisions": {"count": 0, "total_score": 0.0},
        "Geopolitical Shocks": {"count": 0, "total_score": 0.0}
    }

    for f in findings:
        et = f.event.event_type
        lbl = f.event.label.upper()
        score = f.correlation_score
        if et in (EventType.COMPANY_FILING, EventType.NEWS_ANNOUNCEMENT):
            driver_stats["Company News & Filings"]["count"] += 1
            driver_stats["Company News & Filings"]["total_score"] += score
        elif et == EventType.MACRO_RATE_DECISION:
            driver_stats["Global Rate Decisions"]["count"] += 1
            driver_stats["Global Rate Decisions"]["total_score"] += score
        elif et == EventType.MACRO_GEOPOLITICAL:
            driver_stats["Geopolitical Shocks"]["count"] += 1
            driver_stats["Geopolitical Shocks"]["total_score"] += score
        elif et == EventType.MACRO_COMMODITY_SHOCK or "USDINR" in lbl:
            driver_stats["USDINR Volatility"]["count"] += 1
            driver_stats["USDINR Volatility"]["total_score"] += score

    # Format the Root Cause Attribution & Strength table
    attribution_table_lines = [
        "| Driver | Events | Avg Score |",
        "|---|---|---|",
    ]
    for driver, stats_dict in driver_stats.items():
        cnt = stats_dict["count"]
        tot = stats_dict["total_score"]
        avg_str = f"{tot / cnt:.1f}" if cnt > 0 else "0.0"
        attribution_table_lines.append(f"| {driver} | {cnt} | {avg_str} |")
    attribution_table_lines.append(f"| Unknown / Unattributed | {unknown_anomalies} | N/A |")
    attribution_table_str = "\n".join(attribution_table_lines)

    # Most Influential Drivers
    category_scores = {
        "Corporate actions & news": driver_stats["Company News & Filings"]["total_score"],
        "USDINR volatility": driver_stats["USDINR Volatility"]["total_score"],
        "Global rate decisions": driver_stats["Global Rate Decisions"]["total_score"],
        "Geopolitical shocks": driver_stats["Geopolitical Shocks"]["total_score"],
    }
    sorted_drivers = sorted(
        [(k, v) for k, v in category_scores.items() if v > 0], key=lambda x: -x[1]
    )

    # Attribution summary details
    total_findings_count = len(findings)
    company_pct = (driver_stats["Company News & Filings"]["count"] / total_findings_count * 100.0) if total_findings_count > 0 else 0.0
    fx_pct = (driver_stats["USDINR Volatility"]["count"] / total_findings_count * 100.0) if total_findings_count > 0 else 0.0
    rate_pct = (driver_stats["Global Rate Decisions"]["count"] / total_findings_count * 100.0) if total_findings_count > 0 else 0.0
    geo_pct = (driver_stats["Geopolitical Shocks"]["count"] / total_findings_count * 100.0) if total_findings_count > 0 else 0.0

    largest_pos_shock_date = "N/A"
    largest_pos_shock_val = 0.0
    largest_neg_shock_date = "N/A"
    largest_neg_shock_val = 0.0

    findings_returns = []
    for f in findings:
        r_val, _ = date_to_stats.get(f.anomaly_date, (0.0, 0.0))
        findings_returns.append((f.anomaly_date, r_val))

    if findings_returns:
        pos_shocks = [x for x in findings_returns if x[1] > 0]
        if pos_shocks:
            best_pos = max(pos_shocks, key=lambda x: x[1])
            largest_pos_shock_date = str(best_pos[0])
            largest_pos_shock_val = best_pos[1] * 100.0

        neg_shocks = [x for x in findings_returns if x[1] < 0]
        if neg_shocks:
            worst_neg = min(neg_shocks, key=lambda x: x[1])
            largest_neg_shock_date = str(worst_neg[0])
            largest_neg_shock_val = worst_neg[1] * 100.0

    highest_conf_finding = max(findings, key=lambda x: x.correlation_score) if findings else None
    highest_confidence_str = "N/A"
    if highest_conf_finding:
        highest_confidence_str = f"**{highest_conf_finding.anomaly_date}** ({highest_conf_finding.event.label})"

    most_influential_factor = sorted_drivers[0][0] if sorted_drivers else "N/A"

    attribution_summary_block = (
        "\n### 📊 Attribution Summary\n\n"
        f"• **Company News:** `{driver_stats["Company News & Filings"]["count"]}` events (`{company_pct:.0f}%`)\n"
        f"• **FX Shocks:** `{driver_stats["USDINR Volatility"]["count"]}` events (`{fx_pct:.0f}%`)\n"
        f"• **Macro Decisions:** `{driver_stats["Global Rate Decisions"]["count"]}` events (`{rate_pct:.0f}%`)\n"
        f"• **Geopolitical:** `{driver_stats["Geopolitical Shocks"]["count"]}` events (`{geo_pct:.0f}%`)\n\n"
        f"• **Most Influential Factor:** **{most_influential_factor}**\n"
        f"• **FX Correlation:** `r = {correlation:.2f}`\n"
        f"• **FX Beta:** `β = {beta_fx:.2f}`\n"
        f"• **Highest Confidence Attribution:** {highest_confidence_str}\n"
        f"• **Largest Price Shock:** `{largest_pos_shock_date}` (`{largest_pos_shock_val:+.2f}%`)\n"
        f"• **Largest Negative Shock:** `{largest_neg_shock_date}` (`{largest_neg_shock_val:+.2f}%`)\n"
    )

    # If there are no findings, return early with FX validation block
    if not findings:
        msg = (
            f"ℹ️ Correlation analysis complete for **{sym}**.\n"
            f"No significant pre-event or macro-event correlations were mapped for "
            f"the {len(df_ohlcv)} trading days analyzed in this lookback period."
        )
        if validation_block:
            msg += "\n" + validation_block
        return msg

    # 4. Generate Visualizations
    out_dir = Path("output") / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    timeline_base64 = render_correlation_timeline_png(sym, findings, df_ohlcv)
    grid_base64 = render_lead_lag_grid_png(sym, findings)

    saved_charts_msg = ""
    if timeline_base64:
        timeline_path = out_dir / f"{sym}_correlation_timeline.png"
        with open(timeline_path, "wb") as fh:
            fh.write(base64.b64decode(timeline_base64))
        saved_charts_msg += f"  • Timeline Overlay: **{timeline_path.resolve()}**\n"

    if grid_base64:
        grid_path = out_dir / f"{sym}_lead_lag_grid.png"
        with open(grid_path, "wb") as fh:
            fh.write(base64.b64decode(grid_base64))
        saved_charts_msg += f"  • Lead-Lag Feature Grid: **{grid_path.resolve()}**\n"

    # 5. Format Text Summary Report
    drivers_str = ""
    if sorted_drivers:
        drivers_list = []
        for idx, (cat, score) in enumerate(sorted_drivers, 1):
            drivers_list.append(f"{idx}. **{cat}** (Cumulative Score: {score:.1f})")
        drivers_str = "\n".join(drivers_list)
    else:
        drivers_str = "No drivers identified."

    lines = [
        f"📊 **Event Correlation Report: {sym}** (Lookback: {lookback_days} days)",
        f"Found **{len(findings)}** significant anomaly-event correlation(s).",
        "",
        "### 🎯 Root Cause Attribution & Strength",
        attribution_table_str,
        "",
        "### 🔝 Most Influential Drivers",
        drivers_str,
        "",
        "### 📅 Mapped Anomalies Timeline",
        "| Anomaly Date | Observed Return | Abnormal Return | Percentile | Offset | Event Trigger | Strategy | Score | Confidence |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    for f in findings:
        ret_val, pct_val = date_to_stats.get(f.anomaly_date, (0.0, 0.0))
        ret_str = f"{ret_val*100:+.2f}%"
        pct_str = f"{pct_val:.1f}%"
        abn_val = f.abnormal_return if f.abnormal_return is not None else ret_val
        abn_str = f"{abn_val*100:+.2f}%"
        offset_str = f"{f.lead_lag_days:+}d"
        event_lbl = f.event.label
        if len(event_lbl) > 25:
            event_lbl = event_lbl[:22] + "..."
        lines.append(
            f"| {f.anomaly_date} | {ret_str} | {abn_str} | {pct_str} | {offset_str} | {event_lbl} | {f.strategy_name} | {f.correlation_score:.1f} | **{f.confidence}** |"
        )

    lines.append("")
    lines.append("### Detailed Explanations")
    for idx, f in enumerate(findings, 1):
        ret_val, pct_val = date_to_stats.get(f.anomaly_date, (0.0, 0.0))
        ret_str = f"{ret_val*100:+.2f}%"
        pct_str = f"{pct_val:.1f}%"
        abn_val = f.abnormal_return if f.abnormal_return is not None else ret_val
        abn_str = f"{abn_val*100:+.2f}%"
        lines.append(
            f"{idx}. **{f.anomaly_date} ({f.confidence})**: {f.explanation}\n"
            f"   *(Observed Return: {ret_str}, Abnormal Return: {abn_str}, Percentile: {pct_str})*"
        )
        lines.append(f"   *Event Detail:* {f.event.description} (Type: `{f.event.event_type.value}`)")

    if validation_block:
        lines.append(validation_block)

    if attribution_summary_block:
        lines.append(attribution_summary_block)

    if saved_charts_msg:
        lines.append("")
        lines.append("### 📈 Saved Visualizations")
        lines.append(saved_charts_msg)

    return "\n".join(lines)
