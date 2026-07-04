"""
src/tools/intl_etf_tools.py
────────────────────────────
LangChain tools that surface the International ETF Pattern Analysis
(originally built for the Streamlit UI) inside the Mosaic agent chat.

Symbols covered: MAFANG · HNGSNGBEES · MON100 · MASPTOP50 · MAHKTECH · MONQ50
7 analytical lenses: Performance · Premium · Regimes · Correlation ·
                     Seasonality · LightGBM predictability · Drawdowns
"""

from __future__ import annotations

import logging

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

INTL_ETFS = ["MAFANG", "HNGSNGBEES", "MON100", "MASPTOP50", "MAHKTECH", "MONQ50"]

INTL_ETF_LABELS = {
    "MAFANG":    "US Tech (NYSE FANG+)",
    "HNGSNGBEES":"Hang Seng",
    "MON100":    "Nasdaq 100",
    "MASPTOP50": "S&P 500",
    "MAHKTECH":  "HK Tech",
    "MONQ50":    "Nasdaq 50",
}


def _load() -> "tuple":
    """Load price and NAV data using the existing analysis module."""
    from src.ui.intl_etf_analysis import load_data
    from src.db.pool import get_pool
    return load_data(get_pool())


@tool
def get_intl_etf_performance() -> str:
    """
    Show 3-year performance metrics for all 6 international ETFs listed on NSE:
    MAFANG (US Tech, NYSE FANG+), HNGSNGBEES (Hang Seng), MON100 (Nasdaq 100),
    MASPTOP50 (S&P 500), MAHKTECH (HK Tech), MONQ50 (Nasdaq 50).

    Returns a table: Total Return %, Annualised Volatility %, Sharpe Ratio.
    Use when asked: "how have international ETFs performed", "best intl ETF",
    "compare MAFANG vs MON100 returns".
    """
    try:
        from src.ui.intl_etf_analysis import compute_performance
        price_wide, _ = _load()
        perf = compute_performance(price_wide)
        perf.index = [f"{s} ({INTL_ETF_LABELS.get(s, s)})" for s in perf.index]
        return perf.round(2).to_markdown()
    except Exception as exc:
        return f"Error loading performance data: {exc}"


@tool
def get_intl_etf_premium(symbol: str = "") -> str:
    """
    Show scarcity premium / discount statistics for international ETFs.

    RBI's overseas investment cap creates persistent premiums — buying when
    premium is negative (discount) has historically been profitable.

    iNAV freshness: during market hours (IST 09:15–15:30) the latest iNAV is
    fetched live from NSE if the DB snapshot is older than 10 minutes.
    Outside market hours the last stored snapshot is used.

    Args:
        symbol: specific ETF symbol (e.g. 'MAFANG') — blank = all ETFs

    Returns: mean premium %, std, current premium, trend, anomaly count.
    """
    try:
        from src.ui.intl_etf_analysis import compute_premium_stats
        price_wide, nav_wide = _load()
        prem_stats, prem_wide, anomaly_dates = compute_premium_stats(price_wide, nav_wide)

        if symbol.upper() in INTL_ETFS:
            sym = symbol.upper()
            row = prem_stats.loc[sym] if sym in prem_stats.index else None
            if row is None:
                return f"No premium data for {sym}."
            # Latest premium
            latest = prem_wide[sym].dropna().iloc[-1] if sym in prem_wide.columns else None
            anom_count = len(anomaly_dates.get(sym, []))
            return (
                f"### {sym} ({INTL_ETF_LABELS.get(sym, sym)}) — Scarcity Premium\n\n"
                f"| Metric | Value |\n|---|---|\n"
                f"| Mean premium | {row.get('mean_pct', row.iloc[0]):.2f}% |\n"
                f"| Std | {row.get('std_pct', row.iloc[1]):.2f}% |\n"
                f"| Latest premium | {latest:.2f}% |\n"
                f"| Trend | {row.get('trend_pct_mo', row.iloc[2] if len(row) > 2 else 'N/A'):.3f}%/mo |\n"
                f"| Anomaly days (Isolation Forest) | {anom_count} |\n"
            )

        prem_stats.index = [f"{s} ({INTL_ETF_LABELS.get(s, s)})" for s in prem_stats.index]
        return prem_stats.round(3).to_markdown()
    except Exception as exc:
        return f"Error loading premium data: {exc}"


@tool
def get_intl_etf_regimes() -> str:
    """
    Show the current market regime (Bull / Sideways / Bear) for each
    international ETF, detected via KMeans clustering on 30-day rolling
    return, volatility, momentum, and scarcity premium.

    Use when asked: "what regime is MAFANG in", "intl ETF regimes",
    "is the Nasdaq ETF bullish".
    """
    try:
        from src.ui.intl_etf_analysis import compute_regimes, _premium_series
        price_wide, nav_wide = _load()
        prem = _premium_series(price_wide, nav_wide)
        regime_df, regime_series = compute_regimes(price_wide, prem)

        lines = ["### International ETF Market Regimes (current)\n"]
        lines.append("| ETF | Label | Current Regime | Days in Regime |")
        lines.append("|---|---|---|---|")
        for sym in INTL_ETFS:
            if sym not in regime_series:
                continue
            series = regime_series[sym]
            current = series.iloc[-1] if len(series) else "N/A"
            # Count consecutive days in current regime
            run = 0
            for v in reversed(series.tolist()):
                if v == current:
                    run += 1
                else:
                    break
            label = INTL_ETF_LABELS.get(sym, sym)
            emoji = {"Bull": "🟢", "Bear": "🔴", "Sideways": "🟡"}.get(current, "⚪")
            lines.append(f"| {sym} | {label} | {emoji} {current} | {run} days |")
        return "\n".join(lines)
    except Exception as exc:
        return f"Error loading regime data: {exc}"


@tool
def get_intl_etf_seasonality() -> str:
    """
    Show monthly return seasonality for international ETFs — which months
    historically give the best and worst returns.

    Returns a heatmap table: ETFs × months with median monthly return %.
    Use when asked: "best month to buy MAFANG", "intl ETF seasonality",
    "when do international ETFs rally".
    """
    try:
        from src.ui.intl_etf_analysis import compute_seasonality
        price_wide, _ = _load()
        season_med, _ = compute_seasonality(price_wide)

        month_names = ["Jan","Feb","Mar","Apr","May","Jun",
                       "Jul","Aug","Sep","Oct","Nov","Dec"]
        season_med.columns = month_names[:len(season_med.columns)]
        season_med.index = [f"{s} ({INTL_ETF_LABELS.get(s,s)})" for s in season_med.index]
        result = "### International ETF Monthly Seasonality (median return %)\n\n"
        result += season_med.round(2).to_markdown()
        result += "\n\n*Positive = historically bullish month, Negative = bearish*"
        return result
    except Exception as exc:
        return f"Error loading seasonality data: {exc}"


@tool
def get_intl_etf_correlation() -> str:
    """
    Show return correlations between international ETFs and USDINR.

    High correlation with USDINR = INR depreciation amplifies returns.
    Use when asked: "are MON100 and MASPTOP50 correlated", "USDINR correlation",
    "which intl ETF is most correlated with rupee".
    """
    try:
        from src.ui.intl_etf_analysis import compute_correlation
        price_wide, _ = _load()
        corr_df, usdinr_corr = compute_correlation(price_wide)

        corr_df.index   = [f"{s} ({INTL_ETF_LABELS.get(s, s)})" for s in corr_df.index]
        corr_df.columns = [INTL_ETF_LABELS.get(c, c) for c in corr_df.columns]

        result = "### ETF Return Correlations (3-year)\n\n"
        result += corr_df.round(2).to_markdown()

        if usdinr_corr is not None and not usdinr_corr.empty:
            usdinr_corr.index = [f"{s} ({INTL_ETF_LABELS.get(s, s)})" for s in usdinr_corr.index]
            result += "\n\n### USDINR Correlation\n\n" + usdinr_corr.round(2).to_markdown()
        return result
    except Exception as exc:
        return f"Error loading correlation data: {exc}"


@tool
def get_intl_etf_drawdowns() -> str:
    """
    Show major drawdown episodes (> 10% from peak) for international ETFs.
    Includes: symbol, start date, trough, max drawdown %, recovery status.

    Use when asked: "drawdowns for MAFANG", "worst crashes in Hang Seng ETF",
    "has MON100 recovered".
    """
    try:
        from src.ui.intl_etf_analysis import compute_drawdowns
        price_wide, _ = _load()
        dd_df = compute_drawdowns(price_wide)
        if dd_df.empty:
            return "No major drawdowns (> 10%) found in the 3-year window."
        dd_df["label"] = dd_df["symbol"].map(lambda s: INTL_ETF_LABELS.get(s, s))
        cols = [c for c in ["symbol", "label", "peak_date", "trough_date",
                             "max_dd_pct", "recovered", "recovery_date"] if c in dd_df.columns]
        return dd_df[cols].sort_values("max_dd_pct").round(2).to_markdown(index=False)
    except Exception as exc:
        return f"Error loading drawdown data: {exc}"


@tool
def get_intl_etf_lgbm() -> str:
    """
    Show LightGBM feature importance for predicting 5-day return direction
    of each international ETF.

    Top features reveal what drives each ETF:
    - vol_30d    — volatility regime effect
    - premium    — scarcity premium / mean-reversion signal
    - momentum   — price trend continuation
    - usdinr_1d  — rupee move impact

    Use when asked: "what predicts MAFANG returns", "LightGBM feature importance",
    "ML analysis international ETF".
    """
    try:
        from src.ui.intl_etf_analysis import compute_lgbm, _premium_series
        price_wide, nav_wide = _load()
        prem = _premium_series(price_wide, nav_wide)
        lgbm_df, imp_data = compute_lgbm(price_wide, prem)

        lines = ["### LightGBM Feature Importance — 5-day Return Direction\n"]
        for sym, imp in imp_data.items():
            label = INTL_ETF_LABELS.get(sym, sym)
            lines.append(f"\n**{sym} ({label})**")
            top = sorted(imp.items(), key=lambda x: -x[1])[:5]
            lines.append("| Feature | Importance |")
            lines.append("|---|---|")
            for feat, val in top:
                bar = "█" * max(1, int(val / max(v for _, v in top) * 10))
                lines.append(f"| {feat} | {bar} {val:.4f} |")
        return "\n".join(lines)
    except Exception as exc:
        return f"Error loading LightGBM data: {exc}"


INTL_ETF_TOOLS = [
    get_intl_etf_performance,
    get_intl_etf_premium,
    get_intl_etf_regimes,
    get_intl_etf_seasonality,
    get_intl_etf_correlation,
    get_intl_etf_drawdowns,
    get_intl_etf_lgbm,
]
