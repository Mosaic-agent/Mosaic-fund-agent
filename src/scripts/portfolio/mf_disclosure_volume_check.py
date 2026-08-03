"""
src/scripts/portfolio/mf_disclosure_volume_check.py
──────────────────────────────────────────────────────────────────
Cross-check disclosed MF holdings additions (from mf_holdings' monthly MoM
shifts) against the stock's own trading volume during the disclosure month.

MF holdings disclosures are a monthly snapshot with a real-world reporting
lag — they tell you a fund's position *changed*, not exactly when it traded.
An independent volume anomaly during the same month (elevated average daily
volume vs. a trailing baseline) corroborates that the disclosed buying shows
up in the market's own footprint, rather than trusting the disclosure figure
in isolation.

Resolves each addition's security_name/isin to an NSE symbol via
src/tools/security_symbol_resolver.py (Shoonya searchscrip, cached) so it
can pull market_data.daily_prices' volume column for that stock. Additions
that don't resolve to a symbol (acronym-ticker names like BHEL, delisted/
merged names like Cairn India) are reported as NO_PRICE_DATA rather than
silently dropped or guessed at.

Usage:
    python src/scripts/portfolio/mf_disclosure_volume_check.py --category small --amc all
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

from src.db.pool import get_pool
from src.scripts.portfolio.smallcap_pattern_analyzer import MFCapPatternAnalyzer
from src.tools.security_symbol_resolver import get_or_resolve_symbols, is_resolvable_equity

logger = logging.getLogger(__name__)

ELEVATED_VOLUME_RATIO = 1.5   # disclosure-month avg volume >= 1.5x trailing baseline -> corroborated
WEAK_VOLUME_RATIO = 1.0       # between 1.0x and ELEVATED_VOLUME_RATIO -> weak


def _latest_disclosure_month(analyzer: MFCapPatternAnalyzer, amc: str) -> pd.Timestamp | None:
    amc_filter, amc_params = analyzer._get_amc_sql_filter(amc)
    df = analyzer.pool.query_df(
        f"SELECT max(as_of_month) as m FROM market_data.mf_holdings FINAL WHERE {amc_filter}",
        parameters=amc_params,
    )
    if df.empty or pd.isna(df.iloc[0]["m"]):
        return None
    return pd.to_datetime(df.iloc[0]["m"])


def _fetch_price_window(pool, symbol: str, baseline_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.DataFrame:
    return pool.query_df(f"""
        SELECT trade_date, volume FROM market_data.daily_prices FINAL
        WHERE symbol = '{symbol}'
          AND trade_date >= '{baseline_start.strftime('%Y-%m-%d')}'
          AND trade_date <= '{month_end.strftime('%Y-%m-%d')}'
        ORDER BY trade_date
    """)


def _volume_ratio(pool, symbol: str, disclosure_month: pd.Timestamp, baseline_months: int, auto_import: bool = True):
    """Return (disclosure_month_avg_volume, baseline_avg_volume, ratio) for a symbol,
    or (None, None, None) if there's no daily_prices coverage for it — even after an
    on-demand backfill attempt, since most genuine smallcaps aren't pre-imported
    (this codebase imports stock history on demand, not the full NSE universe).
    """
    month_start = disclosure_month.replace(day=1)
    month_end = (month_start + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
    baseline_start = month_start - pd.DateOffset(months=baseline_months)

    df = _fetch_price_window(pool, symbol, baseline_start, month_end)
    if df.empty and auto_import:
        try:
            from src.tools.skills_tools import import_symbol_data_impl
            days_needed = min((pd.Timestamp.today() - baseline_start).days + 5, 1825)
            import_symbol_data_impl(symbol, days=max(days_needed, 30), data_source="shoonya")
        except Exception as exc:
            logger.debug("auto-import failed for %s: %s", symbol, exc)
        df = _fetch_price_window(pool, symbol, baseline_start, month_end)

    if df.empty:
        return None, None, None

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    disclosure_rows = df[(df["trade_date"] >= month_start) & (df["trade_date"] <= month_end)]
    baseline_rows = df[df["trade_date"] < month_start]
    if disclosure_rows.empty or baseline_rows.empty:
        return None, None, None

    disclosure_avg = float(disclosure_rows["volume"].mean())
    baseline_avg = float(baseline_rows["volume"].mean())
    if baseline_avg <= 0:
        return disclosure_avg, baseline_avg, None
    return disclosure_avg, baseline_avg, disclosure_avg / baseline_avg


def check_disclosure_volume_corroboration(
    category: str = "small",
    amc: str = "all",
    baseline_months: int = 3,
    top: int = 20,
    api=None,
    auto_import: bool = True,
) -> pd.DataFrame:
    """For each disclosed MF holdings addition/accumulation this month, check
    whether the stock's own trading volume was elevated during that month
    relative to a trailing baseline — an independent corroboration signal
    for the disclosure-based \"purchase\" inference.
    """
    analyzer = MFCapPatternAnalyzer(category=category)
    additions, _trims = analyzer.fetch_mom_shifts(amc=amc)
    if additions.empty:
        return pd.DataFrame()

    disclosure_month = _latest_disclosure_month(analyzer, amc)
    if disclosure_month is None:
        return pd.DataFrame()

    additions = additions[additions["security_name"].apply(is_resolvable_equity)].copy()
    if additions.empty:
        return pd.DataFrame()

    if api is None:
        from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
        api = get_shoonya_api()
    if api is None:
        raise RuntimeError("Could not obtain an authenticated Shoonya session for symbol resolution.")

    isin_name_pairs = list(zip(additions["isin"], additions["security_name"]))
    symbol_map = get_or_resolve_symbols(api, isin_name_pairs)

    rows = []
    for _, r in additions.iterrows():
        symbol = symbol_map.get(r["isin"])
        disclosure_vol = baseline_vol = ratio = None
        if symbol:
            disclosure_vol, baseline_vol, ratio = _volume_ratio(
                analyzer.pool, symbol, disclosure_month, baseline_months, auto_import=auto_import
            )

        if symbol is None or disclosure_vol is None:
            verdict = "NO_PRICE_DATA"
        elif ratio is None:
            verdict = "NO_PRICE_DATA"
        elif ratio >= ELEVATED_VOLUME_RATIO:
            verdict = "CORROBORATED"
        elif ratio >= WEAK_VOLUME_RATIO:
            verdict = "WEAK"
        else:
            verdict = "NOT_CORROBORATED"

        rows.append({
            "security_name": r["security_name"],
            "isin": r["isin"],
            "symbol": symbol,
            "status": r["status"],
            "mv_change_cr": r["mv_change_cr"],
            "disclosure_month": disclosure_month.strftime("%Y-%m-%d"),
            "baseline_avg_volume": round(baseline_vol, 0) if baseline_vol is not None else None,
            "disclosure_avg_volume": round(disclosure_vol, 0) if disclosure_vol is not None else None,
            "volume_ratio": round(ratio, 2) if ratio is not None else None,
            "verdict": verdict,
        })

    result = pd.DataFrame(rows)
    return result.sort_values("mv_change_cr", ascending=False).head(top).reset_index(drop=True)


def render_report(df: pd.DataFrame) -> str:
    if df.empty:
        return "No additions found to check."
    lines = ["=" * 100]
    lines.append(f"  MF DISCLOSURE vs VOLUME CORROBORATION — {df.iloc[0]['disclosure_month']}")
    lines.append("=" * 100)
    for _, r in df.iterrows():
        sym = r["symbol"] or "?"
        ratio_str = f"{r['volume_ratio']:.2f}x" if r["volume_ratio"] is not None else "n/a"
        lines.append(
            f"{r['security_name'][:32]:32s} | {sym:12s} | +₹{r['mv_change_cr']:7.1f} Cr | "
            f"vol ratio {ratio_str:>7s} | {r['verdict']}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Cross-check MF disclosure additions against trading volume")
    parser.add_argument("--category", "-c", type=str, default="small", choices=["small", "mid", "large"])
    parser.add_argument("--amc", "-a", type=str, default="all")
    parser.add_argument("--baseline-months", type=int, default=3)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--no-auto-import", action="store_true",
                        help="Don't backfill missing daily_prices history for resolved symbols.")
    args = parser.parse_args()
    report_df = check_disclosure_volume_corroboration(
        category=args.category, amc=args.amc, baseline_months=args.baseline_months, top=args.top,
        auto_import=not args.no_auto_import,
    )
    print(render_report(report_df))
