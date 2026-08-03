"""
src/scripts/portfolio/smallcap_pattern_analyzer.py
────────────────────────────────────────────────────
Generic Multi-AMC Market-Cap-Segment Pattern & Institutional Shift Analyzer.

Supports Small, Mid, and Large Cap segments (via --category) across all major
AMCs (Nippon, DSP, HDFC, ICICI, Quant, Kotak, Bajaj, etc.), or aggregate
multi-AMC consensus analysis across all active funds in the segment.

Usage:
    python src/scripts/portfolio/smallcap_pattern_analyzer.py --category small --amc all
    python src/scripts/portfolio/smallcap_pattern_analyzer.py --category mid --amc dsp
    python src/scripts/portfolio/smallcap_pattern_analyzer.py --category large --amc nippon
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional  # noqa: F401 (List used in _score_cross_conviction_persistence)
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

# Ensure project root is on sys.path
_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

from src.db.pool import get_pool


AMC_ALIAS_MAP = {
    "dsp": "lower(fund_name) LIKE 'dsp%'",
    "nippon": "(lower(fund_name) LIKE 'nippon%' OR lower(fund_name) LIKE 'reliance%')",
    "hdfc": "lower(fund_name) LIKE 'hdfc%'",
    "icici": "lower(fund_name) LIKE 'icici%'",
    "quant": "lower(fund_name) LIKE 'quant%'",
    "kotak": "lower(fund_name) LIKE 'kotak%'",
    "bajaj": "lower(fund_name) LIKE 'bajaj%'",
    "sbi": "lower(fund_name) LIKE 'sbi%'",
    "axis": "lower(fund_name) LIKE 'axis%'",
}

# Per-category fund_name / AMFI-category membership rules and the ETF symbols
# used for price trend + composite quant signal. "mid"/"large" explicitly
# exclude "Large & Mid Cap Fund" hybrids so they don't bleed into either pure
# segment (verified against live mf_holdings fund_name values).
CATEGORY_CONFIG = {
    "small": {
        "label": "Small Cap",
        "membership_sql": "(lower(fund_name) LIKE '%small%' AND lower(fund_name) NOT LIKE '%index%' AND lower(fund_name) NOT LIKE '%etf%' AND lower(fund_name) NOT LIKE '%quality%')",
        "amfi_filter_sql": "(lower(category_name) LIKE '%small%' OR lower(subcategory_group) LIKE '%small%')",
        "price_symbol": "SMALLCAP",
        "signal_etf": "SMALL250",
    },
    "mid": {
        "label": "Mid Cap",
        "membership_sql": "(lower(fund_name) LIKE '%mid%cap%' AND lower(fund_name) NOT LIKE '%large%' AND lower(fund_name) NOT LIKE '%small%' AND lower(fund_name) NOT LIKE '%index%' AND lower(fund_name) NOT LIKE '%etf%' AND lower(fund_name) NOT LIKE '%quality%')",
        "amfi_filter_sql": "(lower(category_name) = 'mid cap fund')",
        "price_symbol": "MID150CASE",
        "signal_etf": "MID150BEES",
    },
    "large": {
        "label": "Large Cap",
        "membership_sql": "((lower(fund_name) LIKE '%large%cap%' OR lower(fund_name) LIKE '%bluechip%') AND lower(fund_name) NOT LIKE '%mid%' AND lower(fund_name) NOT LIKE '%index%' AND lower(fund_name) NOT LIKE '%etf%')",
        "amfi_filter_sql": "(lower(category_name) = 'large cap fund')",
        "price_symbol": "TOP100CASE",
        "signal_etf": "NIFTYBEES",
    },
}


@dataclass
class CapPriceMetrics:
    symbol: str
    latest_date: str
    latest_close: float
    ret_1w: float
    ret_1m: float
    ret_3m: float
    ret_6m: float
    ret_1y: float
    sma_50: float
    sma_200: float
    high_52w: float
    low_52w: float
    drawdown_52w: float
    eod_series: pd.DataFrame = field(repr=False)


@dataclass
class CapPatternReport:
    amc: str
    category_label: str
    price_metrics: CapPriceMetrics
    amfi_flows: pd.DataFrame
    top_holdings: pd.DataFrame
    mom_additions: pd.DataFrame
    mom_trims: pd.DataFrame
    cross_conviction: pd.DataFrame
    quant_signal: Dict[str, Any]


# Backward-compat aliases for the pre-existing Small-Cap-only names.
SmallcapPriceMetrics = CapPriceMetrics
SmallcapPatternReport = CapPatternReport


class MFCapPatternAnalyzer:
    """Generic multi-AMC market-cap-segment quantitative analyzer (small/mid/large cap)."""

    def __init__(self, category: str = "small"):
        self.pool = get_pool()
        self.category = category.lower().strip()
        if self.category not in CATEGORY_CONFIG:
            raise ValueError(f"Unknown cap category '{category}' — expected one of {sorted(CATEGORY_CONFIG)}")
        self.cfg = CATEGORY_CONFIG[self.category]

    def _get_amc_sql_filter(self, amc: str) -> tuple[str, dict]:
        """Return (sql_fragment, bind_params). Unmapped AMC names are bound as a
        query parameter rather than spliced into the SQL string."""
        amc_lower = amc.lower().strip()
        membership = self.cfg["membership_sql"]
        if amc_lower == "all" or amc_lower == "all_amcs":
            return (membership, {})
        if amc_lower in AMC_ALIAS_MAP:
            return (f"({AMC_ALIAS_MAP[amc_lower]} AND {membership})", {})
        return (
            f"(lower(fund_name) LIKE {{amc_pattern:String}} AND {membership})",
            {"amc_pattern": f"%{amc_lower}%"},
        )

    def fetch_price_metrics(self, symbol: str = "SMALLCAP") -> CapPriceMetrics:
        """Fetch EOD prices and compute technical return & SMA metrics."""
        query = f"""
            SELECT trade_date, open, high, low, close, volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{symbol}'
            ORDER BY trade_date ASC
        """
        df = self.pool.query_df(query)
        if df.empty:
            raise ValueError(f"No price data found for symbol '{symbol}'")

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date').reset_index(drop=True)

        latest_close = float(df['close'].iloc[-1])
        latest_date = df['trade_date'].iloc[-1].strftime('%Y-%m-%d')

        p_1w = float(df['close'].iloc[-5]) if len(df) >= 5 else latest_close
        p_1m = float(df['close'].iloc[-21]) if len(df) >= 21 else latest_close
        p_3m = float(df['close'].iloc[-63]) if len(df) >= 63 else latest_close
        p_6m = float(df['close'].iloc[-126]) if len(df) >= 126 else latest_close
        p_1y = float(df['close'].iloc[-252]) if len(df) >= 252 else latest_close

        ret_1w = round((latest_close / p_1w - 1) * 100, 2)
        ret_1m = round((latest_close / p_1m - 1) * 100, 2)
        ret_3m = round((latest_close / p_3m - 1) * 100, 2)
        ret_6m = round((latest_close / p_6m - 1) * 100, 2)
        ret_1y = round((latest_close / p_1y - 1) * 100, 2)

        sma_50 = round(float(df['close'].tail(50).mean()), 2)
        sma_200 = round(float(df['close'].tail(200).mean()), 2)
        high_52w = round(float(df['close'].tail(252).max()), 2)
        low_52w = round(float(df['close'].tail(252).min()), 2)
        drawdown_52w = round((latest_close / high_52w - 1) * 100, 2)

        return CapPriceMetrics(
            symbol=symbol,
            latest_date=latest_date,
            latest_close=latest_close,
            ret_1w=ret_1w,
            ret_1m=ret_1m,
            ret_3m=ret_3m,
            ret_6m=ret_6m,
            ret_1y=ret_1y,
            sma_50=sma_50,
            sma_200=sma_200,
            high_52w=high_52w,
            low_52w=low_52w,
            drawdown_52w=drawdown_52w,
            eod_series=df,
        )

    def fetch_amfi_flows(self, limit: int = 6) -> pd.DataFrame:
        """Fetch historical AMFI mutual fund category flows for this cap segment."""
        query = f"""
            SELECT report_month, category_name, subcategory_group, gross_purchase_cr, gross_redemption_cr, net_flow_cr, closing_aum_cr, flow_pct_of_aum
            FROM market_data.amfi_category_flows FINAL
            WHERE {self.cfg['amfi_filter_sql']}
            ORDER BY report_month DESC
            LIMIT {limit}
        """
        return self.pool.query_df(query)

    def fetch_top_holdings(self, amc: str = "all", limit: int = 15) -> pd.DataFrame:
        """Fetch Small Cap fund holdings across specified AMC or ALL AMCs."""
        amc_filter, amc_params = self._get_amc_sql_filter(amc)
        query = f"""
            SELECT 
                security_name, 
                isin, 
                count(DISTINCT fund_name) as fund_count,
                round(avg(pct_of_nav), 2) as avg_pct_nav,
                round(sum(market_value_cr), 1) as total_market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE {amc_filter}
              AND as_of_month = (
                  SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE {amc_filter}
              )
              AND isin NOT LIKE 'PH_%' AND isin != ''
            GROUP BY security_name, isin
            ORDER BY total_market_value_cr DESC
            LIMIT {limit}
        """
        return self.pool.query_df(query, parameters=amc_params)

    def fetch_mom_shifts(self, amc: str = "all") -> tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch MoM net additions and trims across target AMC or ALL AMCs."""
        amc_filter, amc_params = self._get_amc_sql_filter(amc)
        months_df = self.pool.query_df(f"""
            SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL
            WHERE {amc_filter}
            ORDER BY as_of_month DESC LIMIT 2
        """, parameters=amc_params)
        if len(months_df) < 2:
            return pd.DataFrame(), pd.DataFrame()

        m_curr = months_df.iloc[0, 0].strftime('%Y-%m-%d')
        m_prev = months_df.iloc[1, 0].strftime('%Y-%m-%d')

        add_query = f"""
            WITH c AS (
                SELECT security_name, isin, sum(market_value_cr) as mv_curr, avg(pct_of_nav) as nav_curr
                FROM market_data.mf_holdings FINAL
                WHERE {amc_filter} AND as_of_month = '{m_curr}'
                GROUP BY security_name, isin
            ),
            p AS (
                SELECT security_name, isin, sum(market_value_cr) as mv_prev, avg(pct_of_nav) as nav_prev
                FROM market_data.mf_holdings FINAL
                WHERE {amc_filter} AND as_of_month = '{m_prev}'
                GROUP BY security_name, isin
            )
            SELECT 
                c.security_name,
                c.isin,
                round(c.mv_curr, 1) as mv_curr_cr,
                round(c.mv_curr - coalesce(p.mv_prev, 0), 1) as mv_change_cr,
                round(c.nav_curr - coalesce(p.nav_prev, 0), 2) as nav_shift,
                CASE WHEN p.mv_prev IS NULL THEN 'NEW ADDITION' ELSE 'ACCUMULATED' END as status
            FROM c
            LEFT JOIN p ON c.isin = p.isin
            WHERE c.isin NOT LIKE 'PH_%' AND c.isin != ''
            ORDER BY mv_change_cr DESC
            LIMIT 15
        """
        df_add = self.pool.query_df(add_query, parameters=amc_params)

        trim_query = f"""
            WITH c AS (
                SELECT security_name, isin, sum(market_value_cr) as mv_curr, avg(pct_of_nav) as nav_curr
                FROM market_data.mf_holdings FINAL
                WHERE {amc_filter} AND as_of_month = '{m_curr}'
                GROUP BY security_name, isin
            ),
            p AS (
                SELECT security_name, isin, sum(market_value_cr) as mv_prev, avg(pct_of_nav) as nav_prev
                FROM market_data.mf_holdings FINAL
                WHERE {amc_filter} AND as_of_month = '{m_prev}'
                GROUP BY security_name, isin
            )
            SELECT 
                p.security_name,
                p.isin,
                round(coalesce(c.mv_curr, 0), 1) as mv_curr_cr,
                round(coalesce(c.mv_curr, 0) - p.mv_prev, 1) as mv_change_cr,
                round(coalesce(c.nav_curr, 0) - p.nav_prev, 2) as nav_shift,
                CASE WHEN c.mv_curr IS NULL THEN 'EXITED' ELSE 'TRIMMED' END as status
            FROM p
            LEFT JOIN c ON p.isin = c.isin
            WHERE coalesce(c.mv_curr, 0) < p.mv_prev AND p.isin NOT LIKE 'PH_%' AND p.isin != ''
            ORDER BY mv_change_cr ASC
            LIMIT 15
        """
        df_trim = self.pool.query_df(trim_query, parameters=amc_params)

        return df_add, df_trim

    _AMC_CASE_SQL = """multiIf(
                    lower(fund_name) LIKE 'dsp%', 'DSP',
                    lower(fund_name) LIKE 'nippon%' OR lower(fund_name) LIKE 'reliance%', 'Nippon',
                    lower(fund_name) LIKE 'icici%', 'ICICI',
                    lower(fund_name) LIKE 'hdfc%', 'HDFC',
                    lower(fund_name) LIKE 'sbi%', 'SBI',
                    lower(fund_name) LIKE 'axis%', 'Axis',
                    lower(fund_name) LIKE 'quant%', 'Quant',
                    lower(fund_name) LIKE 'kotak%', 'Kotak',
                    lower(fund_name) LIKE 'bajaj%', 'Bajaj',
                    splitByChar('_', fund_name)[1]
                )"""

    def fetch_cross_conviction(self, limit: int = 15, lookback_months: int = 24) -> pd.DataFrame:
        """Fetch multi-AMC cross-fund conviction for this cap segment, ranked by persistence.

        A name held by several AMCs continuously across `lookback_months` scores higher
        than one where several AMCs all bought in the same latest month — the latter
        looks identical on a single-month snapshot but is a much weaker conviction signal.
        """
        membership = self.cfg["membership_sql"]
        months_df = self.pool.query_df(f"""
            SELECT DISTINCT as_of_month
            FROM market_data.mf_holdings FINAL
            WHERE {membership}
            ORDER BY as_of_month DESC
            LIMIT {lookback_months}
        """)
        if months_df.empty:
            return pd.DataFrame()

        window_months = sorted(pd.to_datetime(months_df['as_of_month']).tolist())
        latest_month = window_months[-1].strftime('%Y-%m-%d')
        month_list_sql = ", ".join(f"'{m.strftime('%Y-%m-%d')}'" for m in window_months)

        history = self.pool.query_df(f"""
            SELECT
                as_of_month,
                any(security_name) as security_name,
                isin,
                count(DISTINCT {self._AMC_CASE_SQL}) as amc_count,
                count(DISTINCT fund_name) as total_fund_count,
                round(sum(market_value_cr), 1) as total_market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month IN ({month_list_sql})
              AND isin IN (
                  SELECT DISTINCT isin FROM market_data.mf_holdings FINAL
                  WHERE as_of_month = '{latest_month}'
                    AND {membership}
                    AND isin NOT LIKE 'PH_%' AND isin != ''
              )
              AND isin NOT LIKE 'PH_%' AND isin != ''
            GROUP BY as_of_month, isin
        """)
        if history.empty:
            return pd.DataFrame()

        return self._score_cross_conviction_persistence(history, window_months, limit)

    def _score_cross_conviction_persistence(
        self, history: pd.DataFrame, window_months: List, limit: int
    ) -> pd.DataFrame:
        """Collapse a monthly (isin, as_of_month) amc_count history into a persistence-ranked table."""
        history = history.copy()
        history['as_of_month'] = pd.to_datetime(history['as_of_month'])
        month_index = pd.DatetimeIndex(sorted(window_months))
        latest_month = month_index[-1]

        rows = []
        for isin, grp in history.groupby('isin'):
            by_month = grp.set_index('as_of_month').reindex(month_index)
            amc_series = by_month['amc_count'].fillna(0).astype(int)

            streak = 0
            for month in reversed(month_index):
                if amc_series.loc[month] >= 2:
                    streak += 1
                else:
                    break

            latest_row = grp[grp['as_of_month'] == latest_month]
            security_name = grp['security_name'].dropna().iloc[-1] if grp['security_name'].notna().any() else isin

            rows.append({
                'security_name': security_name,
                'isin': isin,
                'amc_count': int(latest_row['amc_count'].iloc[0]) if not latest_row.empty else 0,
                'total_fund_count': int(latest_row['total_fund_count'].iloc[0]) if not latest_row.empty else 0,
                'total_market_value_cr': float(latest_row['total_market_value_cr'].iloc[0]) if not latest_row.empty else 0.0,
                'avg_amc_count': round(float(amc_series.mean()), 2),
                'months_covered': int((amc_series > 0).sum()),
                'multi_amc_streak_months': streak,
                'window_months': len(month_index),
            })

        result = pd.DataFrame(rows)
        if result.empty:
            return result
        return result.sort_values(
            by=['multi_amc_streak_months', 'amc_count', 'total_market_value_cr'],
            ascending=[False, False, False],
        ).head(limit).reset_index(drop=True)

    def fetch_quant_signal(self, etf_symbol: str = "SMALL250") -> Dict[str, Any]:
        """Fetch composite quant signal record for the target ETF."""
        query = f"""
            SELECT *
            FROM market_data.signal_composite FINAL
            WHERE etf_symbol = '{etf_symbol}'
            ORDER BY as_of DESC
            LIMIT 1
        """
        df = self.pool.query_df(query)
        if df.empty:
            return {"etf_symbol": etf_symbol, "composite_score": None, "action": "N/A"}
        return df.to_dict(orient='records')[0]

    def analyze(self, amc: str = "all", lookback_months: int = 24) -> CapPatternReport:
        """Run complete modular analysis pipeline for targeted AMC or ALL AMCs."""
        pm = self.fetch_price_metrics(self.cfg["price_symbol"])
        amfi = self.fetch_amfi_flows()
        holdings = self.fetch_top_holdings(amc=amc)
        additions, trims = self.fetch_mom_shifts(amc=amc)
        cross = self.fetch_cross_conviction(lookback_months=lookback_months)
        signal = self.fetch_quant_signal(self.cfg["signal_etf"])

        return CapPatternReport(
            amc=amc.upper(),
            category_label=self.cfg["label"],
            price_metrics=pm,
            amfi_flows=amfi,
            top_holdings=holdings,
            mom_additions=additions,
            mom_trims=trims,
            cross_conviction=cross,
            quant_signal=signal,
        )

    def render_ascii_dashboard(self, report: CapPatternReport) -> str:
        """Render complete ASCII Visual Dashboard from report object."""
        pm = report.price_metrics
        df_price = pm.eod_series.tail(120).reset_index(drop=True)

        closes = df_price['close'].values
        dates = df_price['trade_date'].dt.strftime('%b %d').values
        min_c, max_c = min(closes), max(closes)
        height, width = 10, 54

        idx = np.linspace(0, len(closes) - 1, width, dtype=int)
        sampled_closes = closes[idx]
        sampled_dates = dates[idx]

        grid = [[' ' for _ in range(width)] for _ in range(height)]
        for col, val in enumerate(sampled_closes):
            row = int((max_c - val) / (max_c - min_c + 1e-6) * (height - 1))
            row = max(0, min(height - 1, row))
            grid[row][col] = '*'

        lines = []
        lines.append("===============================================================")
        lines.append(f"   {report.category_label.upper()} ETF PRICE TREND ({sampled_dates[0]} - {sampled_dates[-1]}) [Target AMC: {report.amc}]")
        lines.append("===============================================================")
        for r in range(height):
            val_at_r = max_c - (r / (height - 1)) * (max_c - min_c)
            line_str = ''.join(grid[r])
            lines.append(f"{val_at_r:5.2f} | {line_str}")
        lines.append("      +" + "-" * width)
        lines.append(f"        {sampled_dates[0]}                                       {sampled_dates[-1]}")
        lines.append("\n")

        # Top Holdings Chart
        lines.append("===============================================================")
        lines.append(f"    {report.category_label.upper()} TOP EQUITY HOLDINGS (AMC: {report.amc})")
        lines.append("===============================================================")
        if not report.top_holdings.empty:
            df_h = report.top_holdings.head(8)
            max_mv = df_h['total_market_value_cr'].max()
            for _, row in df_h.iterrows():
                sec = row['security_name'][:24].ljust(24)
                mv = row['total_market_value_cr']
                bar_len = int((mv / max_mv) * 30) if max_mv > 0 else 0
                bar = '■' * bar_len
                lines.append(f"{sec} | {bar:<30} | ₹{mv:6.1f} Cr ({row['fund_count']} Funds)")
        lines.append("\n")

        # MoM Accumulation Chart
        lines.append("===============================================================")
        lines.append(f"  TOP MoM NET ACCUMULATION IN {report.category_label.upper()} (AMC: {report.amc})")
        lines.append("===============================================================")
        if not report.mom_additions.empty:
            df_a = report.mom_additions.head(6)
            max_chg = df_a['mv_change_cr'].max()
            for _, row in df_a.iterrows():
                sec = row['security_name'][:24].ljust(24)
                chg = row['mv_change_cr']
                bar_len = int((chg / (max_chg + 1e-6)) * 28) if max_chg > 0 else 0
                bar = '▲' * bar_len
                lines.append(f"{sec} | {bar:<28} | +₹{chg:5.1f} Cr")
        lines.append("\n")

        # Cross Conviction Chart
        window = int(report.cross_conviction['window_months'].iloc[0]) if not report.cross_conviction.empty else 0
        lines.append("===============================================================")
        lines.append(f"  MULTI-AMC CROSS-CONVICTION (PERSISTENCE-RANKED, {window}mo LOOKBACK)")
        lines.append("===============================================================")
        if not report.cross_conviction.empty:
            df_c = report.cross_conviction.head(8)
            for _, row in df_c.iterrows():
                sec = row['security_name'][:24].ljust(24)
                amcs = row['amc_count']
                bar = '█' * amcs
                streak = row['multi_amc_streak_months']
                lines.append(f"{sec} | {bar:<10} | {amcs} AMCs now | {streak:>2}/{window}mo streak | ₹{row['total_market_value_cr']:6.1f} Cr")

        return "\n".join(lines)


# Backward-compat alias — existing callers (src/main.py, smallcap_pattern_tool.py)
# construct this with no args and get the Small Cap segment.
SmallcapPatternAnalyzer = MFCapPatternAnalyzer


def run_cap_pattern_analysis(category: str = "small", amc: str = "all", lookback_months: int = 24, verbose: bool = True) -> CapPatternReport:
    """Run the cap-segment analyzer for the given category (small | mid | large)."""
    analyzer = MFCapPatternAnalyzer(category=category)
    report = analyzer.analyze(amc=amc, lookback_months=lookback_months)
    if verbose:
        dashboard = analyzer.render_ascii_dashboard(report)
        print(dashboard)
    return report


def run_smallcap_analysis(amc: str = "all", lookback_months: int = 24, verbose: bool = True) -> CapPatternReport:
    """Backward-compat wrapper — Small Cap only. Use run_cap_pattern_analysis for mid/large."""
    return run_cap_pattern_analysis("small", amc, lookback_months, verbose)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generic Multi-AMC Market-Cap-Segment Pattern Analyzer")
    parser.add_argument("--category", "-c", type=str, default="small", choices=["small", "mid", "large"], help="Cap segment to analyze (default small)")
    parser.add_argument("--amc", "-a", type=str, default="all", help="AMC group: all | dsp | nippon | hdfc | quant | icici | kotak | bajaj")
    parser.add_argument("--lookback-months", type=int, default=24, help="Months of holdings history to score cross-conviction persistence over")
    args = parser.parse_args()
    run_cap_pattern_analysis(args.category, amc=args.amc, lookback_months=args.lookback_months, verbose=True)
