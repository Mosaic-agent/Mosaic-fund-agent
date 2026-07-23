"""
src/scripts/market/amc_house_intelligence.py
─────────────────────────────────────────────
Unified Multi-AMC Institutional Intelligence & Conviction Engine.

Design Patterns Applied:
  - Strategy Pattern: Modular analysis strategies (ConvictionScoreStrategy,
    MidCapClusterStrategy, HousePivotDriftStrategy).
  - Facade Pattern: AmcIntelligenceFacade provides a unified interface for
    CLI, ReAct agents, and Streamlit UI.

Supported AMCs:
  - DSP Mutual Fund (DSP_)
  - ICICI Prudential MF (ICICI_)
  - HDFC Mutual Fund (HDFC_)
  - Kotak Mahindra MF (KOTAK_)
  - Nippon India MF (NIPPON_)
  - Quant Mutual Fund (QUANT_)
  - Bajaj Finserv MF (BAJAJ_)

Usage:
  python src/scripts/market/amc_house_intelligence.py --amc hdfc
  python src/scripts/market/amc_house_intelligence.py --amc dsp
  python src/scripts/market/amc_house_intelligence.py --amc all
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table

sys.path.append(os.getcwd())
from config.settings import settings

logger = logging.getLogger(__name__)
console = Console()

# AMC Prefix pattern map
AMC_PATTERNS = {
    "dsp": "DSP_%",
    "icici": "ICICI_%",
    "hdfc": "HDFC_%",
    "kotak": "KOTAK_%",
    "nippon": "NIPPON_%",
    "quant": "QUANT_%",
    "bajaj": "BAJAJ_%",
}


def get_latest_available_month(client, pattern: str) -> date | None:
    """Find the latest available as_of_month for the AMC pattern."""
    if pattern == "%":
        query = "SELECT max(as_of_month) FROM market_data.mf_holdings FINAL"
    else:
        query = f"SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name LIKE '{pattern}'"
    res = client.query(query).result_rows
    if res and res[0][0]:
        return res[0][0]
    return None


def get_prev_month(client, pattern: str, cur_month: date) -> date | None:
    """Find the month immediately prior to cur_month."""
    if pattern == "%":
        query = f"SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE as_of_month < '{cur_month}'"
    else:
        query = f"SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name LIKE '{pattern}' AND as_of_month < '{cur_month}'"
    res = client.query(query).result_rows
    if res and res[0][0]:
        return res[0][0]
    return None


# ── Strategy Pattern ─────────────────────────────────────────────────────────

class AMCAnalysisStrategy(ABC):
    """Abstract Strategy interface for institutional holding analysis."""

    @abstractmethod
    def name(self) -> str:
        """Name of the strategy."""

    @abstractmethod
    def execute(
        self,
        client: Any,
        cur_m_str: str,
        prev_m_str: str,
        where_pattern: str,
        nifty50_list: set[str],
    ) -> pd.DataFrame:
        """Execute the analytical strategy and return a DataFrame."""


class ConvictionScoreStrategy(AMCAnalysisStrategy):
    """Strategy 1: Computes House Conviction Score (Schemes Count × Aggregate Weight)."""

    def name(self) -> str:
        return "Top Conviction Holdings"

    def execute(
        self,
        client: Any,
        cur_m_str: str,
        prev_m_str: str,
        where_pattern: str,
        nifty50_list: set[str],
    ) -> pd.DataFrame:
        query = f"""
        SELECT 
            security_name,
            count(DISTINCT fund_name) as fund_count,
            round(sum(pct_of_nav), 2) as agg_weight,
            round(count(DISTINCT fund_name) * sum(pct_of_nav), 0) as conviction_score
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{cur_m_str}'
          AND asset_type = 'equity'
          AND {where_pattern}
          AND security_name NOT LIKE '%ETF%'
          AND security_name NOT LIKE '%Mutual Fund%'
          AND security_name NOT LIKE 'PH_%'
          AND security_name NOT LIKE '%Govt Stock%'
          AND security_name NOT LIKE '%Treps%'
          AND security_name NOT LIKE '%Cash%'
          AND security_name NOT LIKE '%Ncd%'
        GROUP BY security_name
        ORDER BY conviction_score DESC
        LIMIT 12
        """
        return client.query_df(query)


class MidCapClusterStrategy(AMCAnalysisStrategy):
    """Strategy 2: Identifies multi-fund emerging favorites outside Nifty 50."""

    def name(self) -> str:
        return "Mid-Cap Alpha Cluster"

    def execute(
        self,
        client: Any,
        cur_m_str: str,
        prev_m_str: str,
        where_pattern: str,
        nifty50_list: set[str],
    ) -> pd.DataFrame:
        query = f"""
        SELECT 
            security_name,
            count(DISTINCT fund_name) as fund_count,
            round(sum(pct_of_nav), 2) as agg_weight
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{cur_m_str}'
          AND asset_type = 'equity'
          AND {where_pattern}
          AND security_name NOT LIKE '%ETF%'
          AND security_name NOT LIKE '%Mutual Fund%'
          AND security_name NOT LIKE 'PH_%'
          AND security_name NOT LIKE '%Govt Stock%'
          AND security_name NOT LIKE '%Treps%'
          AND security_name NOT LIKE '%Cash%'
          AND security_name NOT LIKE '%Ncd%'
        GROUP BY security_name
        HAVING fund_count BETWEEN 2 AND 15
        ORDER BY agg_weight DESC
        LIMIT 15
        """
        df = client.query_df(query)
        if not df.empty and nifty50_list:
            df = df[~df['security_name'].isin(nifty50_list)].head(10)
        return df


class HousePivotDriftStrategy(AMCAnalysisStrategy):
    """Strategy 3: Calculates MoM weight deltas, additions, and trims."""

    def name(self) -> str:
        return "Synchronized House Pivots"

    def execute(
        self,
        client: Any,
        cur_m_str: str,
        prev_m_str: str,
        where_pattern: str,
        nifty50_list: set[str],
    ) -> pd.DataFrame:
        query = f"""
        WITH cur AS (
            SELECT fund_name, security_name, pct_of_nav as p_cur
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{cur_m_str}' AND {where_pattern}
        ),
        prev AS (
            SELECT fund_name, security_name, pct_of_nav as p_prev
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{prev_m_str}' AND {where_pattern}
        )
        SELECT 
            security_name,
            round(sum(p_cur - p_prev), 2) as house_drift,
            countIf(p_cur > p_prev + 0.1) as add_count,
            countIf(p_cur < p_prev - 0.1) as trim_count
        FROM cur 
        FULL OUTER JOIN prev ON cur.fund_name = prev.fund_name AND cur.security_name = prev.security_name
        WHERE security_name NOT LIKE '%ETF%'
          AND security_name NOT LIKE '%Mutual Fund%'
          AND security_name NOT LIKE 'PH_%'
          AND security_name NOT LIKE '%Govt Stock%'
          AND security_name NOT LIKE '%Treps%'
          AND security_name NOT LIKE '%Cash%'
          AND security_name NOT LIKE '%Ncd%'
        GROUP BY security_name
        HAVING abs(house_drift) > 0.5 OR add_count > 1 OR trim_count > 1
        ORDER BY abs(house_drift) DESC
        LIMIT 10
        """
        return client.query_df(query)


# ── Facade Pattern ───────────────────────────────────────────────────────────

class AmcIntelligenceFacade:
    """
    Facade providing a unified interface over all AMC analytical strategies.
    Used by CLI, ReAct agents, and Streamlit dashboard.
    """

    def __init__(self, amc_key: str = "all") -> None:
        self.amc_key = amc_key.lower()
        self.pattern = AMC_PATTERNS.get(self.amc_key, "%")
        self.amc_label = self.amc_key.upper() if self.amc_key in AMC_PATTERNS else "ALL AMCs"
        self._strategies: list[AMCAnalysisStrategy] = [
            ConvictionScoreStrategy(),
            MidCapClusterStrategy(),
            HousePivotDriftStrategy(),
        ]

    def get_full_report(self) -> dict[str, Any]:
        """Execute all strategies and return structured DataFrames with metadata."""
        from src.db.pool import get_client
        client = get_client()

        cur_month = get_latest_available_month(client, self.pattern)
        if not cur_month:
            client.close()
            return {"error": f"No holdings found for AMC choice: {self.amc_key}"}

        prev_month = get_prev_month(client, self.pattern, cur_month)
        cur_m_str = cur_month.strftime("%Y-%m-%d")
        prev_m_str = prev_month.strftime("%Y-%m-%d") if prev_month else cur_m_str
        where_pattern = "1=1" if self.pattern == "%" else f"fund_name LIKE '{self.pattern}'"

        # Identify Nifty 50 Proxy
        nifty50_df = client.query_df(
            f"SELECT DISTINCT security_name FROM market_data.mf_holdings FINAL "
            f"WHERE (fund_name LIKE '%NIFTY_50%' OR fund_name LIKE '%SENSEX%') "
            f"  AND as_of_month = '{cur_m_str}'"
        )
        nifty50_list = set(nifty50_df.iloc[:, 0].tolist()) if not nifty50_df.empty else set()

        results = {
            "amc_label": self.amc_label,
            "cur_month": cur_m_str,
            "prev_month": prev_m_str,
        }

        for strat in self._strategies:
            try:
                results[strat.name()] = strat.execute(
                    client, cur_m_str, prev_m_str, where_pattern, nifty50_list
                )
            except Exception as exc:
                logger.error("Strategy '%s' failed for AMC '%s': %s", strat.name(), self.amc_key, exc)
                results[strat.name()] = pd.DataFrame()
                results[f"{strat.name()}_error"] = str(exc)

        client.close()
        return results


def run_amc_intelligence(amc_key: str = "all") -> None:
    facade = AmcIntelligenceFacade(amc_key)
    report = facade.get_full_report()

    if "error" in report:
        console.print(f"[red]{report['error']}[/red]")
        return

    cur_m_str = report["cur_month"]
    prev_m_str = report["prev_month"]
    amc_label = report["amc_label"]

    console.print(f"\n[bold cyan]🏛 Institutional Conviction Intelligence: {amc_label} ({cur_m_str})[/bold cyan]")

    # 1. Conviction Score Table
    df_conviction = report.get("Top Conviction Holdings", pd.DataFrame())
    if not df_conviction.empty:
        t1 = Table(title="🏆 Top Conviction Holdings (Scheme Count × Weight)", header_style="bold magenta")
        t1.add_column("Security Name", style="bold white")
        t1.add_column("Conviction Score", justify="right", style="cyan")
        t1.add_column("Schemes", justify="right")
        t1.add_column("Agg Weight (%)", justify="right")
        for _, r in df_conviction.iterrows():
            t1.add_row(r['security_name'][:30], str(int(r['conviction_score'])), str(r['fund_count']), f"{r['agg_weight']}%")
        console.print(t1)

    # 2. Mid-Cap Cluster Table
    df_midcap = report.get("Mid-Cap Alpha Cluster", pd.DataFrame())
    if not df_midcap.empty:
        t2 = Table(title="🚀 Mid-Cap & Multi-Fund Alpha Cluster (Ex-Nifty 50)", header_style="bold yellow")
        t2.add_column("Security Name", style="bold white")
        t2.add_column("Agg Weight (%)", justify="right", style="green")
        t2.add_column("Schemes", justify="right")
        for _, r in df_midcap.iterrows():
            t2.add_row(r['security_name'][:30], f"{r['agg_weight']}%", str(r['fund_count']))
        console.print(t2)

    # 3. Synchronized House Pivots Table
    df_pivot = report.get("Synchronized House Pivots", pd.DataFrame())
    if not df_pivot.empty:
        t3 = Table(title=f"🔄 Synchronized House Pivots (MoM Drift: {prev_m_str} → {cur_m_str})", header_style="bold green")
        t3.add_column("Security Name", style="bold white")
        t3.add_column("House Δ (%)", justify="right")
        t3.add_column("Add Funds", justify="right", style="bold green")
        t3.add_column("Trim Funds", justify="right", style="bold red")
        for _, r in df_pivot.iterrows():
            drift_str = f"{r['house_drift']:+.2f}%"
            color = "green" if r['house_drift'] > 0 else "red"
            t3.add_row(r['security_name'][:30], f"[{color}]{drift_str}[/{color}]", str(r['add_count']), str(r['trim_count']))
        console.print(t3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified AMC House Conviction Intelligence")
    parser.add_argument(
        "--amc",
        choices=["dsp", "icici", "hdfc", "kotak", "nippon", "quant", "bajaj", "all"],
        default="all",
        help="AMC to analyze (default: all)",
    )
    args = parser.parse_args()
    run_amc_intelligence(args.amc)
