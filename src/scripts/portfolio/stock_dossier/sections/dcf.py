from __future__ import annotations

import pandas as pd

from ..base import DossierSection
from ..context import DossierContext
from ..utils import make_markdown_table


def _bar(val: float, max_val: float) -> str:
    if max_val <= 0:
        return ""
    length = int(min(40, max(1, (val / max_val) * 40)))
    return "██" * (length // 2) + ("■" if length % 2 else "")


class DcfValuationSection(DossierSection):
    """3-scenario 2-stage DCF — unique to the deleted
    stock_quant_deepdive_cli.py, no other script implements this. Reuses
    ctx.yf_info / ctx.cur_price / ctx.mcap_cr instead of the original's own
    fresh yfinance.Ticker + ClickHouse latest-close query, so this and the
    rest of the dossier can never disagree on current price/mcap."""

    key = "dcf"
    title = "Intrinsic Value & 2-Stage DCF Valuation"

    def fetch(self, ctx: DossierContext) -> str:
        info = ctx.yf_info
        latest_close = ctx.cur_price
        mcap = ctx.mcap_cr * 1e7

        if mcap == 0.0:
            from src.db.pool import get_pool
            val_df = get_pool().query_df(
                f"SELECT market_cap FROM market_data.stock_valuation FINAL WHERE symbol = '{ctx.symbol}'"
            )
            mcap = float(val_df["market_cap"].iloc[0]) if not val_df.empty else 0.0
        if mcap == 0.0:
            return "*DCF Valuation skipped: could not resolve market capitalization.*"

        shares_outstanding = mcap / latest_close
        total_cash = float(info.get("totalCash") or 0.0)
        total_debt = float(info.get("totalDebt") or 0.0)
        net_cash = total_cash - total_debt

        latest_fcf = float(info.get("freeCashflow") or 0.0)
        if latest_fcf == 0.0:
            try:
                latest_fcf = float(ctx.yf_ticker().cashflow.loc["Free Cash Flow"].iloc[0])
            except Exception:
                latest_fcf = 0.0
        if latest_fcf <= 0.0:
            return f"*DCF Valuation skipped: Free Cash Flow is negative or zero (FY FCF: ₹{latest_fcf / 1e7:.2f} Cr).*"

        scenarios = [
            {"name": "Conservative Case (High Risk/Discount)", "growth": 0.10, "wacc": 0.13, "g_terminal": 0.045},
            {"name": "Base Case (Standard Growth Recovery)", "growth": 0.14, "wacc": 0.115, "g_terminal": 0.05},
            {"name": "Aggressive Case (High Growth/Low WACC)", "growth": 0.18, "wacc": 0.10, "g_terminal": 0.055},
        ]
        results = []
        for sc in scenarios:
            cf, pv_cf = [], []
            current_fcf = latest_fcf
            for yr in range(1, 6):
                current_fcf *= (1 + sc["growth"])
                cf.append(current_fcf)
                pv_cf.append(current_fcf / ((1 + sc["wacc"]) ** yr))
            terminal_value = cf[-1] * (1 + sc["g_terminal"]) / (sc["wacc"] - sc["g_terminal"])
            pv_terminal_value = terminal_value / ((1 + sc["wacc"]) ** 5)
            enterprise_value = sum(pv_cf) + pv_terminal_value
            equity_value = enterprise_value + net_cash
            intrinsic_val = equity_value / shares_outstanding
            margin_of_safety = (1 - (latest_close / intrinsic_val)) * 100
            results.append({
                "Scenario": sc["name"],
                "Proj Growth": f"{sc['growth'] * 100:.1f}%",
                "WACC (Discount)": f"{sc['wacc'] * 100:.1f}%",
                "Terminal Growth": f"{sc['g_terminal'] * 100:.1f}%",
                "PV of Cash Flows (Cr)": f"₹{sum(pv_cf) / 1e7:.2f}",
                "PV of Terminal Val (Cr)": f"₹{pv_terminal_value / 1e7:.2f}",
                "Intrinsic Value": f"₹{intrinsic_val:.2f}",
                "Margin of Safety": f"{margin_of_safety:.1f}%",
            })

        cons_iv = float(results[0]["Intrinsic Value"].replace("₹", ""))
        base_iv = float(results[1]["Intrinsic Value"].replace("₹", ""))
        aggr_iv = float(results[2]["Intrinsic Value"].replace("₹", ""))
        ascii_chart = (
            "```text\n"
            f"Intrinsic Value vs. Current LTP (₹{latest_close:.2f})\n"
            + "=" * 80 + "\n"
            f"Conservative IV | {_bar(cons_iv, aggr_iv)} ₹{cons_iv:.2f}\n"
            f"Base Case IV    | {_bar(base_iv, aggr_iv)} ₹{base_iv:.2f}\n"
            f"Current LTP     | {_bar(latest_close, aggr_iv)} ₹{latest_close:.2f}\n"
            f"Aggressive IV   | {_bar(aggr_iv, aggr_iv)} ₹{aggr_iv:.2f}\n"
            + "=" * 80 + "\n```"
        )
        return (
            f"* **Base Free Cash Flow:** ₹{latest_fcf / 1e7:.2f} Cr\n"
            f"* **Net Cash Buffer:** ₹{net_cash / 1e7:.2f} Cr (Cash: ₹{total_cash / 1e7:.2f} Cr, Debt: ₹{total_debt / 1e7:.2f} Cr)\n\n"
            f"{ascii_chart}\n\n{make_markdown_table(pd.DataFrame(results))}"
        )
