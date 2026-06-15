"""
src/tools/market_context.py
────────────────────────────
Queries recent FII / DII institutional flow data and DXY (US Dollar Index)
data from ClickHouse and formats them as concise LLM-ready context strings.

Public API
──────────
    get_fii_dii_context(days: int = 5) -> dict
    get_dxy_context(days: int = 30) -> str   — @tool decorated, agent-callable
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from langchain_core.tools import tool

logger = logging.getLogger(__name__)


def get_fii_dii_context(days: int = 5) -> dict[str, Any]:
    """
    Fetch the last `days` trading days of FII/DII net flows from ClickHouse.

    Parameters
    ----------
    days : number of recent trading days to retrieve

    Returns
    -------
    dict with keys:
        rows                 — list of row dicts (trade_date, fii_net_cr, dii_net_cr)
        fii_consec_sell_days — consecutive days FII was net seller (most recent streak)
        fii_consec_buy_days  — consecutive days FII was net buyer (most recent streak)
        summary_str          — LLM-ready narrative string
    """
    _empty = {
        "rows": [],
        "fii_consec_sell_days": 0,
        "fii_consec_buy_days": 0,
        "summary_str": "FII/DII flow data unavailable.",
    }

    try:
        from src.db.pool import get_pool as _get_ch_pool

        with _get_ch_pool().acquire() as client:
            result = client.query(
                f"""
                SELECT trade_date, fii_net_cr, dii_net_cr
                FROM market_data.fii_dii_flows FINAL
                ORDER BY trade_date DESC
                LIMIT {int(days)}
                """
            )

        raw_rows = result.result_rows
        if not raw_rows:
            return _empty

        # Sort ascending (oldest first)
        rows = [
            {
                "trade_date": r[0],
                "fii_net_cr": float(r[1]),
                "dii_net_cr": float(r[2]),
            }
            for r in sorted(raw_rows, key=lambda x: x[0])
        ]

    except Exception as exc:
        logger.warning("FII/DII context fetch failed: %s", exc)
        return _empty

    # ── Consecutive sell/buy streak (from most recent day backwards) ──────────
    fii_consec_sell = 0
    fii_consec_buy = 0
    for row in reversed(rows):
        net = row["fii_net_cr"]
        if net < 0:
            if fii_consec_buy > 0:
                break
            fii_consec_sell += 1
        elif net > 0:
            if fii_consec_sell > 0:
                break
            fii_consec_buy += 1
        else:
            break

    # ── Build LLM-ready narrative ─────────────────────────────────────────────
    summary_str = _build_summary(rows, fii_consec_sell, fii_consec_buy)

    return {
        "rows": rows,
        "fii_consec_sell_days": fii_consec_sell,
        "fii_consec_buy_days": fii_consec_buy,
        "summary_str": summary_str,
    }


def _build_summary(
    rows: list[dict],
    fii_consec_sell: int,
    fii_consec_buy: int,
) -> str:
    """
    Build a compact, LLM-readable paragraph from a sorted list of flow rows.

    Example output:
        "FII/DII Flows (last 5 trading days, ₹ Crore, cash segment):
         Date        FII Net    DII Net
         2026-04-02  -1,850.3   +2,100.5
         2026-04-03  +320.0     +850.0
         ...
         FIIs have been net sellers for 3 consecutive days (cumulative: -4,230 Cr).
         DIIs have been net buyers for 5 consecutive days (cumulative: +6,780 Cr),
         partially absorbing FII selling pressure."
    """
    if not rows:
        return "FII/DII flow data unavailable."

    n = len(rows)
    fii_vals = [r["fii_net_cr"] for r in rows]
    dii_vals = [r["dii_net_cr"] for r in rows]
    fii_cum = sum(fii_vals)
    dii_cum = sum(dii_vals)

    # Table header
    lines = [
        f"FII/DII Institutional Flows (last {n} trading days, ₹ Crore, cash segment):",
        f"{'Date':<12}  {'FII Net':>12}  {'DII Net':>12}",
        "-" * 40,
    ]
    for r in rows:
        td = r["trade_date"]
        date_str = td.isoformat() if isinstance(td, date) else str(td)[:10]
        lines.append(
            f"{date_str:<12}  {r['fii_net_cr']:>+12,.1f}  {r['dii_net_cr']:>+12,.1f}"
        )

    lines.append("-" * 40)
    lines.append(f"{'5-day cumul.':<12}  {fii_cum:>+12,.0f}  {dii_cum:>+12,.0f}")

    # Narrative sentence
    if fii_consec_sell >= 3:
        fii_narrative = (
            f"FIIs have been net sellers for {fii_consec_sell} consecutive days "
            f"(cumulative: ₹{fii_cum:+,.0f} Cr), signalling foreign capital outflows."
        )
    elif fii_consec_buy >= 3:
        fii_narrative = (
            f"FIIs have been net buyers for {fii_consec_buy} consecutive days "
            f"(cumulative: ₹{fii_cum:+,.0f} Cr), indicating foreign inflows."
        )
    else:
        direction = "net buyers" if fii_cum >= 0 else "net sellers"
        fii_narrative = (
            f"FIIs have been mixed over the period, net {direction} "
            f"(cumulative: ₹{fii_cum:+,.0f} Cr)."
        )

    if dii_cum >= 0:
        dii_narrative = (
            f"DIIs provided support with ₹{dii_cum:+,.0f} Cr cumulative net buying."
        )
    else:
        dii_narrative = (
            f"DIIs were also net sellers (cumulative: ₹{dii_cum:+,.0f} Cr), "
            "amplifying market weakness."
        )

    lines.append("")
    lines.append(fii_narrative)
    lines.append(dii_narrative)

    return "\n".join(lines)


@tool
def get_dxy_context(days: int = 30) -> str:
    """
    Fetch recent US Dollar Index (DXY) data from ClickHouse (market_data.daily_prices)
    and return an LLM-ready summary: current level, 5-day and 20-day change, trend
    direction, and a short macro interpretation for gold and INR.

    DXY must be imported first: `mosaic import --categories indices`
    Falls back to a clear error message if data is unavailable.

    Args:
        days: number of calendar days of history to fetch (default 30; min 21 for trend)
    """
    days = max(int(days), 21)

    try:
        from src.db.pool import get_pool as _get_pool

        with _get_pool().acquire() as client:
            result = client.query(
                f"""
                SELECT trade_date, close
                FROM market_data.daily_prices FINAL
                WHERE symbol = 'DXY'
                  AND trade_date >= today() - {days}
                ORDER BY trade_date ASC
                """
            )

        raw = result.result_rows
        if not raw:
            return (
                "DXY data not available in ClickHouse. "
                "Run: `mosaic import --categories indices` to import it."
            )

        rows = [{"trade_date": r[0], "close": float(r[1])} for r in raw]

    except Exception as exc:
        logger.warning("DXY context fetch failed: %s", exc)
        return f"DXY context unavailable: {exc}"

    latest   = rows[-1]
    prev5    = rows[-6]["close"] if len(rows) >= 6 else rows[0]["close"]
    prev20   = rows[-21]["close"] if len(rows) >= 21 else rows[0]["close"]
    level    = latest["close"]
    chg5     = (level - prev5)  / prev5  * 100
    chg20    = (level - prev20) / prev20 * 100
    as_of    = latest["trade_date"]
    as_of_str = as_of.isoformat() if isinstance(as_of, date) else str(as_of)[:10]

    # Trend label
    if chg5 >= 0.3:
        trend = "strengthening"
    elif chg5 <= -0.3:
        trend = "weakening"
    else:
        trend = "flat"

    # Macro interpretation
    if level <= 100:
        regime = "weak-dollar regime — historically supportive for gold and INR."
    elif level <= 104:
        regime = "moderate-dollar zone — gold and INR under mild pressure."
    elif level <= 108:
        regime = "strong-dollar zone — headwind for gold (USD-denominated) and INR."
    else:
        regime = "very strong dollar — significant headwind for gold and EM currencies."

    lines = [
        f"US Dollar Index (DXY) as of {as_of_str}:",
        f"  Level : {level:.2f}",
        f"  5-day : {chg5:+.2f}%  ({trend})",
        f"  20-day: {chg20:+.2f}%",
        f"  Regime: {regime}",
    ]
    return "\n".join(lines)
