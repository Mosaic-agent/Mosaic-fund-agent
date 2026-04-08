"""
src/tools/market_context.py
────────────────────────────
Queries recent FII / DII institutional flow data from ClickHouse and
formats it as a concise LLM-ready context string.

Used by the portfolio agent (Phase 3) to ground the LLM's reasoning with
actual institutional-flow figures rather than relying on the model's
general knowledge about NSE market dynamics.

Public API
──────────
    get_fii_dii_context(days: int = 5) -> dict

    Returns:
        {
          "rows": [{"trade_date": date, "fii_net_cr": float, "dii_net_cr": float}, ...],
          "fii_consec_sell_days": int,   # consecutive days FII was net seller
          "fii_consec_buy_days":  int,   # consecutive days FII was net buyer
          "summary_str": str,            # LLM-ready one-paragraph narrative
        }

    On any error (ClickHouse unavailable, table empty, etc.) returns:
        {"rows": [], "fii_consec_sell_days": 0, "fii_consec_buy_days": 0,
         "summary_str": "FII/DII flow data unavailable."}
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

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
        import clickhouse_connect
        from config.settings import settings

        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )

        result = client.query(
            f"""
            SELECT trade_date, fii_net_cr, dii_net_cr
            FROM market_data.fii_dii_flows FINAL
            ORDER BY trade_date DESC
            LIMIT {int(days)}
            """
        )
        client.close()

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
