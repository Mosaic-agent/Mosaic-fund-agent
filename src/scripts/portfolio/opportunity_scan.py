"""
scripts/opportunity_scan.py
────────────────────────────
Cross-asset opportunity scanner across the full ClickHouse database.

Signals computed per asset:
  • Price momentum  : 5D / 20D / 60D returns
  • Mean reversion  : % from 52-week high (drawdown) + RSI-14
  • iNAV disc/prem  : ETFs only — discount = buy opportunity
  • ETF AUM trend   : 30D AUM change (%)
  • FII/DII flows   : 5D net (equity ETFs / safe-haven inverse)
  • ML prediction   : GOLDBEES-specific LightGBM 5D return

Composite opportunity score (0–100):
  60% momentum + mean-reversion blend
  20% iNAV / flow / AUM
  20% ML / macro (where available, else neutral 50)

Output: ranked table — best opportunities at top.
"""
from __future__ import annotations

import os, sys, logging
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

logging.basicConfig(level=logging.ERROR)

import math
from datetime import date, timedelta
import numpy as np
import pandas as pd
import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.rule import Rule
from rich import box

from config.settings import settings

console = Console()

# ── helpers ────────────────────────────────────────────────────────────────────

def _pct(new, old):
    if old and old != 0:
        return round((new - old) / abs(old) * 100, 2)
    return None

def _rsi(closes: pd.Series, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    delta = closes.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean().iloc[-1]
    loss  = (-delta.clip(upper=0)).rolling(period).mean().iloc[-1]
    if loss == 0:
        return 100.0
    rs = gain / loss
    return round(float(100 - 100 / (1 + rs)), 1)

def _score_momentum(ret5, ret20, ret60) -> float:
    """Score momentum 0–100. Blend of short/mid/long returns."""
    scores = []
    if ret5  is not None: scores.append(max(0, min(100, 50 + ret5  * 5)))
    if ret20 is not None: scores.append(max(0, min(100, 50 + ret20 * 2.5)))
    if ret60 is not None: scores.append(max(0, min(100, 50 + ret60 * 1.0)))
    return round(float(np.mean(scores)), 1) if scores else 50.0

def _score_reversion(drawdown_pct, rsi) -> float:
    """Mean-reversion opportunity: deep drawdown + low RSI = higher score."""
    scores = []
    # drawdown_pct is negative (e.g. -25 means 25% below 52W high)
    if drawdown_pct is not None:
        # -50% dd → 100,  0% dd → 0  (buying at lows)
        scores.append(max(0, min(100, -drawdown_pct * 2)))
    if rsi is not None:
        # RSI 20 → 100, RSI 80 → 0
        scores.append(max(0, min(100, (80 - rsi) * (100 / 60))))
    return round(float(np.mean(scores)), 1) if scores else 50.0

def _score_inav(disc_pct) -> float:
    """Discount = opportunity. +0.5% disc → 100; +0.5% prem → 0."""
    if disc_pct is None:
        return 50.0
    # disc_pct > 0 = premium (bad), < 0 = discount (good)
    # remap: -0.5% → 100, 0 → 50, +0.5% → 0
    return round(float(max(0, min(100, 50 - disc_pct * 100))), 1)

# ── fetch ──────────────────────────────────────────────────────────────────────

def fetch_all(client) -> dict:
    cutoff_5d  = (date.today() - timedelta(days=8)).isoformat()
    cutoff_20d = (date.today() - timedelta(days=30)).isoformat()
    cutoff_60d = (date.today() - timedelta(days=90)).isoformat()
    cutoff_52w = (date.today() - timedelta(days=365)).isoformat()

    # ── price history (last 365 days) ─────────────────────────────────────────
    console.print("[dim]Querying price history...[/dim]")
    rows = client.query(f"""
        SELECT symbol, category, trade_date,
               argMax(close,  imported_at) AS close
        FROM market_data.daily_prices
        WHERE trade_date >= toDate('{cutoff_52w}')
        GROUP BY symbol, category, trade_date
        ORDER BY symbol, trade_date ASC
    """).result_rows
    df_prices = pd.DataFrame(rows, columns=["symbol","category","trade_date","close"])
    df_prices["trade_date"] = pd.to_datetime(df_prices["trade_date"])
    df_prices["close"] = pd.to_numeric(df_prices["close"], errors="coerce")

    # ── iNAV snapshots (latest per symbol) ────────────────────────────────────
    console.print("[dim]Querying iNAV snapshots...[/dim]")
    inav_rows = client.query("""
        SELECT symbol, argMax(premium_discount_pct, snapshot_at) AS disc_pct,
               max(snapshot_at) AS latest
        FROM market_data.inav_snapshots
        GROUP BY symbol
    """).result_rows
    inav = {r[0]: float(r[1]) for r in inav_rows if r[1] is not None}

    # ── ETF AUM (latest + 30d ago) ─────────────────────────────────────────────
    console.print("[dim]Querying ETF AUM...[/dim]")
    aum_rows = client.query("""
        SELECT symbol,
               argMax(aum_usd, trade_date) AS aum_latest,
               argMin(aum_usd, trade_date) AS aum_30d
        FROM market_data.etf_aum
        WHERE trade_date >= today() - 35
        GROUP BY symbol
    """).result_rows
    aum = {r[0]: {"latest": float(r[1] or 0), "d30": float(r[2] or 0)}
           for r in aum_rows if r[1]}

    # ── FII/DII 5D net ─────────────────────────────────────────────────────────
    console.print("[dim]Querying FII/DII flows...[/dim]")
    flow_rows = client.query("""
        SELECT sum(fii_net_cr) AS fii_5d, sum(dii_net_cr) AS dii_5d
        FROM market_data.fii_dii_flows FINAL
        WHERE trade_date >= today() - 5
    """).result_rows
    fii_5d = float(flow_rows[0][0] or 0) if flow_rows else 0
    dii_5d = float(flow_rows[0][1] or 0) if flow_rows else 0
    fii_net = fii_5d + dii_5d

    # ── ML prediction ─────────────────────────────────────────────────────────
    console.print("[dim]Querying ML predictions...[/dim]")
    ml_rows = client.query("""
        SELECT expected_return_pct, as_of
        FROM market_data.ml_predictions
        ORDER BY as_of DESC LIMIT 1
    """).result_rows
    ml_pred = {"GOLDBEES": float(ml_rows[0][0])} if ml_rows else {}

    return dict(df_prices=df_prices, inav=inav, aum=aum,
                fii_net=fii_net, ml_pred=ml_pred)


def build_signals(data: dict) -> list[dict]:
    df   = data["df_prices"]
    inav = data["inav"]
    aum  = data["aum"]
    fii_net = data["fii_net"]
    ml_pred = data["ml_pred"]

    # FII flow score (equity ETFs benefit, safe-haven inverse)
    fii_clamped = max(-15000, min(15000, fii_net))
    equity_flow_score = round(50 + (fii_clamped / 15000) * 50, 1)
    haven_flow_score  = round(100 - equity_flow_score, 1)

    EQUITY_ETFS = {"NIFTYBEES","BANKBEES","ITBEES","JUNIORBEES","CPSEETF",
                   "AUTOBEES","PHARMABEES","PSUBNKBEES","MID150BEES","SMALL250",
                   "HDFCNIFTY","SETFNIF50","FMCGIETF","MONIFTY500",
                   "ICICIB22","MON100","MAFANG","HNGSNGBEES","MAHKTECH","MASPTOP50"}
    HAVEN_ETFS  = {"GOLDBEES","SILVERBEES","LIQUIDBEES","LIQUIDCASE","GILT5YBEES"}

    results = []
    for (symbol, category), grp in df.groupby(["symbol","category"]):
        grp = grp.sort_values("trade_date").dropna(subset=["close"])
        closes = grp["close"].reset_index(drop=True)
        if len(closes) < 5:
            continue

        last = float(closes.iloc[-1])

        # Returns
        def _ret(n):
            idx = max(0, len(closes) - n - 1)
            old = float(closes.iloc[idx])
            return _pct(last, old)

        ret5  = _ret(5)
        ret20 = _ret(20)
        ret60 = _ret(60)
        ret252 = _ret(252)

        # 52W high/low
        high52 = float(closes.max())
        low52  = float(closes.min())
        dd_from_high = _pct(last, high52)      # negative = below high
        up_from_low  = _pct(last, low52)       # positive = above low

        rsi = _rsi(closes)

        # Scores
        mom_score = _score_momentum(ret5, ret20, ret60)
        rev_score = _score_reversion(dd_from_high, rsi)

        # iNAV
        disc_pct = inav.get(symbol)
        inav_score = _score_inav(disc_pct)

        # AUM trend
        aum_data = aum.get(symbol)
        aum_change_pct = None
        if aum_data and aum_data["d30"] > 0:
            aum_change_pct = _pct(aum_data["latest"], aum_data["d30"])

        # Flow score
        if symbol in EQUITY_ETFS:
            flow_score = equity_flow_score
        elif symbol in HAVEN_ETFS:
            flow_score = haven_flow_score
        else:
            flow_score = 50.0

        # ML score
        ml_ret = ml_pred.get(symbol)
        if ml_ret is not None:
            ml_score = max(0, min(100, 50 + ml_ret * (100/3)))
        else:
            ml_score = 50.0

        # ── Composite ────────────────────────────────────────────────────────
        # Weights vary by asset type
        if category == "etfs":
            composite = (
                mom_score  * 0.30 +
                rev_score  * 0.20 +
                inav_score * 0.20 +
                flow_score * 0.15 +
                ml_score   * 0.15
            )
        else:
            composite = (
                mom_score  * 0.45 +
                rev_score  * 0.35 +
                ml_score   * 0.20
            )

        results.append({
            "symbol":       symbol,
            "category":     category,
            "last":         round(last, 2),
            "ret_5d":       ret5,
            "ret_20d":      ret20,
            "ret_60d":      ret60,
            "ret_1y":       ret252,
            "dd_52w":       dd_from_high,
            "rsi":          rsi,
            "disc_pct":     disc_pct,
            "aum_chg_pct":  aum_change_pct,
            "mom_score":    mom_score,
            "rev_score":    rev_score,
            "inav_score":   inav_score,
            "flow_score":   flow_score,
            "ml_score":     ml_score,
            "composite":    round(composite, 1),
        })

    return sorted(results, key=lambda x: -x["composite"])


def _fmt_pct(v, suffix="%") -> str:
    if v is None: return "[dim]N/A[/dim]"
    color = "green" if v > 0 else ("red" if v < 0 else "dim")
    return f"[{color}]{v:+.1f}{suffix}[/{color}]"

def _fmt_score(v) -> str:
    if v is None: return "—"
    if v >= 70: return f"[bold green]{v:.0f}[/bold green]"
    if v >= 55: return f"[green]{v:.0f}[/green]"
    if v >= 45: return f"[yellow]{v:.0f}[/yellow]"
    if v >= 30: return f"[red]{v:.0f}[/red]"
    return f"[bold red]{v:.0f}[/bold red]"

def _action(score) -> str:
    if score >= 72: return "[bold green]BUY[/bold green]"
    if score >= 60: return "[green]ACCUMULATE[/green]"
    if score >= 45: return "[yellow]HOLD[/yellow]"
    if score >= 30: return "[red]TRIM[/red]"
    return "[bold red]AVOID[/bold red]"


def print_results(results: list[dict], fii_net: float) -> None:
    # ── Header ────────────────────────────────────────────────────────────────
    flow_dir = "net BUYING" if fii_net > 0 else "net SELLING"
    flow_col = "green" if fii_net > 0 else "red"
    console.print()
    console.print(Rule(f"[bold cyan]Cross-Asset Opportunity Scanner[/bold cyan]  "
                       f"[dim]FII+DII 5D: [{flow_col}]{fii_net:+,.0f} Cr ({flow_dir})[/{flow_col}][/dim]"))
    console.print()

    categories = ["etfs", "commodities", "stocks", "indices"]
    cat_labels  = {"etfs": "ETFs", "commodities": "Commodities",
                   "stocks": "Nifty 50 Stocks", "indices": "Indices"}

    for cat in categories:
        cat_results = [r for r in results if r["category"] == cat]
        if not cat_results:
            continue

        console.print(f"\n[bold white]{cat_labels[cat]}[/bold white]  "
                      f"[dim]{len(cat_results)} symbols[/dim]")

        tbl = Table(box=box.SIMPLE_HEAD, show_header=True,
                    header_style="bold dim", padding=(0, 1))
        tbl.add_column("Symbol",   style="cyan",  width=14)
        tbl.add_column("Last",     justify="right", width=9)
        tbl.add_column("5D",       justify="right", width=7)
        tbl.add_column("20D",      justify="right", width=7)
        tbl.add_column("60D",      justify="right", width=7)
        tbl.add_column("DD 52W",   justify="right", width=8)
        tbl.add_column("RSI",      justify="right", width=5)
        if cat == "etfs":
            tbl.add_column("iNAV±",    justify="right", width=7)
        tbl.add_column("Score",    justify="right", width=6)
        tbl.add_column("Signal",   width=12)

        # Top 20 for stocks (too many), all others
        limit = 20 if cat == "stocks" else len(cat_results)
        for r in cat_results[:limit]:
            row = [
                r["symbol"],
                f"{r['last']:,.2f}",
                _fmt_pct(r["ret_5d"]),
                _fmt_pct(r["ret_20d"]),
                _fmt_pct(r["ret_60d"]),
                _fmt_pct(r["dd_52w"]),
                f"{r['rsi']:.0f}" if r["rsi"] else "—",
            ]
            if cat == "etfs":
                row.append(_fmt_pct(r["disc_pct"]) if r["disc_pct"] is not None else "[dim]N/A[/dim]")
            row += [_fmt_score(r["composite"]), _action(r["composite"])]
            tbl.add_row(*row)

        console.print(tbl)

    # ── Top 10 across all assets ───────────────────────────────────────────────
    console.print()
    console.print(Rule("[bold green]Top 10 Opportunities (all assets)[/bold green]"))
    top10 = [r for r in results if r["category"] != "indices"][:10]
    tbl2 = Table(box=box.SIMPLE_HEAD, show_header=True,
                 header_style="bold dim", padding=(0, 1))
    tbl2.add_column("Rank",    width=5,  justify="right")
    tbl2.add_column("Symbol",  width=14, style="bold cyan")
    tbl2.add_column("Cat",     width=12)
    tbl2.add_column("Score",   width=6,  justify="right")
    tbl2.add_column("Signal",  width=12)
    tbl2.add_column("5D",      width=7,  justify="right")
    tbl2.add_column("20D",     width=7,  justify="right")
    tbl2.add_column("DD 52W",  width=8,  justify="right")
    tbl2.add_column("RSI",     width=5,  justify="right")
    tbl2.add_column("Why",     width=35)

    for i, r in enumerate(top10, 1):
        # Build a short "why" string
        reasons = []
        if r["ret_20d"] and r["ret_20d"] > 5:   reasons.append(f"momentum +{r['ret_20d']:.1f}%")
        if r["dd_52w"]  and r["dd_52w"] < -20:  reasons.append(f"DD {r['dd_52w']:.0f}% from 52W")
        if r["rsi"]     and r["rsi"] < 35:       reasons.append(f"RSI oversold {r['rsi']:.0f}")
        if r["disc_pct"] is not None and r["disc_pct"] < -0.3: reasons.append(f"iNAV disc {r['disc_pct']:+.2f}%")
        if r["ml_score"] and r["ml_score"] > 65: reasons.append("ML bullish")
        why = " · ".join(reasons) if reasons else "blend of signals"

        tbl2.add_row(
            str(i), r["symbol"], r["category"],
            _fmt_score(r["composite"]), _action(r["composite"]),
            _fmt_pct(r["ret_5d"]), _fmt_pct(r["ret_20d"]),
            _fmt_pct(r["dd_52w"]),
            f"{r['rsi']:.0f}" if r["rsi"] else "—",
            f"[dim]{why}[/dim]",
        )
    console.print(tbl2)

    # ── Avoid list ─────────────────────────────────────────────────────────────
    console.print()
    console.print(Rule("[bold red]Avoid / Trim (bottom 10, excl. indices)[/bold red]"))
    bottom10 = [r for r in reversed(results) if r["category"] != "indices"][:10]
    tbl3 = Table(box=box.SIMPLE_HEAD, show_header=True,
                 header_style="bold dim", padding=(0, 1))
    tbl3.add_column("Symbol",  width=14, style="red")
    tbl3.add_column("Cat",     width=12)
    tbl3.add_column("Score",   width=6,  justify="right")
    tbl3.add_column("Signal",  width=10)
    tbl3.add_column("5D",      width=7,  justify="right")
    tbl3.add_column("20D",     width=7,  justify="right")
    tbl3.add_column("RSI",     width=5,  justify="right")

    for r in bottom10:
        tbl3.add_row(
            r["symbol"], r["category"],
            _fmt_score(r["composite"]), _action(r["composite"]),
            _fmt_pct(r["ret_5d"]), _fmt_pct(r["ret_20d"]),
            f"{r['rsi']:.0f}" if r["rsi"] else "—",
        )
    console.print(tbl3)


if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=10,
    )
    console.print("[bold cyan]Scanning database for opportunities...[/bold cyan]")
    data    = fetch_all(client)
    client.close()

    console.print("[dim]Computing signals...[/dim]")
    results = build_signals(data)
    print_results(results, data["fii_net"])
