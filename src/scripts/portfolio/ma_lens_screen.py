"""
src/scripts/portfolio/ma_lens_screen.py
────────────────────────────────────────
Screen any NSE symbol through a DSP Multi Asset fund manager's lens.

Gates are DERIVED FROM DSP_MULTI_ASSET's own live book in market_data.mf_holdings,
not hardcoded, so the screen adapts as the fund's positioning changes. Thresholds
are compared against the sleeve matching the symbol's OWN AMFI cap category.

Pipeline
--------
  1. Resolve symbol -> ISIN + AMFI cap category / rank / mcap
  2. Split-back-adjust the price series and flag staleness
  3. Per-fund institutional ownership (each fund at ITS OWN latest filing)
  4. Price-neutral accumulation test (implied share count, not market value)
  5. Valuation snapshot
  6. Five-gate chain + house-signal sizing modifier

Why each step exists
--------------------
  * Split adjustment: an unadjusted 5:1 split reads as a fake -80% drawdown.
  * Per-fund latest filing: AMC calendars are ragged. A single global
    max(as_of_month) collapses to the earliest-filing AMC and silently drops
    every other holder.
  * Price-neutral accumulation: a rising market value is usually just price.
    Only the implied share count distinguishes buying from a mark-up.
  * AMC normalisation: "5 AMCs" is meaningless if 85% of the money is one house.

Usage
-----
  python src/scripts/portfolio/ma_lens_screen.py NUVAMA
  python src/scripts/portfolio/ma_lens_screen.py NUVAMA GODIGIT BECTORFOOD
  python src/scripts/portfolio/ma_lens_screen.py NUVAMA --no-chart --days 365
"""

from __future__ import annotations

import sys
import argparse
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.db.pool import get_pool
from rich.console import Console
from rich.table import Table
from rich import box

console = Console(width=200)

# AMC prefixes. fund_name formats are mixed ("Mirae Asset X Fund" vs
# "DSP_MULTICAP"), so a naive split() overstates the count of independent houses.
AMC_PREFIXES = [
    "MIRAE", "INVESCO", "BAJAJ", "AXIS", "DSP", "ICICI", "HDFC", "SBI", "KOTAK",
    "NIPPON", "RELIANCE", "MOTILAL", "QUANT", "CANARA", "ABAKKUS", "HELIOS",
    "TATA", "UTI", "FRANKLIN", "ADITYA", "BIRLA", "EDELWEISS", "PGIM",
    "MAHINDRA", "BANDHAN", "SUNDARAM", "JM", "LIC", "BARODA", "UNION", "360",
    "WHITEOAK", "SAMCO", "NAVI", "ZERODHA", "GROWW", "TRUST", "ITI", "SHRIRAM",
    "QUANTUM", "TAURUS", "MOTILALOSWAL", "PPFAS", "PARAG",
]
# Passive / non-conviction sleeves. Index and ETF funds buy mechanically, so
# their "accumulation" carries no information about a manager's view.
PASSIVE_SQL = (
    "lower(fund_name) NOT LIKE '%index%' AND lower(fund_name) NOT LIKE '%etf%' "
    "AND lower(fund_name) NOT LIKE '%arbitrage%' AND lower(fund_name) NOT LIKE '%fof%'"
)
MA_FUND = "DSP_MULTI_ASSET"
STALE_FILING_DAYS = 62   # a holder this far behind the newest filing may have exited
STALE_PRICE_DAYS = 5
MAX_DAYS_TO_EXIT = 3.0   # a position must be exitable in <= this many median days


def fund_key(fund: str) -> str:
    """Normalise a fund_name for de-duplication.

    fund_name has the same spelling drift as security_name:
    BAJAJ_FINSERV_LARGE_AND_MIDCAP_FUND and BAJAJ_FINSERV_LARGE_AND_MID_CAP_FUND
    are one fund filed under two labels. Counting both inflates the holder count
    and understates ownership concentration, so G3 would pass on a phantom holder.
    """
    return "".join(ch for ch in fund.upper() if ch.isalnum())


def amc_of(fund: str) -> str:
    u = fund.upper().replace("_", " ").strip()
    for a in sorted(AMC_PREFIXES, key=len, reverse=True):
        if u.startswith(a):
            return a
    return u.split()[0] if u.split() else u


def back_adjust(df: pd.DataFrame, col: str = "close") -> tuple[pd.Series, int]:
    """Back-adjust unadjusted split/bonus gaps so history is comparable.

    A split shows up as a single-session close-to-close collapse. Everything
    before the gap is divided by the implied ratio. Returns (series, n_gaps).
    """
    gap = df[col] / df[col].shift(1)
    factor = np.ones(len(df))
    n = 0
    for i in range(len(df) - 1, 0, -1):
        g = gap.iloc[i]
        if g == g and abs((g - 1) * 100) > 25:
            factor[:i] = factor[:i] * g
            n += 1
    return df[col] * factor, n


def fmt_signed(v, f="{:+.1f}") -> str:
    if v is None or v != v:
        return "[dim]n/a[/dim]"
    s = f.format(v)
    return f"[green]{s}[/green]" if v >= 0 else f"[red]{s}[/red]"


# ── data layer ───────────────────────────────────────────────────

def load_ma_template(pool) -> dict:
    """Read DSP_MULTI_ASSET's own book and derive the position template.

    Everything the gates compare against comes from here, so the screen tracks
    the fund's real positioning instead of a snapshot baked in at authoring time.
    """
    # ClickHouse Date columns arrive as pandas Timestamps; interpolating one
    # straight back into SQL yields '2026-08-31 00:00:00', which Date rejects.
    month = pd.to_datetime(pool.query_df(
        f"SELECT max(as_of_month) AS m FROM market_data.mf_holdings FINAL "
        f"WHERE fund_name='{MA_FUND}'"
    )["m"].iloc[0]).date()

    mix = pool.query_df(f"""
        SELECT asset_type, round(sum(market_value_cr),1) AS mv_cr,
               round(sum(pct_of_nav),2) AS pct_nav, count() AS n
        FROM market_data.mf_holdings FINAL
        WHERE fund_name='{MA_FUND}' AND as_of_month='{month}'
        GROUP BY asset_type ORDER BY mv_cr DESC
    """)
    # Implied fund NAV from any sleeve: mv / (pct/100). Equity is the largest and
    # therefore the least rounding-sensitive sleeve to infer from.
    eq = mix[mix["asset_type"].str.lower() == "equity"]
    nav_cr = float(eq["mv_cr"].iloc[0] / (eq["pct_nav"].iloc[0] / 100)) if not eq.empty else np.nan

    sleeves = pool.query_df(f"""
        WITH caps AS (
            SELECT isin, rank, cap_category FROM market_data.amfi_market_cap FINAL
            WHERE period_end_date=(SELECT max(period_end_date) FROM market_data.amfi_market_cap FINAL)
        )
        SELECT caps.cap_category AS cap, count() AS n,
               round(sum(h.pct_of_nav),2) AS pct_nav,
               round(max(h.pct_of_nav),3) AS max_pos,
               round(median(h.pct_of_nav),3) AS med_pos,
               toUInt32(median(caps.rank)) AS med_rank,
               max(caps.rank) AS max_rank
        FROM (
            SELECT isin, pct_of_nav FROM market_data.mf_holdings FINAL
            WHERE fund_name='{MA_FUND}' AND as_of_month='{month}' AND lower(asset_type)='equity'
        ) h INNER JOIN caps ON h.isin=caps.isin
        GROUP BY cap
    """)
    return {"month": month, "mix": mix, "nav_cr": nav_cr,
            "sleeves": sleeves.set_index("cap") if not sleeves.empty else sleeves}


def resolve(pool, symbol: str) -> dict:
    """Resolve a ticker to ISIN + AMFI cap classification."""
    r = pool.query_df(f"""
        SELECT isin, company_name, rank, avg_mcap_cr, cap_category, period_end_date
        FROM market_data.amfi_market_cap FINAL
        WHERE nse_symbol='{symbol}'
          AND period_end_date=(SELECT max(period_end_date) FROM market_data.amfi_market_cap FINAL)
        LIMIT 1
    """)
    if not r.empty:
        x = r.iloc[0]
        return dict(isin=x["isin"], company=x["company_name"], rank=int(x["rank"]),
                    mcap=float(x["avg_mcap_cr"]), cap=x["cap_category"],
                    amfi_period=str(pd.to_datetime(x["period_end_date"]).date()),
                    resolved_via="amfi_market_cap")
    r = pool.query_df(
        f"SELECT isin, security_name FROM market_data.security_symbol_map FINAL "
        f"WHERE symbol='{symbol}' AND isin != '' LIMIT 1"
    )
    if not r.empty:
        return dict(isin=r["isin"].iloc[0], company=r["security_name"].iloc[0], rank=None,
                    mcap=np.nan, cap=None, amfi_period=None, resolved_via="security_symbol_map")
    return dict(isin="", company=None, rank=None, mcap=np.nan, cap=None,
                amfi_period=None, resolved_via="UNRESOLVED")


def price_metrics(pool, symbol: str, days: int) -> dict | None:
    px = pool.query_df(f"""
        SELECT trade_date, close, volume FROM market_data.daily_prices FINAL
        WHERE symbol='{symbol}' AND trade_date >= today() - {max(days, 420)}
        ORDER BY trade_date
    """)
    if px.empty:
        return None
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    px = px.sort_values("trade_date").reset_index(drop=True)
    px["adj"], n_adj = back_adjust(px)

    asof = px["trade_date"].max()
    last = float(px["adj"].iloc[-1])
    lag = (pd.Timestamp(date.today()) - asof).days

    def ret(n):
        p = px[px["trade_date"] <= asof - pd.Timedelta(days=n)]
        return (last / p["adj"].iloc[-1] - 1) * 100 if not p.empty else np.nan

    w = px[px["trade_date"] >= asof - pd.Timedelta(days=365)]
    hi = float(w["adj"].max())
    dret = px["adj"].pct_change().dropna()
    turn = px["close"] * px["volume"] / 1e7           # Rs crore traded per day
    return dict(
        series=px, asof=asof.date(), lag=lag, n_adj=n_adj, close=last,
        hi_52w=hi, hi_date=w.loc[w["adj"].idxmax(), "trade_date"].date(),
        lo_52w=float(w["adj"].min()), dd=(last / hi - 1) * 100,
        ret_1m=ret(30), ret_3m=ret(91), ret_6m=ret(182), ret_1y=ret(365),
        vol=float(dret.tail(250).std() * np.sqrt(252) * 100) if len(dret) >= 60 else np.nan,
        max_dd=float(((px["adj"] / px["adj"].cummax() - 1) * 100).min()),
        vs_sma200=(last / px["adj"].tail(200).mean() - 1) * 100 if len(px) >= 200 else np.nan,
        turn_med=float(turn.tail(120).median()), turn_p25=float(turn.tail(120).quantile(0.25)),
    )


def ownership(pool, isin: str) -> dict:
    """Per-fund ownership, each fund at ITS OWN latest filing.

    A single global max(as_of_month) for the ISIN would collapse to whichever
    AMC filed most recently and drop every other holder.
    """
    df = pool.query_df(f"""
        SELECT fund_name,
               argMax(as_of_month, as_of_month) AS latest_month,
               argMax(market_value_cr, as_of_month) AS val_cr,
               argMax(pct_of_nav, as_of_month) AS pct_nav
        FROM market_data.mf_holdings FINAL
        WHERE isin='{isin}' AND lower(asset_type)='equity' AND {PASSIVE_SQL}
        GROUP BY fund_name ORDER BY val_cr DESC
    """)
    if df.empty:
        return dict(df=df, n_funds=0, n_amcs=0, total_cr=0.0, top_amc=None,
                    top_amc_pct=np.nan, n_stale=0, dsp_cr=0.0, newest=None,
                    oldest=None, n_dupes=0)
    df["latest_month"] = pd.to_datetime(df["latest_month"])
    # Collapse spelling variants of one fund, keeping its most recent filing.
    df["key"] = df["fund_name"].map(fund_key)
    before = len(df)
    df = (df.sort_values("latest_month")
            .groupby("key", as_index=False).tail(1)
            .sort_values("val_cr", ascending=False)
            .reset_index(drop=True))
    n_dupes = before - len(df)
    df["amc"] = df["fund_name"].map(amc_of)
    newest = df["latest_month"].max()
    df["is_current"] = (newest - df["latest_month"]).dt.days <= STALE_FILING_DAYS
    cur = df[df["is_current"]]
    by_amc = cur.groupby("amc")["val_cr"].sum()
    return dict(
        df=df, n_funds=int(cur["fund_name"].nunique()), n_amcs=int(cur["amc"].nunique()),
        total_cr=float(cur["val_cr"].sum()),
        top_amc=by_amc.idxmax() if len(by_amc) and by_amc.sum() > 0 else None,
        top_amc_pct=float(by_amc.max() / by_amc.sum() * 100) if by_amc.sum() > 0 else np.nan,
        n_stale=int((~df["is_current"]).sum()),
        dsp_cr=float(cur[cur["amc"] == "DSP"]["val_cr"].sum()),
        newest=newest, oldest=df["latest_month"].min(), n_dupes=n_dupes,
    )


def accumulation(pool, isin: str, px: pd.DataFrame, months: int = 4) -> pd.DataFrame:
    """Price-neutral accumulation test.

    Converts each fund's reported market value into an implied share count using
    the split-adjusted month-end close. A rising market value is usually just
    price appreciation; only the share count separates buying from a mark-up.
    """
    h = pool.query_df(f"""
        SELECT fund_name, as_of_month, market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE isin='{isin}' AND lower(asset_type)='equity' AND {PASSIVE_SQL}
        ORDER BY fund_name, as_of_month
    """)
    if h.empty or px is None or px.empty:
        return pd.DataFrame()
    me = px.copy()
    me["m"] = me["trade_date"].values.astype("datetime64[M]")
    me = me.groupby("m").tail(1)[["m", "adj"]].rename(columns={"adj": "m_close"})

    h["as_of_month"] = pd.to_datetime(h["as_of_month"])
    h["m"] = h["as_of_month"].values.astype("datetime64[M]")
    h = h.merge(me, on="m", how="left")
    h["shares_lk"] = h["market_value_cr"] * 1e7 / h["m_close"] / 1e5

    h["key"] = h["fund_name"].map(fund_key)
    out = []
    for _, g in h.groupby("key"):
        fund = g.sort_values("as_of_month")["fund_name"].iloc[-1]
        g = g.sort_values("as_of_month").drop_duplicates("as_of_month", keep="last")
        g = g.tail(months).reset_index(drop=True)
        g["sh_mom"] = g["shares_lk"].pct_change() * 100
        first, lastr = g.iloc[0], g.iloc[-1]
        span = ((lastr["shares_lk"] / first["shares_lk"] - 1) * 100
                if len(g) > 1 and first["shares_lk"] and first["shares_lk"] == first["shares_lk"]
                else np.nan)
        read = ("n/a" if span != span else "ADDING" if span > 2
                else "TRIMMING" if span < -2 else "HOLDING")
        out.append(dict(fund=fund, amc=amc_of(fund), months=len(g),
                        first_month=first["as_of_month"].date(),
                        last_month=lastr["as_of_month"].date(),
                        shares_first=first["shares_lk"], shares_last=lastr["shares_lk"],
                        val_first=first["market_value_cr"], val_last=lastr["market_value_cr"],
                        px_first=first["m_close"], px_last=lastr["m_close"],
                        share_chg=span, read=read))
    return pd.DataFrame(out).sort_values("val_last", ascending=False)


def valuation(pool, symbol: str) -> dict:
    v = pool.query_df(f"""
        SELECT argMax(trailing_pe, snapshot_date) AS pe,
               argMax(price_to_book, snapshot_date) AS pb,
               argMax(snapshot_date, snapshot_date) AS snap
        FROM market_data.stock_valuation FINAL WHERE symbol='{symbol}'
    """)
    if v.empty or v["snap"].isna().all():
        return dict(pe=np.nan, pb=np.nan, snap=None)
    x = v.iloc[0]
    pe = float(x["pe"]) if x["pe"] == x["pe"] and x["pe"] else np.nan
    pb = float(x["pb"]) if x["pb"] == x["pb"] and x["pb"] else np.nan
    return dict(pe=pe, pb=pb, snap=str(x["snap"])[:10])


# ── gate chain ──────────────────────────────────────────────────

def build_gates(tpl: dict, cap: str | None) -> dict:
    """Derive gate thresholds from the fund's own sleeve for this cap category."""
    s = None
    if cap and len(tpl["sleeves"]) and cap in tpl["sleeves"].index:
        s = tpl["sleeves"].loc[cap]
    med_pos = float(s["med_pos"]) if s is not None else 0.445
    max_rank = int(s["max_rank"]) if s is not None else 550
    target_cr = med_pos / 100 * tpl["nav_cr"] if tpl["nav_cr"] == tpl["nav_cr"] else np.nan
    # Liquidity floor: the template-sized position must clear in <= MAX_DAYS_TO_EXIT
    # days of median turnover.
    liq_floor = target_cr / MAX_DAYS_TO_EXIT if target_cr == target_cr else 25.0
    return dict(med_pos=med_pos, max_pos=float(s["max_pos"]) if s is not None else 0.72,
                max_rank=max_rank, target_cr=target_cr, liq_floor=liq_floor,
                sleeve_pct=float(s["pct_nav"]) if s is not None else np.nan,
                sleeve_n=int(s["n"]) if s is not None else 0)


def score(pm: dict, own: dict, val: dict, res: dict, g: dict) -> list[dict]:
    rank = res["rank"]
    return [
        dict(name=f"G1 Liquidity >= Rs {g['liq_floor']:.0f} Cr/d",
             ok=pm["turn_med"] >= g["liq_floor"],
             got=f"{pm['turn_med']:.1f} Cr/d (p25 {pm['turn_p25']:.1f})",
             why=f"a Rs {g['target_cr']:.0f} Cr position must exit in <= {MAX_DAYS_TO_EXIT:.0f} median days"),
        dict(name=f"G2 AMFI rank <= {g['max_rank']}",
             ok=rank is not None and rank <= g["max_rank"],
             got=str(rank) if rank is not None else "unclassified",
             why=f"deepest rank the fund actually owns in this sleeve is {g['max_rank']}"),
        dict(name="G3 >=3 funds, >=3 AMCs, top AMC <60%",
             ok=(own["n_funds"] >= 3 and own["n_amcs"] >= 3
                 and own["top_amc_pct"] == own["top_amc_pct"] and own["top_amc_pct"] < 60),
             got=f"{own['n_funds']}f / {own['n_amcs']}a / top "
                 + (f"{own['top_amc']} {own['top_amc_pct']:.0f}%" if own["top_amc"] else "n/a"),
             why="independent corroboration, not one house's thesis"),
        dict(name="G4 Trailing PE <= 50",
             ok=val["pe"] == val["pe"] and 0 < val["pe"] <= 50,
             got=f"{val['pe']:.1f}" if val["pe"] == val["pe"] else "NO DATA (unscoreable)",
             why="no substituting a remembered number for a missing one"),
        dict(name="G5 Drawdown from 52w high <= -10%",
             ok=pm["dd"] <= -10,
             got=f"{pm['dd']:+.1f}% (high {pm['hi_date']})",
             why="buying a correction, not chasing a high"),
    ]


def house_signal(acc: pd.DataFrame) -> tuple[str, str]:
    """What DSP's own active funds are doing, price-neutral."""
    if acc.empty:
        return "NO DSP POSITION", "no DSP active fund holds this name"
    dsp = acc[acc["amc"] == "DSP"]
    if dsp.empty:
        return "NO DSP POSITION", "no DSP active fund holds this name"
    reads = set(dsp["read"])
    funds = ", ".join(f"{r['fund']} {r['read']} ({r['share_chg']:+.1f}% shares)"
                      for _, r in dsp.iterrows() if r["read"] != "n/a")
    if "ADDING" in reads:
        return "DSP ADDING", funds
    if reads == {"TRIMMING"} or ("TRIMMING" in reads and "HOLDING" not in reads):
        return "DSP DISTRIBUTING", funds
    return "DSP HOLDING", funds


def verdict(n_pass: int, signal: str) -> tuple[str, str]:
    if n_pass == 5 and signal == "DSP ADDING":
        return "BUY", "clears every gate and DSP is adding - size toward the template median"
    if n_pass == 5:
        return "STARTER", ("clears every gate but DSP is not adding - size at the template "
                           "FLOOR, by elimination rather than endorsement")
    if n_pass == 4:
        return "WATCH", "one gate short - revisit if that gate flips"
    return "REJECT", "fails the manager's structural constraints"


# ── render ────────────────────────────────────────────────────

def render_template(tpl: dict) -> None:
    console.rule(f"[bold cyan]DSP MULTI ASSET BOOK @ {tpl['month']} — gates derive from this[/bold cyan]")
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan")
    for h in ["Asset Type", "MV Cr", "% NAV", "Positions"]:
        t.add_column(h, justify="right")
    for _, r in tpl["mix"].iterrows():
        t.add_row(f"[bold]{r['asset_type']}[/bold]", f"{r['mv_cr']:,.1f}",
                  f"{r['pct_nav']:.2f}", str(r["n"]))
    console.print(t)
    console.print(f"  Implied fund NAV: [bold]Rs {tpl['nav_cr']:,.0f} Cr[/bold]\n")
    if len(tpl["sleeves"]):
        t = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan",
                  title="[bold]Equity sleeve by AMFI cap bucket - the position template[/bold]")
        for h in ["Cap", "Names", "% NAV", "Max Pos %", "Median Pos %", "Median Rank", "Deepest Rank"]:
            t.add_column(h, justify="right")
        for cap, r in tpl["sleeves"].iterrows():
            t.add_row(f"[bold]{cap}[/bold]", str(r["n"]), f"{r['pct_nav']:.2f}",
                      f"{r['max_pos']:.3f}", f"{r['med_pos']:.3f}",
                      str(r["med_rank"]), str(r["max_rank"]))
        console.print(t)


def render_symbol(symbol, res, pm, own, acc, val, g, gates, sig, sig_detail, verd, verd_why):
    console.print()
    console.rule(f"[bold magenta]{symbol} — {res['company'] or 'unresolved'}[/bold magenta]")

    if res["resolved_via"] == "UNRESOLVED":
        console.print(f"[bold red]⚠️  [UNRESOLVED_SECURITY: symbol={symbol}][/bold red] no ISIN in "
                      "amfi_market_cap or security_symbol_map. Ownership NOT checked — "
                      "this is a lookup gap, not an absence of holders.")
    if pm is None:
        console.print(f"[bold red]❌ No rows in market_data.daily_prices for '{symbol}'.[/bold red] "
                      "Import it first: ./mosaic.sh import --category stocks --source nse")
        return
    if pm["lag"] > STALE_PRICE_DAYS:
        console.print(f"[bold yellow]⚠️  STALE PRICE: latest bar {pm['asof']} ({pm['lag']}d old). "
                      "Every price figure below is as-of that date, not today.[/bold yellow]")
    if pm["n_adj"]:
        console.print(f"[bold yellow]⚠️  {pm['n_adj']} unadjusted split/bonus gap(s) detected and "
                      "back-adjusted. Raw series would show a fake drawdown.[/bold yellow]")

    t = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan", title="[bold]Price & risk (split-adjusted)[/bold]")
    for h in ["Close", "As Of", "Cap", "Rank", "MCap Cr", "DD 52wHi", "HiDate", "vs SMA200",
              "1M", "3M", "6M", "1Y", "AnnVol", "MaxDD", "Turn Cr/d", "p25"]:
        t.add_column(h, justify="right")
    t.add_row(f"{pm['close']:,.1f}", str(pm["asof"]), res["cap"] or "[dim]n/a[/dim]",
              str(res["rank"]) if res["rank"] else "[dim]n/a[/dim]",
              f"{res['mcap']:,.0f}" if res["mcap"] == res["mcap"] else "[dim]n/a[/dim]",
              fmt_signed(pm["dd"]), str(pm["hi_date"]), fmt_signed(pm["vs_sma200"]),
              fmt_signed(pm["ret_1m"]), fmt_signed(pm["ret_3m"]), fmt_signed(pm["ret_6m"]),
              fmt_signed(pm["ret_1y"]), f"[yellow]{pm['vol']:.0f}[/yellow]",
              fmt_signed(pm["max_dd"]), f"{pm['turn_med']:,.1f}", f"{pm['turn_p25']:,.1f}")
    console.print(t)

    t = Table(box=box.HEAVY_HEAD, header_style="bold cyan", title="[bold]Gate chain[/bold]")
    t.add_column("Gate"); t.add_column("Result", justify="center")
    t.add_column("Measured", justify="right"); t.add_column("Rationale", style="dim")
    for gt in gates:
        t.add_row(gt["name"], "[bold green]PASS[/bold green]" if gt["ok"] else "[bold red]FAIL[/bold red]",
                  gt["got"], gt["why"])
    console.print(t)

    if own["n_funds"] or own["n_stale"]:
        d = own["df"].copy()
        d["flag"] = np.where(~d["is_current"], "<- STALE/possible exit", "")
        d["latest_month"] = d["latest_month"].dt.date
        t = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan",
                  title=f"[bold]Active-fund ownership — {own['n_funds']} funds / {own['n_amcs']} AMCs / "
                        f"Rs {own['total_cr']:,.1f} Cr[/bold]")
        for h in ["Fund", "AMC", "Filed", "MV Cr", "% NAV", ""]:
            t.add_column(h, justify="right")
        for _, r in d.head(15).iterrows():
            t.add_row(r["fund_name"][:46], r["amc"], str(r["latest_month"]),
                      f"{r['val_cr']:,.2f}", f"{r['pct_nav']:.2f}",
                      f"[yellow]{r['flag']}[/yellow]")
        console.print(t)
        if own["oldest"] != own["newest"]:
            console.print("[yellow]⚠️  RAGGED DISCLOSURE: filing months span "
                          f"{own['oldest'].date()} .. {own['newest'].date()} — the total is "
                          "NOT a single-date figure.[/yellow]")
        if own.get("n_dupes"):
            console.print(f"[yellow]⚠️  {own['n_dupes']} duplicate fund_name spelling(s) collapsed "
                          "(e.g. MIDCAP vs MID_CAP) — counting both would inflate breadth.[/yellow]")
        if own["top_amc_pct"] == own["top_amc_pct"] and own["top_amc_pct"] >= 60:
            console.print(f"[red]⚠️  CONCENTRATED OWNERSHIP: {own['top_amc']} is "
                          f"{own['top_amc_pct']:.0f}% of institutional money — "
                          f"'{own['n_amcs']} AMCs' overstates independence.[/red]")

    if not acc.empty:
        t = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan",
                  title="[bold]Price-neutral accumulation — implied share count, not market value[/bold]")
        for h in ["Fund", "Window", "Px first→last", "MV Cr first→last",
                  "Shares lk first→last", "Shares Δ%", "Read"]:
            t.add_column(h, justify="right")
        for _, r in acc.head(10).iterrows():
            colour = {"ADDING": "bold green", "TRIMMING": "bold red",
                      "HOLDING": "yellow", "n/a": "dim"}[r["read"]]
            t.add_row(r["fund"][:40], f"{r['first_month']} → {r['last_month']}",
                      f"{r['px_first']:,.0f} → {r['px_last']:,.0f}",
                      f"{r['val_first']:,.1f} → {r['val_last']:,.1f}",
                      f"{r['shares_first']:,.2f} → {r['shares_last']:,.2f}",
                      fmt_signed(r["share_chg"]), f"[{colour}]{r['read']}[/{colour}]")
        console.print(t)

    n_pass = sum(1 for gt in gates if gt["ok"])
    vcol = {"BUY": "bold green", "STARTER": "green", "WATCH": "yellow", "REJECT": "bold red"}[verd]
    if verd == "REJECT":
        size = "n/a - not investable under this lens"
    elif g["target_cr"] == g["target_cr"]:
        floor = g["target_cr"] / 2
        size = (f"Rs {floor:.0f}-{g['target_cr']:.0f} Cr "
                f"({g['med_pos'] / 2:.3f}-{g['med_pos']:.3f}% NAV, sleeve cap {g['max_pos']:.3f}%)")
        if verd == "STARTER":
            size = f"floor only: Rs {floor:.0f} Cr ({g['med_pos'] / 2:.3f}% NAV)"
    else:
        size = "n/a"
    console.print(f"""
┌─ VERDICT ───────────────────────────────────────────────────────────────────
│  {symbol}: [{vcol}]{verd}[/{vcol}]  ({n_pass}/5 gates)
│  {verd_why}
│
│  House signal : [bold]{sig}[/bold]
│                 {sig_detail or 'n/a'}
│  If taken     : {size}
└────────────────────────────────────────────────────────────────────""")

    console.print(f"""[dim]Provenance: cap/ISIN market_data.amfi_market_cap FINAL period={res['amfi_period']} (Tier-1 AMFI)
            prices    market_data.daily_prices FINAL as-of {pm['asof']} (NSE/Shoonya)
            holdings  market_data.mf_holdings FINAL per-fund latest filing (Tier-1 SEBI)
            valuation market_data.stock_valuation FINAL snapshot={val['snap']} (Tier-2 vendor)
            resolved via {res['resolved_via']}[/dim]""")


def chart(pm: dict, symbol: str, n_pass: int) -> None:
    try:
        import plotext as plt
    except ImportError:
        return
    g = pm["series"]
    g = g[g["trade_date"] >= g["trade_date"].max() - pd.Timedelta(days=365)]
    if g.empty:
        return
    plt.clear_figure(); plt.theme("pro"); plt.plot_size(112, 15)
    plt.plot(list(range(len(g))), g["adj"].round(1).tolist(), marker="braille", color="cyan")
    plt.horizontal_line(float(g["adj"].max()), color="red")
    sma = float(g["adj"].tail(200).mean())
    plt.horizontal_line(sma, color="orange")
    plt.title(f"{symbol} [{n_pass}/5]  52wHi {g['adj'].max():,.0f} (red) | "
              f"SMA200 {sma:,.0f} (orange) | last {g['adj'].iloc[-1]:,.0f}")
    plt.xlabel(f"{g['trade_date'].iloc[0].date()} -> {g['trade_date'].iloc[-1].date()}")
    plt.show()
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Screen symbols through the DSP Multi Asset lens.")
    ap.add_argument("symbols", nargs="+", help="NSE ticker(s), e.g. NUVAMA GODIGIT")
    ap.add_argument("--days", type=int, default=420, help="price history window (default 420)")
    ap.add_argument("--no-chart", action="store_true")
    ap.add_argument("--months", type=int, default=4, help="accumulation lookback months (default 4)")
    a = ap.parse_args()

    pool = get_pool()
    tpl = load_ma_template(pool)
    render_template(tpl)

    summary = []
    for sym in [s.upper().strip() for s in a.symbols]:
        res = resolve(pool, sym)
        pm = price_metrics(pool, sym, a.days)
        own = ownership(pool, res["isin"]) if res["isin"] else ownership(pool, "__none__")
        acc = accumulation(pool, res["isin"], pm["series"] if pm else None, a.months) \
            if res["isin"] else pd.DataFrame()
        val = valuation(pool, sym)
        g = build_gates(tpl, res["cap"])
        if pm is None:
            render_symbol(sym, res, pm, own, acc, val, g, [], "", "", "REJECT", "no price data")
            summary.append((sym, 0, "NO DATA", "-"))
            continue
        gates = score(pm, own, val, res, g)
        n_pass = sum(1 for x in gates if x["ok"])
        sig, detail = house_signal(acc)
        verd, why = verdict(n_pass, sig)
        render_symbol(sym, res, pm, own, acc, val, g, gates, sig, detail, verd, why)
        if not a.no_chart:
            chart(pm, sym, n_pass)
        summary.append((sym, n_pass, verd, sig))

    if len(summary) > 1:
        console.rule("[bold cyan]SUMMARY[/bold cyan]")
        t = Table(box=box.HEAVY_HEAD, header_style="bold cyan")
        for h in ["Symbol", "Gates", "Verdict", "House Signal"]:
            t.add_column(h, justify="right")
        for sym, n, v, s in sorted(summary, key=lambda x: -x[1]):
            vcol = {"BUY": "bold green", "STARTER": "green", "WATCH": "yellow",
                    "REJECT": "bold red"}.get(v, "dim")
            t.add_row(f"[bold]{sym}[/bold]", f"{n}/5", f"[{vcol}]{v}[/{vcol}]", s)
        console.print(t)


if __name__ == "__main__":
    main()
