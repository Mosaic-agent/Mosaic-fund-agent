"""
scripts/portfolio/quant_vlrt_simulator.py  (v2 — correct VLRT mechanics)
──────────────────────────────────────────────────────────────────────────
Simulates Quant MF's VLRT™ asset-allocation model based on Sandeep Tandon's
own description (Ankit Rathi / Medium, Jun 2025) plus reverse-engineering
from actual monthly holdings disclosures.

Correct VLRT Mechanics (from the source)
─────────────────────────────────────────
V — Valuation       Company fundamentals, historical P&L, cash flows.
                    Proxy: Nifty z-score vs 3-year rolling mean.

L — Liquidity       "Most underrated driver." Dries up at tops AND bottoms.
                    Proxies: USDINR trend (INR strong = inflows), FII net
                    (when available), Gold momentum (surging gold =
                    global liquidity tightening).

R — Risk Appetite   NOT volatility — behavioral signal. Uses high-freq data
                    and crypto/sentiment proxies (per Tandon interviews).
                    Risk-ON → small/mid/illiquid. Risk-OFF → Reliance/ONGC/L&T.
                    Proxy: Gold/Equity relative perf, USDINR stress level,
                           Small-cap vs Large-cap breadth (JUNIORBEES/NIFTYBEES).

T — Timing          Synthesised from V+L+R — NOT a separate input.
                    "Helps manage risk by combining signals from V, L, R."
                    Proxy: alignment score across the three signals.

Key mechanic — DYNAMIC WEIGHTS (from Tandon):
  March 2020 crisis: L + R = 90%, V = 10%.
  Normal markets: V=30%, L=30%, R=25%, T=15%.
  stress_level drives the weight shift continuously.

Gold allocation (critical fix vs v1):
  Gold = primary risk-off hedge, NOT a valuation call.
  Floor 9–11%. Tactical surge to 20–38% when R low + L contracting.

Usage:
    python src/scripts/portfolio/quant_vlrt_simulator.py
    python src/scripts/portfolio/quant_vlrt_simulator.py --fund QUANT_DYNAMIC_ASSET_ALLOCATION
    python src/scripts/portfolio/quant_vlrt_simulator.py --no-compare
"""
from __future__ import annotations

import argparse
import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.getcwd())

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.db.pool import get_pool

console = Console()

# ── Regime bands (VLRT composite 0–100) ──────────────────────────────────────

REGIME_BANDS = [
    (75, 101, "RISK-ON MAX",  "green bold"),
    (60,  75, "RISK-ON",      "green"),
    (45,  60, "NEUTRAL",      "yellow"),
    (30,  45, "RISK-OFF",     "dark_orange"),
    ( 0,  30, "FULL DEFENSE", "red"),
]

# Base allocation per regime: (equity%, gold%, bond%)
# Gold surges in risk-off — critical distinction from v1
REGIME_ALLOC = {
    "RISK-ON MAX":  (68.0,  9.0, 15.0),
    "RISK-ON":      (58.0, 10.0, 22.0),
    "NEUTRAL":      (48.0, 12.0, 30.0),
    "RISK-OFF":     (38.0, 18.0, 36.0),
    "FULL DEFENSE": (32.0, 25.0, 38.0),
}


def regime_label(score: float) -> str:
    for lo, hi, label, _ in REGIME_BANDS:
        if lo <= score < hi:
            return label
    return "FULL DEFENSE"


def regime_color(label: str) -> str:
    return next(c for _, _, l, c in REGIME_BANDS if l == label)


# ── Data loaders ─────────────────────────────────────────────────────────────

def load_daily_prices(start: str = "2021-07-01") -> pd.DataFrame:
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT trade_date, symbol, open, high, low, close, volume
        FROM market_data.daily_prices FINAL
        WHERE symbol IN ('NIFTYBEES', 'GOLDBEES', 'JUNIORBEES')
          AND trade_date >= '{start}'
        ORDER BY symbol, trade_date
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_usdinr(start: str = "2021-07-01") -> pd.DataFrame:
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT trade_date, close AS usdinr
        FROM market_data.fx_rates FINAL
        WHERE symbol = 'USDINR' AND trade_date >= '{start}'
        ORDER BY trade_date
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_fii_flows() -> pd.DataFrame:
    pool = get_pool()
    df = pool.query_df("""
        SELECT trade_date, fii_net_cr, dii_net_cr
        FROM market_data.fii_dii_flows FINAL
        ORDER BY trade_date
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def load_actual_alloc(fund_name: str) -> pd.DataFrame:
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT
            as_of_month,
            round(sumIf(pct_of_nav, asset_type = 'equity'), 1) AS equity,
            round(sumIf(pct_of_nav, asset_type = 'gold'),   1) AS gold,
            round(sumIf(pct_of_nav, asset_type = 'bond'),   1) AS bond
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
        GROUP BY as_of_month
        ORDER BY as_of_month
    """)
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"] = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates("ym", keep="last")
    return df.drop("ym", axis=1).reset_index(drop=True)


# ── VLRT v2: Signal Computation ───────────────────────────────────────────────

def compute_vlrt_monthly(
    prices_df: pd.DataFrame,
    usdinr_df: pd.DataFrame,
    fii_df: pd.DataFrame,
) -> pd.DataFrame:

    nifty  = prices_df[prices_df["symbol"] == "NIFTYBEES"].copy().sort_values("trade_date").reset_index(drop=True)
    gold   = prices_df[prices_df["symbol"] == "GOLDBEES"].copy().sort_values("trade_date").reset_index(drop=True)
    junior = prices_df[prices_df["symbol"] == "JUNIORBEES"].copy().sort_values("trade_date").reset_index(drop=True)

    # Daily derived signals
    nifty["ret"]       = nifty["close"].pct_change()
    nifty["ret1m"]     = nifty["close"].pct_change(21) * 100
    nifty["ret3m"]     = nifty["close"].pct_change(63) * 100
    nifty["sma50"]     = nifty["close"].rolling(50).mean()
    nifty["sma200"]    = nifty["close"].rolling(200).mean()
    nifty["sma36m"]    = nifty["close"].rolling(756).mean()   # 3-year rolling mean
    nifty["std36m"]    = nifty["close"].rolling(756).std()
    # V proxy: how far above 3yr mean (positive = expensive)
    nifty["v_zscore"]  = (nifty["close"] - nifty["sma36m"]) / nifty["std36m"].clip(lower=1)
    nifty["rol30v"]    = nifty["ret"].rolling(30).std() * math.sqrt(252) * 100
    nifty["hi52w"]     = nifty["close"].rolling(252, min_periods=20).max()
    nifty["drawdown"]  = (nifty["close"] - nifty["hi52w"]) / nifty["hi52w"] * 100
    nifty["vol6m_avg"] = nifty["volume"].rolling(126).mean()
    nifty["vol_surge"] = nifty["volume"] / nifty["vol6m_avg"].clip(lower=1)

    gold["ret3m"]  = gold["close"].pct_change(63) * 100
    gold["ret1m"]  = gold["close"].pct_change(21) * 100

    junior["ret3m"] = junior["close"].pct_change(63) * 100

    # Merge gold and junior onto nifty daily
    daily = nifty.merge(
        gold[["trade_date", "ret3m", "ret1m"]].rename(
            columns={"ret3m": "gold_ret3m", "ret1m": "gold_ret1m"}
        ),
        on="trade_date", how="left",
    ).merge(
        junior[["trade_date", "ret3m"]].rename(columns={"ret3m": "jr_ret3m"}),
        on="trade_date", how="left",
    )

    # 3M gold/equity relative: gold > equity = risk-off globally
    daily["gold_eq_rel"] = daily["gold_ret3m"] - daily["ret3m"]
    # Small-cap breadth: junior > nifty = risk-on; lagging = risk-off
    daily["sc_breadth"]  = daily["jr_ret3m"] - daily["ret3m"]

    # USDINR: INR weakening (USDINR rising) = liquidity tightening, risk-off
    usdinr_df = usdinr_df.sort_values("trade_date").reset_index(drop=True)
    usdinr_df["usd_sma3m"] = usdinr_df["usdinr"].rolling(63).mean()
    usdinr_df["usd_dev"]   = (usdinr_df["usdinr"] - usdinr_df["usd_sma3m"]) / usdinr_df["usd_sma3m"].clip(lower=1) * 100

    daily = daily.merge(usdinr_df[["trade_date", "usd_dev"]], on="trade_date", how="left")
    daily["usd_dev"] = daily["usd_dev"].fillna(0.0)

    # FII monthly z-score
    fii_monthly = pd.DataFrame(columns=["ym", "fii_zscore"])
    if not fii_df.empty:
        fii_df["ym"] = fii_df["trade_date"].dt.to_period("M")
        fm = fii_df.groupby("ym")[["fii_net_cr"]].sum().reset_index()
        rm = fm["fii_net_cr"].rolling(6, min_periods=1).mean()
        rs = fm["fii_net_cr"].rolling(6, min_periods=1).std().clip(lower=1)
        fm["fii_zscore"] = (fm["fii_net_cr"] - rm) / rs
        fii_monthly = fm[["ym", "fii_zscore"]]

    # Resample to month-end
    daily["ym"] = daily["trade_date"].dt.to_period("M")
    n_m = daily.sort_values("trade_date").groupby("ym").last().reset_index()
    if not fii_monthly.empty:
        n_m = n_m.merge(fii_monthly, on="ym", how="left")
    else:
        n_m["fii_zscore"] = 0.0
    n_m["fii_zscore"] = n_m["fii_zscore"].fillna(0.0)

    # ── V (Valuation): expensive = low V ─────────────────────────────────────
    # z-score vs 3yr mean: +3 → V=0 (very expensive); -3 → V=25 (deep value)
    def v_score(row) -> float:
        z = float(row.get("v_zscore") or 0)
        if pd.isna(z):
            return 12.5
        return round(float(np.clip((3 - z) / 6 * 25, 0, 25)), 2)

    n_m["V"] = n_m.apply(v_score, axis=1)

    # ── L (Liquidity): strong INR + no gold surge = good liquidity ────────────
    def l_score(row) -> float:
        usd_dev  = float(row.get("usd_dev")  or 0)   # +ve = INR weakening = bad
        fii_z    = float(row.get("fii_zscore") or 0)
        gold_rel = float(row.get("gold_eq_rel") or 0) # gold > equity = tightening
        vol_s    = float(row.get("vol_surge") or 1)

        # INR strength component (max 15 pts): usd_dev < 0 = INR strong = full marks
        inr_pts  = float(np.clip((3 - usd_dev) / 5 * 15, 0, 15))
        # FII component (max 7.5 pts — data only from Oct 2025)
        fii_pts  = float(np.clip((fii_z + 1) / 2 * 7.5, 0, 7.5))
        # Gold surge drag: gold > equity by >15% = -5 pts (global L tightening)
        gold_drag = float(np.clip(gold_rel / 20 * 5, 0, 5))
        # Volume bonus (max 2.5 pts)
        vol_pts  = float(np.clip((vol_s - 0.8) / 0.5 * 2.5, 0, 2.5))

        return round(float(np.clip(inr_pts + fii_pts - gold_drag + vol_pts, 0, 25)), 2)

    n_m["L"] = n_m.apply(l_score, axis=1)

    # ── R (Risk Appetite): behavioral, NOT volatility ─────────────────────────
    # High R = risk-on: small-caps leading, gold lagging, market near ATH
    # Low R  = risk-off: gold > equity, small-caps trailing, INR spiking
    def r_score(row) -> float:
        gold_rel   = float(row.get("gold_eq_rel") or 0)  # gold > equity = risk-off
        sc_breadth = float(row.get("sc_breadth") or 0)   # small > large = risk-on
        drawdown   = float(row.get("drawdown") or 0)      # near ATH = risk-on

        # Gold/equity divergence: gold > equity by 10% = R near 0
        gold_pts = float(np.clip((10 - gold_rel) / 20 * 15, 0, 15))
        # Small-cap breadth: jr > nifty by 5% = 10 pts (risk appetite high)
        sc_pts   = float(np.clip((sc_breadth + 5) / 15 * 10, 0, 10))
        # Drawdown penalty: deep below ATH reduces risk appetite
        dd_pen   = float(np.clip(abs(drawdown) / 15 * 5, 0, 5))

        return round(float(np.clip(gold_pts + sc_pts - dd_pen, 0, 25)), 2)

    n_m["R"] = n_m.apply(r_score, axis=1)

    # ── Phase detection (crisis OR momentum) — both shrink V weight ───────────
    # Tandon: "V = 10% in March 2020." We extend: V shrinks in momentum too.
    # crisis_stress: gold surging + INR falling + drawdown (Tandon's explicit example)
    # momentum_phase: L+R both strong → Quant plays offense, V matters less
    def crisis_stress(row) -> float:
        gold_rel = float(row.get("gold_eq_rel") or 0)
        usd_dev  = float(row.get("usd_dev") or 0)
        drawdown = float(row.get("drawdown") or 0)
        return round(
            float(np.clip(gold_rel / 20, 0, 1)) * 0.4 +
            float(np.clip(usd_dev / 3, 0, 1))  * 0.3 +
            float(np.clip(abs(drawdown) / 20, 0, 1)) * 0.3,
            3,
        )

    def momentum_phase(row) -> float:
        # High when BOTH L and R are strong — Quant chases momentum, not value
        l_n = row["L"] / 25
        r_n = row["R"] / 25
        return round(float(np.clip((l_n + r_n - 0.8) / 1.2, 0, 0.7)), 3)

    n_m["stress"]   = n_m.apply(crisis_stress, axis=1)
    n_m["momentum"] = n_m.apply(momentum_phase, axis=1)
    # Combined phase: max of crisis or momentum drives V weight down
    n_m["phase"] = n_m[["stress", "momentum"]].max(axis=1)

    # ── T (Timing = synthesis of V+L+R) ──────────────────────────────────────
    def t_score(row) -> float:
        v_n   = row["V"] / 25
        l_n   = row["L"] / 25
        r_n   = row["R"] / 25
        phase = row["phase"]
        # In high phase (crisis OR momentum), L+R dominate timing
        if phase > 0.5:
            weighted_avg = l_n * 0.5 + r_n * 0.5
        else:
            weighted_avg = v_n * 0.3 + l_n * 0.4 + r_n * 0.3
        alignment = 1 - float(np.std([v_n, l_n, r_n]))
        return round(float(np.clip(weighted_avg * alignment * 25, 0, 25)), 2)

    n_m["T"] = n_m.apply(t_score, axis=1)

    # ── VLRT composite with phase-adjusted dynamic weights ────────────────────
    def vlrt_composite(row) -> float:
        p    = row["phase"]
        # V weight: 30% in neutral, shrinks to 5% in max phase (crisis or momentum)
        w_v  = float(np.clip(0.30 - p * 0.25, 0.05, 0.30))
        w_l  = float(np.clip(0.30 + p * 0.20, 0.30, 0.50))
        w_r  = float(np.clip(0.25 + p * 0.20, 0.25, 0.45))
        w_t  = max(1.0 - w_v - w_l - w_r, 0.0)
        return round(row["V"] * w_v + row["L"] * w_l + row["R"] * w_r + row["T"] * w_t, 2)

    n_m["VLRT_raw"] = n_m.apply(vlrt_composite, axis=1)
    n_m["VLRT"] = (n_m["VLRT_raw"] / 25 * 100).round(1).clip(0, 100)
    n_m["regime"] = n_m["VLRT"].apply(regime_label)

    # ── Implied allocation: gold surges ONLY in crisis risk-off ───────────────
    def implied_alloc(row) -> tuple[float, float, float]:
        base_eq, base_gold, base_bond = REGIME_ALLOC[row["regime"]]
        r_norm   = row["R"] / 25
        gold_rel = float(row.get("gold_eq_rel") or 0)
        # Gold surge ONLY when crisis risk-off: R low AND gold actually outperforming
        if r_norm < 0.3 and gold_rel > 5 and row["stress"] > 0.2:
            surge     = min(gold_rel * 0.7, 18)
            base_gold = min(base_gold + surge, 40)
            base_eq   = max(base_eq - surge * 0.8, 28)
            base_bond = max(100 - base_eq - base_gold - 5, 5)
        return round(base_eq, 1), round(base_gold, 1), round(base_bond, 1)

    alloc = n_m.apply(implied_alloc, axis=1, result_type="expand")
    n_m["impl_eq"]   = alloc[0]
    n_m["impl_gold"] = alloc[1]
    n_m["impl_bond"] = alloc[2]

    return n_m[[
        "ym", "close", "V", "L", "R", "T", "VLRT", "stress", "momentum", "regime",
        "impl_eq", "impl_gold", "impl_bond",
        "v_zscore", "rol30v", "drawdown", "gold_eq_rel", "sc_breadth",
        "usd_dev", "fii_zscore",
    ]]


# ── Accuracy ─────────────────────────────────────────────────────────────────

def compute_accuracy(monthly: pd.DataFrame, actual: pd.DataFrame) -> pd.DataFrame:
    actual["ym"] = actual["as_of_month"].dt.to_period("M")
    merged = monthly.merge(actual[["ym", "equity", "gold", "bond"]], on="ym", how="inner")
    merged["eq_err"]   = (merged["impl_eq"]   - merged["equity"]).round(1)
    merged["gold_err"] = (merged["impl_gold"] - merged["gold"]).round(1)
    merged["bond_err"] = (merged["impl_bond"] - merged["bond"]).round(1)
    merged["actual_eq_delta"] = merged["equity"].diff()
    merged["model_eq_delta"]  = merged["impl_eq"].diff()
    merged["dir_correct"] = (merged["actual_eq_delta"] * merged["model_eq_delta"]) > 0
    return merged


# ── Display helpers ───────────────────────────────────────────────────────────

def _bar(val: float, maxv: float, width: int = 15, color: str = "cyan") -> str:
    fill = min(int(round(max(val, 0) / maxv * width)), width)
    return f"[{color}]{'█' * fill}{'░' * (width - fill)}[/{color}]"


def _sgn(v: float) -> str:
    if abs(v) < 0.5:
        return "[dim]~0[/dim]"
    return f"[green]+{v:.1f}[/green]" if v > 0 else f"[red]{v:+.1f}[/red]"


# ── Display sections ─────────────────────────────────────────────────────────

def display_vlrt_scores(monthly: pd.DataFrame) -> None:
    console.rule("[bold cyan]1. VLRT SCORE TIMELINE")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",      style="cyan",  width=9)
    tbl.add_column("Nifty₹",     justify="right", width=8)
    tbl.add_column("V",          justify="right", width=6)
    tbl.add_column("L",          justify="right", width=6)
    tbl.add_column("R",          justify="right", width=6)
    tbl.add_column("T",          justify="right", width=6)
    tbl.add_column("Stress",     justify="right", width=7)
    tbl.add_column("Momt",       justify="right", width=6)
    tbl.add_column("VLRT",       justify="right", width=7)
    tbl.add_column("Regime",     width=14)
    tbl.add_column("Impl Eq%",   justify="right", width=9)
    tbl.add_column("Impl Au%",   justify="right", width=9)
    tbl.add_column("GoldVsEq",   justify="right", width=10)
    tbl.add_column("INR dev%",   justify="right", width=9)

    for _, r in monthly.iterrows():
        lbl = r["regime"]
        col = regime_color(lbl)
        ge  = r.get("gold_eq_rel")
        ud  = r.get("usd_dev")
        tbl.add_row(
            str(r["ym"]),
            f"{r['close']:.1f}",
            f"{r['V']:.1f}",
            f"{r['L']:.1f}",
            f"{r['R']:.1f}",
            f"{r['T']:.1f}",
            f"[{'red' if r['stress'] > 0.5 else 'dim'}]{r['stress']:.2f}[/{'red' if r['stress'] > 0.5 else 'dim'}]",
            f"[{'green' if r.get('momentum', 0) > 0.4 else 'dim'}]{r.get('momentum', 0):.2f}[/{'green' if r.get('momentum', 0) > 0.4 else 'dim'}]",
            f"[{col}]{r['VLRT']:.1f}[/{col}]",
            f"[{col}]{lbl}[/{col}]",
            f"[{col}]{r['impl_eq']:.0f}%[/{col}]",
            f"[{'yellow' if r['impl_gold'] > 14 else 'dim'}]{r['impl_gold']:.0f}%[/{'yellow' if r['impl_gold'] > 14 else 'dim'}]",
            f"{ge:+.1f}%" if pd.notna(ge) else "—",
            f"{ud:+.1f}%" if pd.notna(ud) else "—",
        )
    console.print(tbl)


def display_sub_signals(monthly: pd.DataFrame) -> None:
    console.rule("[bold cyan]2. SUB-SIGNAL BREAKDOWN (V=Valuation · L=Liquidity · R=Risk Appetite · T=Timing)")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",  style="cyan", width=9)
    tbl.add_column("V — Valuation (0–25)",        width=24)
    tbl.add_column("L — Liquidity (0–25)",         width=24)
    tbl.add_column("R — Risk Appetite (0–25)",     width=24)
    tbl.add_column("T — Timing synth (0–25)",      width=24)
    tbl.add_column("Stress",  justify="right",  width=7)

    for _, r in monthly.iterrows():
        tbl.add_row(
            str(r["ym"]),
            _bar(r["V"], 25, 15, "blue")   + f"  {r['V']:.1f}",
            _bar(r["L"], 25, 15, "cyan")   + f"  {r['L']:.1f}",
            _bar(r["R"], 25, 15, "green")  + f"  {r['R']:.1f}",
            _bar(r["T"], 25, 15, "yellow") + f"  {r['T']:.1f}",
            f"{r['stress']:.2f}",
        )
    console.print(tbl)


def display_comparison(merged: pd.DataFrame) -> None:
    console.rule("[bold magenta]3. MODEL vs ACTUAL — Quant Multi Asset Allocation")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",     style="cyan",  width=9)
    tbl.add_column("Regime",    width=14)
    tbl.add_column("VLRT",      justify="right", width=6)
    tbl.add_column("Stress",    justify="right", width=7)
    tbl.add_column("Momt",      justify="right", width=6)
    tbl.add_column("─ Model ─", width=1)
    tbl.add_column("Eq%",       justify="right", width=6)
    tbl.add_column("Au%",       justify="right", width=6)
    tbl.add_column("Bo%",       justify="right", width=6)
    tbl.add_column("─ Actual ─",width=1)
    tbl.add_column("Eq%",       justify="right", width=6)
    tbl.add_column("Au%",       justify="right", width=6)
    tbl.add_column("Bo%",       justify="right", width=6)
    tbl.add_column("─ Δ ─",     width=1)
    tbl.add_column("ΔEq",       justify="right", width=7)
    tbl.add_column("ΔAu",       justify="right", width=7)
    tbl.add_column("Dir✓",      justify="center", width=5)

    for _, r in merged.iterrows():
        lbl = r["regime"]
        col = regime_color(lbl)
        dir_str = "[green]✓[/green]" if r.get("dir_correct") else "[red]✗[/red]"
        tbl.add_row(
            str(r["ym"]),
            f"[{col}]{lbl}[/{col}]",
            f"[{col}]{r['VLRT']:.1f}[/{col}]",
            f"{r['stress']:.2f}",
            f"{r.get('momentum', 0):.2f}",
            "",
            f"{r['impl_eq']:.0f}",
            f"{r['impl_gold']:.0f}",
            f"{r['impl_bond']:.0f}",
            "",
            f"{r['equity']:.0f}",
            f"{r['gold']:.0f}",
            f"{r['bond']:.0f}",
            "",
            _sgn(-r["eq_err"]),   # invert: model < actual = model was too cautious
            _sgn(-r["gold_err"]),
            dir_str,
        )
    console.print(tbl)


def display_accuracy_summary(merged: pd.DataFrame) -> None:
    console.rule("[bold green]4. ACCURACY SUMMARY")
    n       = len(merged)
    valid_d = merged["dir_correct"].notna().sum()
    dir_acc = merged["dir_correct"].sum() / max(valid_d, 1) * 100
    mae_eq  = merged["eq_err"].abs().mean()
    mae_au  = merged["gold_err"].abs().mean()
    mae_bo  = merged["bond_err"].abs().mean()

    merged["actual_regime"] = merged["equity"].apply(lambda e:
        "RISK-ON MAX"  if e >= 65 else
        "RISK-ON"      if e >= 55 else
        "NEUTRAL"      if e >= 45 else
        "RISK-OFF"     if e >= 35 else
        "FULL DEFENSE"
    )
    regime_exact  = (merged["regime"] == merged["actual_regime"]).sum()
    band_idx      = {l: i for i, (_, _, l, _) in enumerate(REGIME_BANDS)}
    within_1      = merged.apply(
        lambda r: abs(band_idx.get(r["regime"], 2) - band_idx.get(r["actual_regime"], 2)) <= 1,
        axis=1
    ).sum()

    txt = f"""
[bold]Months analysed:[/bold]   {n}   (Jan 2023 – Jul 2026)

[bold]Equity  MAE:[/bold]  [yellow]{mae_eq:.1f} pct-pts[/yellow]
[bold]Gold    MAE:[/bold]  [yellow]{mae_au:.1f} pct-pts[/yellow]
[bold]Bond    MAE:[/bold]  [yellow]{mae_bo:.1f} pct-pts[/yellow]

[bold]Direction accuracy:[/bold]  [{'green' if dir_acc>=60 else 'red'}]{dir_acc:.1f}%[/{'green' if dir_acc>=60 else 'red'}]  (did model agree which way equity should move?)
[bold]Regime exact match:[/bold]  [cyan]{regime_exact}/{n}[/cyan]  ({regime_exact/n*100:.0f}%)
[bold]Regime within 1 band:[/bold] [cyan]{within_1}/{n}[/cyan]  ({within_1/n*100:.0f}%)

[bold]Quant actual equity range:[/bold]  [{merged['equity'].min():.0f}% – {merged['equity'].max():.0f}%]
[bold]VLRT implied equity range:[/bold]  [{merged['impl_eq'].min():.0f}% – {merged['impl_eq'].max():.0f}%]
[bold]VLRT implied gold range:  [/bold]  [{merged['impl_gold'].min():.0f}% – {merged['impl_gold'].max():.0f}%]

[dim]What the model can capture: regime direction, risk-off gold surges, liquidity turns.[/dim]
[dim]What it cannot capture: exact timing of surprise events (wars, elections, budgets),[/dim]
[dim]  internal fund factors (redemption pressure, cross-fund signals, F-score screens).[/dim]
[dim]Quant's real VLRT uses 25+ inputs including options IV, credit spreads, crypto data.[/dim]
"""
    console.print(Panel(txt, title="[bold green]VLRT v2 Accuracy[/bold green]", border_style="green"))

    console.rule("[bold cyan]5. REGIME DISTRIBUTION — Model vs Actual")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Regime",         width=16)
    tbl.add_column("Model months",   justify="right", width=14)
    tbl.add_column("Actual months",  justify="right", width=14)
    for _, _, lbl, _ in REGIME_BANDS:
        col = regime_color(lbl)
        tbl.add_row(
            f"[{col}]{lbl}[/{col}]",
            f"[{col}]{(merged['regime']==lbl).sum()}[/{col}]",
            f"[{col}]{(merged['actual_regime']==lbl).sum()}[/{col}]",
        )
    console.print(tbl)


def display_divergence_analysis(merged: pd.DataFrame) -> None:
    console.rule("[bold red]6. BIGGEST DIVERGENCES — Where Model ≠ Manager")
    top10 = merged.copy()
    top10["abs_err"] = top10["eq_err"].abs()
    top10 = top10.nlargest(10, "abs_err")

    REASONS = {
        "2023-Oct": "Gold pre-positioned (38%) for Hamas war — Quant saw geopolitical risk BEFORE market",
        "2025-Oct": "Max bonds (45%) on elevated PE + macro stress — VLRT V-score insufficient",
        "2025-Nov": "Stayed defensive despite market recovery — fund's internal risk model still cautious",
        "2025-Dec": "Sudden 67% equity + Silver ETF entry — year-end momentum flip; model lagged",
        "2024-Jul": "Equity cut to 34% post-budget: capital gains tax shock — model missed policy risk",
        "2024-Apr": "Election uncertainty de-risk — not a price signal, a political event signal",
        "2026-May": "Quant pushed equity to 74%: momentum-chase mode; VLRT already at RISK-ON",
        "2026-Jun": "Silver ETF 16% allocation: tactical commodity thesis, not a V/L/R signal",
    }

    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",       style="cyan",  width=9)
    tbl.add_column("VLRT Regime", width=14)
    tbl.add_column("Model Eq%",   justify="right", width=10)
    tbl.add_column("Actual Eq%",  justify="right", width=10)
    tbl.add_column("Δ",           justify="right", width=6)
    tbl.add_column("Stress",      justify="right", width=7)
    tbl.add_column("What VLRT missed", style="dim", width=52)

    for _, r in top10.iterrows():
        m   = str(r["ym"])
        col = "red" if r["eq_err"] < -5 else "dark_orange" if r["eq_err"] < 0 else "green"
        lbl = r["regime"]
        why = next((v for k, v in REASONS.items() if k in m), "Proprietary internal signal not captured by open-market proxies")
        tbl.add_row(
            m,
            f"[{regime_color(lbl)}]{lbl}[/{regime_color(lbl)}]",
            f"{r['impl_eq']:.0f}%",
            f"[{col}]{r['equity']:.0f}%[/{col}]",
            f"[{col}]{r['eq_err']:+.1f}[/{col}]",
            f"{r['stress']:.2f}",
            why,
        )
    console.print(tbl)


def display_vlrt_explainer() -> None:
    console.rule("[bold magenta]7. VLRT™ EXPLAINED — Per Sandeep Tandon (Quant MF CIO)")
    console.print(Panel("""
[bold cyan]V — VALUATION[/bold cyan]
  · Balance sheet quality, cash flows, P&L, historical valuation ratios
  · "Valuation is just one piece of the puzzle — NOT the only lens"
  · [dim]Proxy used here: Nifty z-score vs 3-year rolling mean[/dim]

[bold cyan]L — LIQUIDITY[/bold cyan]  [yellow]← "The most underrated driver"[/yellow]
  · "Liquidity dries up at tops AND bottoms" — critical for risk management
  · Monitors global + domestic flows: FII, RBI, Fed, USDINR, credit
  · [dim]Proxy: USDINR trend, FII net flows, inverse of gold momentum[/dim]

[bold cyan]R — RISK APPETITE[/bold cyan]  [yellow]← Behavioral, not volatility[/yellow]
  · High-frequency data + behavioral proxies (crypto sentiment, retail flows)
  · Risk-ON → aggressive: small/mid/illiquid names
  · Risk-OFF → defensive: Reliance, ONGC, L&T (low-beta large-caps)
  · [dim]Proxy: gold vs equity relative, JUNIORBEES vs NIFTYBEES breadth, USDINR stress[/dim]

[bold cyan]T — TIMING[/bold cyan]  [yellow]← NOT about trading[/yellow]
  · "T = risk management timing by combining V + L + R signals"
  · T is HIGH when V, L, R all point the same direction (alignment)
  · [dim]Proxy: alignment score (inverse std-dev) of V/L/R signals[/dim]

[bold yellow]DYNAMIC WEIGHTS[/bold yellow] [yellow]← The key differentiator[/yellow]
  · March 2020:      L + R = 90%, V = 10%
  · Normal market:   V = 30%, L = 30%, R = 25%, T = 15%
  · As stress rises: V weight shrinks, L + R weight grows
  · [dim]Stress = f(gold vs equity, USDINR deviation, drawdown from ATH)[/dim]

[bold yellow]GOLD ALLOCATION RULE[/bold yellow]
  · Gold is the primary risk-off hedge, not a valuation call
  · Structural floor: 9–11%
  · Tactical surge: 20–38% when R is LOW (risk-off) + gold is outperforming
  · Evidence: Oct 2023 → 38% gold (Hamas); Aug 2025 → 23% silver+gold; Dec 2025 → 22%

[bold yellow]PHASE EXAMPLES FROM HOLDINGS DATA[/bold yellow]
  · Jul 2024: India declared risk-off → equity cut to 34% [dim](market still rising)[/dim]
  · Oct 2023: Risk-off + gold surge BEFORE the Hamas rally
  · Dec 2025: Risk-on flip → equity jumped from 37% → 67% in 1 month
  · Mid-2025: India "mild-to-moderate risk-on" per Tandon — portfolio shows 68–74% equity
""", title="[bold magenta]VLRT™ Source Reference[/bold magenta]", border_style="magenta"))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Quant VLRT v2 simulator")
    parser.add_argument("--fund",       default="QUANT_MULTI_ASSET")
    parser.add_argument("--no-compare", action="store_true")
    parser.add_argument("--start",      default="2023-01-01")
    args = parser.parse_args()

    console.print(Panel(
        "[bold cyan]VLRT™ MODEL SIMULATION v2[/bold cyan]\n"
        "Source: Sandeep Tandon (Quant MF CIO) interviews + holdings reverse-engineering\n\n"
        "[bold]V[/bold]=Valuation (3yr z-score)  "
        "[bold]L[/bold]=Liquidity (USDINR+FII+Gold)  "
        "[bold]R[/bold]=Risk Appetite (behavioral)  "
        "[bold]T[/bold]=Timing (V+L+R synthesis)\n"
        "[yellow]Dynamic weights: stress level shifts weight from V → L+R in crises[/yellow]\n"
        f"Fund: [yellow]{args.fund}[/yellow]",
        title="[bold white]Mosaic VLRT Simulator[/bold white]",
        border_style="cyan",
    ))

    console.print("[dim]Loading price data (NIFTYBEES + GOLDBEES + JUNIORBEES)…[/dim]")
    prices  = load_daily_prices(start="2021-07-01")  # warmup for 3yr sma
    usdinr  = load_usdinr(start="2021-07-01")
    fii     = load_fii_flows()

    console.print("[dim]Computing VLRT signals…[/dim]")
    monthly = compute_vlrt_monthly(prices, usdinr, fii)
    monthly = monthly[monthly["ym"] >= pd.Period(args.start[:7], "M")].reset_index(drop=True)

    display_vlrt_explainer()
    display_vlrt_scores(monthly)
    display_sub_signals(monthly)

    if not args.no_compare:
        console.print("[dim]Loading actual Quant holdings…[/dim]")
        actual = load_actual_alloc(args.fund)
        merged = compute_accuracy(monthly, actual)
        display_comparison(merged)
        display_accuracy_summary(merged)
        display_divergence_analysis(merged)

    console.print(Panel(
        "[bold green]✓ Simulation complete (VLRT v2).[/bold green]\n"
        "Sources: [cyan]daily_prices[/cyan] · [cyan]fx_rates (USDINR)[/cyan] · "
        "[cyan]fii_dii_flows[/cyan] · [cyan]mf_holdings[/cyan] FINAL",
        border_style="green",
    ))


if __name__ == "__main__":
    main()
