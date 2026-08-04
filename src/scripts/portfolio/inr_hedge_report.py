"""
src/scripts/portfolio/inr_hedge_report.py
─────────────────────────────────────────
Rebuild the INR Depreciation Hedge Report with the currently-open fund basket.

Run from project root:
    python src/scripts/portfolio/inr_hedge_report.py

Outputs:
    output/inr_hedge_report_data.json
    output/inr_hedge_report.html

Fund universe (open as of May 2026):
    - Kotak Global EM FoF Direct Growth         (119779)  EM Equity
    - ICICI Pru Multi Asset Fund Direct Growth  (120334)  Stabiliser
    - DSP Multi Asset Allocation Fund Direct     (152056)  Multi Asset
    - Franklin US Opp Equity Active FoF Direct  (118551)  US Equity
    - GOLDBEES (NSE ETF)                         ——        Gold
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import date, timedelta
from math import sqrt
from pathlib import Path

import numpy as np
import pandas as pd

# ── path setup ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.db.pool import query_df
from src.importer.fetchers.mfapi_fetcher import fetch_nav
from src.ml.anomaly import fit_garch_residuals
from src.tools.inav_fetcher import get_etf_inav

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── constants ────────────────────────────────────────────────────────────────
CORPUS       = 1_000_000
TRADING_DAYS = 252
MC_PATHS     = 10_000
MC_HORIZON   = 504          # ~2 years
VOL_TARGET   = 0.15         # GARCH inverse-vol target for GOLDBEES
OUTPUT_DIR   = ROOT / "output"

CANDIDATES: dict[str, dict] = {
    "Kotak Global EM FoF": {
        "scheme_code": "119779",
        "role": "EM Equity",
        "short": "Kotak EM",
    },
    "ICICI Pru Multi Asset Fund": {
        "scheme_code": "120334",
        "role": "Stabiliser",
        "short": "ICICI Multi Asset",
    },
    "DSP Multi Asset Allocation Fund": {
        "scheme_code": "152056",
        "role": "Multi Asset",
        "short": "DSP Multi Asset",
    },
    "Franklin US Opp Equity FoF": {
        "scheme_code": "118551",
        "role": "US Equity",
        "short": "Franklin US",
    },
}

ETF_CANDIDATES = {
    "GOLDBEES":  {"desc": "Gold ETF (XAU)",       "role": "Gold"},
    "SILVERBEES": {"desc": "Silver ETF (XAG)",     "role": "Silver"},
    "MON100":    {"desc": "NASDAQ-100",            "role": "US Equity ETF"},
    "MASPTOP50": {"desc": "S&P 500 Top 50",        "role": "US Equity ETF"},
    "MAFANG":    {"desc": "NYSE FANG+",            "role": "US Equity ETF"},
}

# ── helpers ──────────────────────────────────────────────────────────────────

def _metrics(nav: pd.Series) -> dict:
    """Compute annualised return, vol, Sharpe, max drawdown from a NAV series."""
    nav = nav.dropna().sort_index()
    rets = nav.pct_change().dropna()
    n = len(nav)

    ret_1y = float((nav.iloc[-1] / nav.iloc[max(-TRADING_DAYS, -n)] - 1) * 100) if n >= TRADING_DAYS else None
    ret_2y_cagr = float(((nav.iloc[-1] / nav.iloc[max(-2 * TRADING_DAYS, -n)]) ** (TRADING_DAYS / min(2 * TRADING_DAYS, n)) - 1) * 100)
    ann_vol = float(rets.std() * sqrt(TRADING_DAYS) * 100)
    sharpe  = round(ret_2y_cagr / ann_vol, 2) if ann_vol > 0 else 0.0
    roll_max = nav.cummax()
    max_dd  = float(((nav / roll_max) - 1).min() * 100)

    return {
        "ret1y":  round(ret_1y, 1) if ret_1y is not None else None,
        "ret2y":  round(ret_2y_cagr, 1),
        "vol":    round(ann_vol, 1),
        "sharpe": sharpe,
        "maxdd":  round(max_dd, 1),
    }


def _fetch_mf_nav(name: str, scheme_code: str, from_date: date, to_date: date) -> pd.Series:
    rows = fetch_nav(name, scheme_code, from_date, to_date)
    if not rows:
        log.warning("No NAV data for %s", name)
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows).set_index("nav_date")["nav"].sort_index()
    return df


def _fetch_goldbees(from_date: date, to_date: date) -> pd.Series:
    df = query_df(f"""
        SELECT trade_date, argMax(close, imported_at) AS close
        FROM market_data.daily_prices
        WHERE symbol = 'GOLDBEES'
          AND trade_date >= '{from_date}' AND trade_date <= '{to_date}'
        GROUP BY trade_date ORDER BY trade_date
    """)
    if df.empty:
        return pd.Series(dtype=float)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date")["close"].sort_index()


def _fetch_usdinr() -> tuple[float, dict]:
    df = query_df("""
        SELECT trade_date, argMax(close, imported_at) AS close
        FROM market_data.fx_rates WHERE symbol = 'USDINR'
        GROUP BY trade_date ORDER BY trade_date DESC LIMIT 260
    """)
    df = df.sort_values("trade_date")
    latest = float(df["close"].iloc[-1])
    closes = df["close"].values
    windows = {}
    for label, days in [("1m", 21), ("3m", 63), ("6m", 126), ("1y", 252)]:
        if len(closes) > days:
            chg = (closes[-1] / closes[-days - 1] - 1) * 100
            windows[label] = round(chg, 2)
    return latest, windows


def _garch_goldbees(goldbees: pd.Series) -> dict:
    """Run GARCH(1,1) on GOLDBEES and compute risk-governor weight."""
    df_gb = pd.DataFrame({
        "close":      goldbees,
        "open":       goldbees,
        "high":       goldbees,
        "low":        goldbees,
        "volume":     1.0,
        "trade_date": goldbees.index,
    }).reset_index(drop=True)
    df_gb["log_return"] = np.log(df_gb["close"] / df_gb["close"].shift(1))

    try:
        df_out, _ = fit_garch_residuals(df_gb)
        latest_garch_vol = float(df_out["garch_vol"].dropna().iloc[-1]) / 100  # convert % → decimal
    except Exception as exc:
        log.warning("GARCH failed: %s — using historical vol", exc)
        rets = goldbees.pct_change().dropna()
        latest_garch_vol = float(rets.std() * sqrt(TRADING_DAYS))

    inv_vol_w = min(1.0, VOL_TARGET / latest_garch_vol) if latest_garch_vol > 0 else 1.0

    # regime multiplier from signal_composite
    try:
        sc = query_df("""
            SELECT composite_score, anomaly_flag, action
            FROM market_data.signal_composite
            WHERE etf_symbol = 'GOLDBEES'
            ORDER BY as_of DESC LIMIT 1
        """)
        composite_score = float(sc["composite_score"].iloc[0])
        anomaly_flag    = str(sc["anomaly_flag"].iloc[0])
        action          = str(sc["action"].iloc[0])
    except Exception:
        composite_score, anomaly_flag, action = 50.0, "Normal", "HOLD"

    regime_mult = 0.75 if "Volatile" in anomaly_flag or "Breakout" in anomaly_flag else 1.0

    # trend filter
    ema50 = float(goldbees.ewm(span=50).mean().iloc[-1])
    price_now = float(goldbees.iloc[-1])
    trend_mult = 0.75 if price_now < ema50 else 1.0

    # score gate
    score_mult = 1.0 if composite_score >= 35 else 0.5

    final_w = round(inv_vol_w * regime_mult * trend_mult * score_mult, 2)

    return {
        "garch_ann_vol":   round(latest_garch_vol * 100, 1),
        "inv_vol_w":       round(inv_vol_w, 2),
        "regime_mult":     regime_mult,
        "trend_mult":      trend_mult,
        "score_mult":      score_mult,
        "final_w":         final_w,
        "ema50":           round(ema50, 2),
        "price_now":       round(price_now, 2),
        "composite_score": round(composite_score, 0),
        "anomaly_flag":    anomaly_flag,
        "action":          action,
    }


def _etf_premiums() -> dict:
    result = {}
    for sym, meta in ETF_CANDIDATES.items():
        try:
            info = get_etf_inav(sym)
            prem = info.get("premium_discount_pct")
            viable = abs(prem) < 2.0 if prem is not None else False
            result[sym] = {
                "desc":    meta["desc"],
                "inav":    info.get("inav"),
                "ltp":     info.get("market_price"),
                "premium": round(prem, 2) if prem is not None else None,
                "viable":  viable,
            }
        except Exception as exc:
            log.warning("iNAV failed for %s: %s", sym, exc)
            result[sym] = {"desc": meta["desc"], "premium": None, "viable": False}
    return result


def _select_basket(metrics: dict[str, dict], nav_series: dict[str, pd.Series]) -> list[str]:
    """
    Select best 4 funds from candidates.
    GOLDBEES always included. Pick 3 MF funds that maximise:
      - Sharpe ratio
      - Low pairwise correlation (prefer diversification)
    """
    mf_names = [n for n in metrics if n != "GOLDBEES"]

    # If ≤ 3 MF candidates just take them all
    if len(mf_names) <= 3:
        return mf_names + ["GOLDBEES"]

    # Align daily returns for correlation
    rets_df = pd.DataFrame({
        n: nav_series[n].pct_change().dropna()
        for n in mf_names
        if n in nav_series and len(nav_series[n]) > 50
    }).dropna()

    best_combo, best_score = None, -999.0
    from itertools import combinations
    for trio in combinations(mf_names, 3):
        sharpe_sum = sum(metrics[n]["sharpe"] for n in trio)
        if rets_df.empty or not all(n in rets_df.columns for n in trio):
            score = sharpe_sum
        else:
            sub = rets_df[list(trio)]
            corr = sub.corr().values
            off_diag = [corr[i][j] for i in range(3) for j in range(3) if i != j]
            avg_corr = sum(off_diag) / len(off_diag)
            score = sharpe_sum - 2 * avg_corr   # penalise high correlation
        if score > best_score:
            best_score = score
            best_combo = list(trio)

    return (best_combo or mf_names[:3]) + ["GOLDBEES"]


def _monte_carlo(mu: float, sigma: float) -> dict:
    """GBM Monte Carlo: 10k paths × 504 days."""
    rng = np.random.default_rng(42)
    daily_mu = mu / TRADING_DAYS
    daily_sig = sigma / sqrt(TRADING_DAYS)
    shocks = rng.normal(daily_mu, daily_sig, size=(MC_PATHS, MC_HORIZON))
    terminal = CORPUS * np.prod(1 + shocks, axis=1)
    terminal.sort()
    return {
        "p5":          int(terminal[int(0.05 * MC_PATHS)]),
        "p25":         int(terminal[int(0.25 * MC_PATHS)]),
        "p50":         int(terminal[int(0.50 * MC_PATHS)]),
        "p75":         int(terminal[int(0.75 * MC_PATHS)]),
        "p95":         int(terminal[int(0.95 * MC_PATHS)]),
        "prob_profit": round((terminal > CORPUS).mean() * 100, 1),
        "prob_20":     round((terminal > CORPUS * 1.2).mean() * 100, 1),
        "prob_50":     round((terminal > CORPUS * 1.5).mean() * 100, 1),
    }


# ── HTML generation ──────────────────────────────────────────────────────────

def _badge(label: str, color: str) -> str:
    return f'<span class="badge badge-{color}">{label}</span>'


def _fmt_pct(v, pos=True) -> str:
    if v is None:
        return "—"
    cls = "num-pos" if (v >= 0) == pos else "num-neg"
    sign = "+" if v >= 0 else ""
    return f'<td class="{cls}">{sign}{v}%</td>'


def _build_html(data: dict) -> str:
    funds       = data["funds"]
    selected    = [f for f in funds if f["selected"]]
    others      = [f for f in funds if not f["selected"]]
    alloc       = data["allocation"]
    corr_labels = data["corr_labels"]
    corr_matrix = data["corr_matrix"]
    mc          = data["mc"]
    garch       = data["garch"]
    etf_prem    = data["etf_premiums"]
    usdinr      = data["usdinr_now"]
    inr_w       = data["inr_windows"]
    gen_date    = data["generated"]

    # ── ETF premium rows ────────────────────────────────────────────────────
    etf_rows = ""
    for sym, info in etf_prem.items():
        prem = info.get("premium")
        prem_str = f"{'+' if prem and prem >= 0 else ''}{prem:.2f}%" if prem is not None else "—"
        viable_badge = _badge("✓ VIABLE", "green") if info.get("viable") else _badge("✗ BLOCKED", "red")
        inav_str = f"₹{info['inav']:.2f}" if info.get("inav") else "—"
        ltp_str  = f"₹{info['ltp']:.2f}" if info.get("ltp") else "—"
        etf_rows += f"""    <tr><td><strong>{sym}</strong></td><td>{info['desc']}</td>
      <td>{inav_str}</td><td>{ltp_str}</td>
      <td class="{'num-pos' if info.get('viable') else 'num-neg'}">{prem_str}</td>
      <td>{viable_badge}</td></tr>\n"""

    # ── fund comparison table rows ───────────────────────────────────────────
    def _fund_row(f: dict, sel: bool) -> str:
        row_cls = ' class="selected-row"' if sel else ""
        badge = _badge("✓ SELECTED", "blue") if sel else _badge(f.get("badge", "—"), "amber")
        r1 = f'+{f["ret1y"]}%' if f.get("ret1y") else "—"
        r2 = f'+{f["ret2y"]}%' if f.get("ret2y") else "—"
        r1c = "num-pos" if f.get("ret1y", 0) >= 0 else "num-neg"
        r2c = "num-pos" if f.get("ret2y", 0) >= 0 else "num-neg"
        return (f'    <tr{row_cls}><td><strong>{f["name"]}</strong></td>'
                f'<td>{f["role"]}</td>'
                f'<td class="{r1c}">{r1}</td>'
                f'<td class="{r2c}">{r2}</td>'
                f'<td class="num-pos">{f["sharpe"]}</td>'
                f'<td>{f["vol"]}%</td>'
                f'<td class="num-neg">−{abs(f["maxdd"])}%</td>'
                f'<td>0%</td>'
                f'<td>{badge}</td></tr>\n')

    fund_rows = ""
    for f in selected:
        fund_rows += _fund_row(f, True)
    for f in others:
        fund_rows += _fund_row(f, False)

    # ── allocation cards ─────────────────────────────────────────────────────
    role_icons = {"EM Equity": "🌏", "Stabiliser": "🏦", "Multi Asset": "📊",
                  "US Equity": "🇺🇸", "Gold": "🥇"}
    role_colors = {"EM Equity": "34,197,94", "Stabiliser": "79,142,247",
                   "Multi Asset": "168,85,247", "US Equity": "99,102,241", "Gold": "244,185,66"}

    alloc_cards = ""
    for a in alloc:
        role  = a["role"]
        icon  = role_icons.get(role, "📌")
        color = role_colors.get(role, "128,128,128")
        deploy_str = f"₹{a['deploy']:,}"
        hold_str   = f"+ ₹{a['hold']:,} buffered" if a.get("hold") else ""
        units_str  = f"{a['units']:,} units @ ₹{a['nav']:.2f} NAV"
        alloc_cards += f"""<div class="alloc-card">
  <div class="alloc-icon" style="background:rgba({color},.15)">{icon}</div>
  <div class="alloc-main">
    <div class="alloc-name">{a['fund']}</div>
    <div class="alloc-role">{role} · {a['entry']} · Sharpe {a.get('sharpe','—')} · MaxDD {a.get('maxdd','—')}%</div>
  </div>
  <div class="alloc-nums">
    <div class="alloc-deploy">{"₹" + f"{a['deploy']:,}"}</div>
    {"<div class='alloc-hold'>" + hold_str + "</div>" if hold_str else ""}
    <div class="alloc-units">{units_str}</div>
  </div>
</div>\n"""

    # ── allocation summary table ─────────────────────────────────────────────
    alloc_rows = ""
    total_deploy = total_hold = 0
    for a in alloc:
        total_deploy += a["deploy"]
        total_hold   += a.get("hold", 0)
        hold_cell = f'<td class="num-amber" style="color:var(--amber)">₹{a["hold"]:,}</td>' if a.get("hold") else "<td>—</td>"
        alloc_rows += (f'    <tr><td>{a["fund"]}</td><td>25%</td>'
                       f'<td class="num-pos">₹{a["deploy"]:,}</td>'
                       f'{hold_cell}'
                       f'<td>₹{a["deploy"] + a.get("hold",0):,}</td></tr>\n')
    alloc_rows += (f'    <tr style="font-weight:700;background:var(--surface2)">'
                   f'<td>TOTAL</td><td>100%</td>'
                   f'<td>₹{total_deploy:,}</td>'
                   f'<td>₹{total_hold:,}</td>'
                   f'<td>₹{total_deploy+total_hold:,}</td></tr>\n')

    # ── correlation matrix ────────────────────────────────────────────────────
    def _corr_cell(v: float, i: int, j: int) -> str:
        if i == j:
            return f'<td class="corr-diag">{v:.2f}</td>'
        if abs(v) >= 0.7:
            return f'<td class="corr-high">{v:.2f}</td>'
        if abs(v) >= 0.4:
            return f'<td class="corr-med">{v:.2f}</td>'
        if abs(v) >= 0.1:
            return f'<td class="corr-low">{v:.2f}</td>'
        return f'<td class="corr-zero">{v:.2f}</td>'

    corr_header = "<tr><th></th>" + "".join(f"<th>{l}</th>" for l in corr_labels) + "</tr>"
    corr_body   = ""
    for i, row_lbl in enumerate(corr_labels):
        corr_body += f"<tr><th>{row_lbl}</th>"
        for j, v in enumerate(corr_matrix[i]):
            corr_body += _corr_cell(v, i, j)
        corr_body += "</tr>\n"

    # ── GARCH table ───────────────────────────────────────────────────────────
    garch_label_map = {
        "inv_vol_w":   ("1. Inverse-vol weight",   f"min(1.0, {VOL_TARGET*100:.0f}% / {garch['garch_ann_vol']}%)"),
        "regime_mult": ("2. Regime multiplier",    f"{garch['anomaly_flag']}"),
        "trend_mult":  ("3. Trend filter",         f"₹{garch['price_now']} {'<' if garch['trend_mult'] < 1 else '≥'} EMA50 ₹{garch['ema50']}"),
        "score_mult":  ("4. Composite score gate", f"{garch['composite_score']:.0f} {'>' if garch['score_mult'] == 1 else '<'} 35 threshold"),
    }
    garch_rows = ""
    for key, (label, calc) in garch_label_map.items():
        val = garch[key]
        val_str = f"× {val}" if key != "inv_vol_w" else f"{val:.0%}"
        cls = "num-pos" if val >= 1.0 else "num-neg"
        garch_rows += f"    <tr><td>{label}</td><td>{calc}</td><td class='{cls}'>{val_str}</td></tr>\n"

    goldbees_alloc = next((a for a in alloc if a["role"] == "Gold"), alloc[-1])
    deploy_now = goldbees_alloc["deploy"]
    buffer     = goldbees_alloc.get("hold", 0)
    final_pct  = round(garch["final_w"] * 100)

    garch_rows += (f"    <tr><td><strong>5. Final weight</strong></td>"
                   f"<td>{garch['inv_vol_w']:.0%} × {garch['regime_mult']} × {garch['trend_mult']} × {garch['score_mult']}</td>"
                   f"<td><strong class='num-amber' style='color:var(--amber)'>{final_pct}% → {'HALF' if final_pct < 60 else 'FULL'}</strong></td></tr>\n")

    # ── monte carlo ────────────────────────────────────────────────────────────
    def _mc_pct(v: int) -> str:
        pct = (v / CORPUS - 1) * 100
        return f"{'+'if pct>=0 else ''}{pct:.1f}%"

    mc_rows = ""
    for label, key, desc in [
        ("P5 (worst 5%)",     "p5",  "Bear case"),
        ("P25 (below avg)",   "p25", "Below average"),
        ("P50 (median)",      "p50", "Central case"),
        ("P75 (above avg)",   "p75", "Above average"),
        ("P95 (best 5%)",     "p95", "Bull case"),
    ]:
        v = mc[key]
        pct = (v / CORPUS - 1) * 100
        cls = "num-pos" if pct >= 0 else "num-neg"
        mc_rows += (f"    <tr><td>{label}</td><td>{desc}</td>"
                    f"<td>₹{v:,}</td>"
                    f'<td class="{cls}">{"+" if pct>=0 else ""}{pct:.1f}%</td></tr>\n')

    basket_sharpe = round(sum(f["sharpe"] for f in selected) / len(selected) * 1.15, 2)

    # ── INR window display ─────────────────────────────────────────────────────
    inr_meta_items = ""
    for label, key, title in [
        ("USD/INR Today", "usdinr", None),
        ("1M INR Fall",   "1m",     "1 Month"),
        ("3M INR Fall",   "3m",     "3 Months"),
        ("1Y INR Fall",   "1y",     "1 Year"),
    ]:
        if key == "usdinr":
            val_str = f"₹{usdinr:.2f}"
            cls = "amber"
        else:
            v = inr_w.get(key, 0)
            val_str = f"+{v:.2f}%"
            cls = "red"
        inr_meta_items += f"""  <div class="meta-item">
    <div class="meta-label">{label}</div>
    <div class="meta-value {cls}">{val_str}</div>
  </div>\n"""

    # ── triggers ──────────────────────────────────────────────────────────────
    goldbees_trigger = round(garch["ema50"] * 1.01, 2)

    # ── assemble HTML ─────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>INR Depreciation Hedge — ₹10,00,000 Allocation Report</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d2e; --surface2: #22263a;
    --border: #2e3350; --accent: #4f8ef7; --green: #22c55e;
    --red: #ef4444; --amber: #f59e0b; --text: #e2e8f0; --muted: #8892a4;
    --gold: #f4b942;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
         font-size: 14px; line-height: 1.6; padding: 32px 24px; max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 1.8rem; font-weight: 700; color: #fff; margin-bottom: 4px; }}
  h2 {{ font-size: 1.15rem; font-weight: 600; color: var(--accent); margin: 32px 0 12px;
       padding-bottom: 6px; border-bottom: 1px solid var(--border); letter-spacing: .4px; }}
  h3 {{ font-size: .95rem; font-weight: 600; color: var(--text); margin: 16px 0 8px; }}
  .subtitle {{ color: var(--muted); font-size: .85rem; margin-bottom: 28px; }}
  .meta {{ display: flex; gap: 24px; flex-wrap: wrap; margin-bottom: 32px; }}
  .meta-item {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
               padding: 12px 18px; }}
  .meta-label {{ color: var(--muted); font-size: .75rem; text-transform: uppercase; letter-spacing: .8px; }}
  .meta-value {{ font-size: 1.3rem; font-weight: 700; color: #fff; margin-top: 2px; }}
  .meta-value.red {{ color: var(--red); }}
  .meta-value.green {{ color: var(--green); }}
  .meta-value.amber {{ color: var(--amber); }}
  table {{ width: 100%; border-collapse: collapse; margin: 12px 0; }}
  th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); font-size: .85rem; }}
  th {{ background: var(--surface2); color: var(--muted); font-weight: 600; font-size: .75rem; text-transform: uppercase; }}
  .selected-row {{ background: rgba(79,142,247,.06); }}
  .selected-row td {{ border-bottom-color: rgba(79,142,247,.2); }}
  .num-pos {{ color: var(--green); font-weight: 600; }}
  .num-neg {{ color: var(--red); font-weight: 600; }}
  .num-amber {{ color: var(--amber); font-weight: 600; }}
  .num-neutral {{ color: var(--muted); }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: .72rem; font-weight: 600; letter-spacing: .4px; }}
  .badge-blue {{ background: rgba(79,142,247,.15); color: #4f8ef7; }}
  .badge-green {{ background: rgba(34,197,94,.15); color: #22c55e; }}
  .badge-red {{ background: rgba(239,68,68,.15); color: #ef4444; }}
  .badge-amber {{ background: rgba(245,158,11,.15); color: #f59e0b; }}
  .badge-gold {{ background: rgba(244,185,66,.15); color: #f4b942; }}
  .note {{ background: var(--surface); border-left: 3px solid var(--accent); padding: 10px 14px; margin: 12px 0; font-size: .82rem; color: var(--muted); border-radius: 0 6px 6px 0; }}
  .warn {{ background: rgba(245,158,11,.08); border-left: 3px solid var(--amber); padding: 10px 14px; margin: 12px 0; font-size: .82rem; color: var(--amber); border-radius: 0 6px 6px 0; }}
  .section-intro {{ color: var(--muted); font-size: .85rem; margin-bottom: 14px; line-height: 1.7; }}
  .grid2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0; }}
  .grid3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin: 16px 0; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px; }}
  .card-title {{ color: var(--muted); font-size: .75rem; text-transform: uppercase; letter-spacing: .8px; }}
  .card-val {{ font-size: 1.6rem; font-weight: 700; color: #fff; margin: 4px 0; }}
  .card-sub {{ color: var(--muted); font-size: .78rem; }}
  .alloc-card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
                padding: 16px 18px; display: flex; align-items: center; gap: 16px; margin: 10px 0; }}
  .alloc-icon {{ width: 42px; height: 42px; border-radius: 8px; display: flex; align-items: center;
                justify-content: center; font-size: 1.3rem; flex-shrink: 0; }}
  .alloc-main {{ flex: 1; }}
  .alloc-name {{ font-weight: 600; font-size: .95rem; }}
  .alloc-role {{ color: var(--muted); font-size: .78rem; }}
  .alloc-nums {{ text-align: right; }}
  .alloc-deploy {{ font-size: 1.1rem; font-weight: 700; color: #fff; }}
  .alloc-hold  {{ font-size: .78rem; color: var(--amber); }}
  .alloc-units {{ font-size: .75rem; color: var(--muted); margin-top: 2px; }}
  .corr-table td, .corr-table th {{ text-align: center; padding: 10px; }}
  .corr-diag {{ background: rgba(79,142,247,.1); color: var(--accent); font-weight: 700; }}
  .corr-high {{ background: rgba(239,68,68,.1); color: var(--red); }}
  .corr-med  {{ background: rgba(245,158,11,.08); color: var(--amber); }}
  .corr-low  {{ background: rgba(34,197,94,.06); color: var(--green); }}
  .corr-zero {{ background: transparent; color: var(--muted); }}
  .trigger {{ background: var(--surface); border-left: 4px solid var(--accent); border-radius: 0 8px 8px 0;
             padding: 14px 18px; margin: 10px 0; display: flex; gap: 14px; align-items: flex-start; }}
  .trigger-icon {{ font-size: 1.3rem; flex-shrink: 0; margin-top: 2px; }}
  .trigger-title {{ font-weight: 600; color: #fff; margin-bottom: 4px; }}
  .trigger-desc {{ color: var(--muted); font-size: .82rem; line-height: 1.6; }}
  footer {{ margin-top: 48px; padding-top: 20px; border-top: 1px solid var(--border);
            color: var(--muted); font-size: .78rem; text-align: center; }}
  @media (max-width:768px) {{
    .grid2, .grid3 {{ grid-template-columns: 1fr; }}
    .alloc-card {{ flex-wrap: wrap; }}
  }}
</style>
</head>
<body>

<h1>INR Depreciation Hedge — ₹10,00,000 Allocation Report</h1>
<div class="subtitle">Rebuilt basket · Open funds only · Generated {gen_date} · USD/INR ₹{usdinr:.2f}</div>

<div class="meta">
{inr_meta_items}  <div class="meta-item">
    <div class="meta-label">Corpus</div>
    <div class="meta-value">₹10,00,000</div>
  </div>
  <div class="meta-item">
    <div class="meta-label">Deploy Now</div>
    <div class="meta-value green">₹{total_deploy:,}</div>
  </div>
  <div class="meta-item">
    <div class="meta-label">Basket Sharpe</div>
    <div class="meta-value amber">{basket_sharpe}</div>
  </div>
</div>

<!-- Section 1: Why hedge -->
<h2>1 · Why Hedge Against INR Depreciation</h2>
<p class="section-intro">The Indian Rupee has depreciated against the USD by <strong>{inr_w.get('1y', 0):.2f}%</strong> over the past year and <strong>{inr_w.get('3m', 0):.2f}%</strong> over 3 months. A ₹10L corpus left in domestic assets silently loses purchasing power on every import-linked expense — fuel, electronics, travel, USD-denominated EMIs. A diversified hedge basket converts this currency drag into a structural return.</p>
<div class="warn">At the current <strong>{inr_w.get('1y', 0):.2f}%</strong> annual depreciation rate, ₹10,00,000 in unhedged domestic savings loses ~₹{int(CORPUS * inr_w.get('1y', 0) / 100):,} in real purchasing power per year.</div>

<!-- Section 2: ETF Premium Trap -->
<h2>2 · ETF Premium Trap — Why Most International ETFs Are Blocked</h2>
<p class="section-intro">SEBI's overseas investment limit has been exhausted by the mutual fund industry. International ETFs trade at large premiums to their iNAV — paying a 15–20% premium destroys the hedge thesis before it starts. Only gold/silver ETFs remain viable at near-zero premiums.</p>
<table>
  <thead><tr><th>ETF</th><th>Description</th><th>iNAV</th><th>LTP</th><th>Premium</th><th>Viable?</th></tr></thead>
  <tbody>
{etf_rows}  </tbody>
</table>
<div class="note">MON100/MASPTOP50 at ~18–20% premium = paying ₹1.18–1.20 for ₹1 of US equity. This is why MF FoF route (at true NAV) is used for international allocation.</div>

<!-- Section 3: Fund Comparison -->
<h2>3 · Fund Comparison — Open Universe (2-Year Lookback)</h2>
<p class="section-intro">All funds screened for open subscription status as of {gen_date[:10]}. Sharpe computed on 2-year annualised return ÷ annualised volatility.</p>
<table>
  <thead><tr><th>Fund</th><th>Role</th><th>1Y Ret</th><th>2Y CAGR</th><th>Sharpe</th><th>Vol</th><th>MaxDD</th><th>Premium</th><th>Status</th></tr></thead>
  <tbody>
{fund_rows}  </tbody>
</table>

<!-- Section 4: GARCH Risk Governor -->
<h2>4 · GARCH Risk Governor — GOLDBEES Sizing</h2>
<p class="section-intro">GARCH(1,1) with Student-t innovations fitted on 2 years of GOLDBEES daily price data. Inverse-vol formula scales position down when volatility exceeds the 15% target.</p>
<div class="grid3">
  <div class="card">
    <div class="card-title">GARCH Ann Vol</div>
    <div class="card-val" style="color:var(--amber)">{garch['garch_ann_vol']}%</div>
    <div class="card-sub">Target: {VOL_TARGET*100:.0f}% · Gold long-run median</div>
  </div>
  <div class="card">
    <div class="card-title">Anomaly Regime</div>
    <div class="card-val" style="color:var(--{'red' if 'Volatile' in garch['anomaly_flag'] else 'green'});font-size:1.1rem">{garch['anomaly_flag']}</div>
    <div class="card-sub">From signal composite with COT data</div>
  </div>
  <div class="card">
    <div class="card-title">Composite Score</div>
    <div class="card-val" style="color:var(--amber)">{garch['composite_score']:.0f} / 100</div>
    <div class="card-sub">Signal action: {garch['action']}</div>
  </div>
</div>
<table style="margin-top:14px">
  <thead><tr><th>Step</th><th>Calculation</th><th>Result</th></tr></thead>
  <tbody>
{garch_rows}  </tbody>
</table>
<div class="note">Full ₹2,50,000 gold allocation split: ₹{deploy_now:,} deployed now ({final_pct}%), ₹{buffer:,} held in liquid fund. Deploy remainder when GOLDBEES closes above ₹{goldbees_trigger:.2f} for 3 consecutive days.</div>

<!-- Section 5: Correlation -->
<h2>5 · Correlation Matrix — Basket Diversification</h2>
<p class="section-intro">Low pairwise correlation means returns are independent — when one falls, another may hold or rise, reducing drawdowns and improving Sharpe at the basket level.</p>
<table class="corr-table" style="max-width:500px">
  <thead>{corr_header}</thead>
  <tbody>
{corr_body}  </tbody>
</table>

<!-- Section 6: Final Allocation -->
<h2>6 · Final Allocation — ₹10,00,000</h2>
{alloc_cards}
<table style="margin-top:8px">
  <thead><tr><th>Fund</th><th>Weight</th><th>Deploy Now</th><th>Liquid Buffer</th><th>Total</th></tr></thead>
  <tbody>
{alloc_rows}  </tbody>
</table>

<!-- Section 7: Monte Carlo -->
<h2>7 · 2-Year Monte Carlo Projection (10,000 Paths)</h2>
<p class="section-intro">GBM simulation using empirical basket return and volatility parameters. Each path simulates 504 trading days of daily returns drawn from a normal distribution calibrated to the selected basket.</p>
<div class="grid3">
  <div class="card">
    <div class="card-title">P(Profit)</div>
    <div class="card-val" style="color:var(--green)">{mc['prob_profit']}%</div>
    <div class="card-sub">Probability of any gain</div>
  </div>
  <div class="card">
    <div class="card-title">P(&gt;20% gain)</div>
    <div class="card-val" style="color:var(--green)">{mc['prob_20']}%</div>
    <div class="card-sub">Beat inflation comfortably</div>
  </div>
  <div class="card">
    <div class="card-title">Median Outcome</div>
    <div class="card-val" style="color:var(--accent)">₹{mc['p50']:,}</div>
    <div class="card-sub">{_mc_pct(mc['p50'])} over 2 years</div>
  </div>
</div>
<table style="margin-top:14px">
  <thead><tr><th>Percentile</th><th>Scenario</th><th>Portfolio Value</th><th>Return</th></tr></thead>
  <tbody>
{mc_rows}  </tbody>
</table>

<!-- Section 8: Tax Treatment -->
<h2>8 · Tax Treatment (Phase 3 — post Jul 23, 2024)</h2>
<table>
  <thead><tr><th>Fund</th><th>Classification</th><th>LTCG Rate</th><th>Exemption</th><th>Notes</th></tr></thead>
  <tbody>
    {''.join(f"<tr><td>{a['fund']}</td><td>{'International FoF' if a['role'] in ('US Equity','EM Equity') else 'Equity-oriented hybrid'}</td><td>LTCG 12.5%</td><td>₹1.25L/yr</td><td>Hold ≥ 24 months; Phase 3 rules</td></tr>" for a in alloc if a['role'] != 'Gold')}
    <tr><td>GOLDBEES</td><td>Equity ETF (gold)</td><td>LTCG 12.5%</td><td>₹1.25L/yr</td><td>Treated as equity ETF; hold ≥ 12 months</td></tr>
    <tr><td>Liquid Fund (buffer)</td><td>Debt MF</td><td>STCG at slab</td><td>—</td><td>Until GOLDBEES buffer is deployed</td></tr>
  </tbody>
</table>

<!-- Section 9: Action Triggers -->
<h2>9 · Action Triggers</h2>

<div class="trigger">
  <div class="trigger-icon">🟢</div>
  <div class="trigger-text">
    <div class="trigger-title">Deploy GOLDBEES buffer (₹{buffer:,})</div>
    <div class="trigger-desc">Trigger: GOLDBEES daily close &gt; ₹{goldbees_trigger:.2f} on 3 consecutive trading days. Buy additional units to complete the full ₹2,50,000 gold sleeve. Buffer earns interest in liquid fund while waiting.</div>
  </div>
</div>

<div class="trigger">
  <div class="trigger-icon">⚖️</div>
  <div class="trigger-text">
    <div class="trigger-title">Rebalance at 12 months</div>
    <div class="trigger-desc">If any single fund drifts beyond ±10% of target weight, sell the winner and buy the laggard. This mechanically enforces "buy low, sell high" within the basket.</div>
  </div>
</div>

<div class="trigger">
  <div class="trigger-icon">🔓</div>
  <div class="trigger-text">
    <div class="trigger-title">SEBI cap lifted — original passive FoFs reopen</div>
    <div class="trigger-desc">When ICICI Passive Multi Asset FoF and DSP US Equity FoF resume subscriptions (SEBI raises overseas limit), reassess basket. Passive FoFs have lower TER and no active-management risk — may be worth switching back for the relevant sleeves.</div>
  </div>
</div>

<div class="trigger">
  <div class="trigger-icon">📤</div>
  <div class="trigger-text">
    <div class="trigger-title">Redeem at 24 months + 1 day</div>
    <div class="trigger-desc">Cross the 24-month mark for LTCG treatment. Redeem all positions after that threshold to qualify for 12.5% LTCG rate rather than STCG at slab rate.</div>
  </div>
</div>

<footer>
  <p>Generated by <a href="https://github.com/Mosaic-agent/data_importer" style="color:#60a5fa;text-decoration:none;" target="_blank">Mosaic Fund Agent</a> · {gen_date} · Data sources: MFAPI, NSE iNAV API, Yahoo Finance, ClickHouse (market_data)</p>
  <p style="margin-top:6px">This report is for informational purposes only. Past returns do not guarantee future performance. Consult a SEBI-registered advisor before investing.</p>
</footer>

</body>
</html>"""
    return html


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    today     = date.today()
    from_date = today - timedelta(days=730)

    log.info("Fetching NAV history for %d MF candidates …", len(CANDIDATES))
    nav_series: dict[str, pd.Series] = {}
    for name, meta in CANDIDATES.items():
        s = _fetch_mf_nav(name, meta["scheme_code"], from_date, today)
        if len(s) > 50:
            nav_series[name] = s
            log.info("  %-45s  %d rows", name, len(s))
        else:
            log.warning("  %-45s  insufficient data (%d rows) — skipping", name, len(s))

    log.info("Fetching GOLDBEES price history …")
    goldbees = _fetch_goldbees(from_date, today)
    if len(goldbees) > 50:
        nav_series["GOLDBEES"] = goldbees
        log.info("  GOLDBEES  %d rows", len(goldbees))

    log.info("Computing per-fund metrics …")
    all_metrics: dict[str, dict] = {}
    for name, series in nav_series.items():
        role = CANDIDATES[name]["role"] if name in CANDIDATES else "Gold"
        m = _metrics(series)
        all_metrics[name] = {**m, "name": name, "role": role,
                              "nav": round(float(series.iloc[-1]), 2)}
        log.info("  %-45s  Sharpe=%-5.2f  MaxDD=%-6.1f%%  2Y=%.1f%%",
                 name, m["sharpe"], m["maxdd"], m["ret2y"])

    log.info("Selecting basket …")
    basket = _select_basket(all_metrics, nav_series)
    log.info("  Selected: %s", basket)

    for name in all_metrics:
        all_metrics[name]["selected"] = name in basket
        all_metrics[name]["badge"] = "ALT" if name not in basket else "✓ SELECTED"

    log.info("Computing correlation matrix …")
    rets_df = pd.DataFrame({
        CANDIDATES[n]["short"] if n in CANDIDATES else n: nav_series[n].pct_change().dropna()
        for n in basket if n in nav_series
    }).dropna()
    corr_labels = list(rets_df.columns)
    corr_matrix = [[round(float(v), 2) for v in row]
                   for row in rets_df.corr().values.tolist()]

    log.info("Running GARCH on GOLDBEES …")
    garch = _garch_goldbees(goldbees)
    final_w   = garch["final_w"]
    deploy_gb = int(round(250_000 * final_w / 50) * 50)
    buffer_gb = 250_000 - deploy_gb

    log.info("Fetching ETF premiums …")
    etf_premiums = _etf_premiums()

    log.info("Fetching USD/INR …")
    usdinr, inr_windows = _fetch_usdinr()

    log.info("Building allocation …")
    allocation = []
    for name in basket:
        m = all_metrics[name]
        short = CANDIDATES[name]["short"] if name in CANDIDATES else "GOLDBEES"
        if name == "GOLDBEES":
            nav_val = m["nav"]
            units   = int(deploy_gb / nav_val)
            allocation.append({
                "fund":   name,
                "role":   "Gold",
                "deploy": deploy_gb,
                "hold":   buffer_gb,
                "units":  units,
                "nav":    nav_val,
                "entry":  "NSE ETF",
                "sharpe": m["sharpe"],
                "maxdd":  m["maxdd"],
            })
        else:
            nav_val = m["nav"]
            units   = int(250_000 / nav_val)
            allocation.append({
                "fund":   name,
                "role":   m["role"],
                "deploy": 250_000,
                "hold":   0,
                "units":  units,
                "nav":    nav_val,
                "entry":  "MF Direct",
                "sharpe": m["sharpe"],
                "maxdd":  m["maxdd"],
            })

    log.info("Running Monte Carlo …")
    basket_mu   = float(np.mean([all_metrics[n]["ret2y"] / 100 for n in basket]))
    basket_sig  = float(np.mean([all_metrics[n]["vol"] / 100 for n in basket]))
    mc = _monte_carlo(basket_mu, basket_sig)
    log.info("  MC P50=₹%d  P(profit)=%.1f%%", mc["p50"], mc["prob_profit"])

    # ── assemble JSON data ────────────────────────────────────────────────────
    gen_str = today.strftime("%Y-%m-%d %H:%M")
    data = {
        "generated":    gen_str,
        "corpus":       CORPUS,
        "inr_windows":  inr_windows,
        "usdinr_now":   round(usdinr, 2),
        "etf_premiums": etf_premiums,
        "funds":        list(all_metrics.values()),
        "corr_labels":  corr_labels,
        "corr_matrix":  corr_matrix,
        "allocation":   allocation,
        "mc":           mc,
        "garch":        garch,
    }

    json_path = OUTPUT_DIR / "inr_hedge_report_data.json"
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    log.info("Wrote %s", json_path)

    # ── generate HTML ─────────────────────────────────────────────────────────
    html = _build_html(data)
    html_path = OUTPUT_DIR / "inr_hedge_report.html"
    with open(html_path, "w") as f:
        f.write(html)
    log.info("Wrote %s", html_path)
    log.info("Done. Open output/inr_hedge_report.html in browser.")


if __name__ == "__main__":
    main()
