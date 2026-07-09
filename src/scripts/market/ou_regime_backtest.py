"""
src/scripts/market/ou_regime_backtest.py
─────────────────────────────────────────
ETF Premium Strategy Backtest — PELT regime detection + mean-reversion optimal switching for International ETF Premiums.

Strategy
────────
Trades the premium-to-iNAV of MON100 (or any tracked international ETF) using:
  1. PELT change-point detection to find the current stationary regime window
  2. ADF + KPSS stationarity gate — OU model is ONLY applied when stationary
  3. ZJL optimal double-stopping thresholds (b*, s*) from the OU fit
  4. Event-flag override stub (default: all zeros)

Pipeline per day:
  premiums[0..t]
    → PELT → latest segment
    → ADF + KPSS gate
    → if stationary: fit_ou → ZJL → trade on b*/s*
    → if not stationary: NON_STATIONARY → hold
    → if event_flag: TRANSITION → floor exposure

This is a POSITION-SIZING OVERLAY. It does NOT model the underlying NASDAQ-100
index return. P&L reported = premium-harvesting P&L only.

Usage
─────
    PYTHONPATH=. python src/scripts/market/ou_regime_backtest.py \\
        --symbol MON100 --start 2023-01-01 --end 2026-07-09

    PYTHONPATH=. python src/scripts/market/ou_regime_backtest.py \\
        --symbol MAFANG --start 2023-06-01 \\
        --c-buy 15 --c-sell 15 --pen-multiplier 2.0

    # Use a CSV override instead of ClickHouse:
    PYTHONPATH=. python src/scripts/market/ou_regime_backtest.py \\
        --symbol MON100 --csv-path data/mon100_premium.csv
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import warnings
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Path setup ────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))
os.environ.setdefault("ALLOW_LOCAL_RUN", "1")

from src.ml.premium_regime import (
    detect_regime, RegimeState,
    STATUS_STATIONARY, STATUS_NON_STATIONARY, STATUS_INSUFFICIENT_DATA,
    STATUS_STRUCTURAL_SHIFT,
)

log = logging.getLogger(__name__)


def setup_logging(log_level: str = "WARNING") -> None:
    """Configure root logger — stdlib only, no external deps."""
    numeric = getattr(logging, log_level.upper(), logging.WARNING)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

# ── Constants ─────────────────────────────────────────────────────────────────
NOTIONAL_DEFAULT = 1_000_000           # ₹10,00,000
BURNIN_DEFAULT   = 90                  # trading days
FLOOR_EXPOSURE   = 0.20               # exposure during TRANSITION events
OUTPUT_DIR       = _ROOT / "output" / "reports"

# ── Data loading ──────────────────────────────────────────────────────────────

def _load_from_clickhouse(symbol: str, start: date, end: date) -> pd.DataFrame | None:
    """
    Query market_data.inav_snapshots for daily premium series.
    Returns DataFrame[date, price, inav, premium_pct] or None if unavailable.
    """
    try:
        from src.db.pool import query_df
        sql = """
            SELECT
                toDate(snapshot_at) AS date,
                argMax(market_price,  snapshot_at) AS price,
                argMax(inav,          snapshot_at) AS inav_val,
                argMax(premium_discount_pct, snapshot_at) AS premium_pct
            FROM market_data.inav_snapshots FINAL
            WHERE symbol = %(sym)s
              AND toDate(snapshot_at) BETWEEN %(start)s AND %(end)s
              AND inav > 0
            GROUP BY date
            ORDER BY date
        """
        df = query_df(sql, {"sym": symbol, "start": str(start), "end": str(end)})
        if df.empty:
            return None
        df = df.rename(columns={"inav_val": "inav"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        for col in ("price", "inav", "premium_pct"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["premium_pct"]).reset_index(drop=True)
        return df if len(df) >= 2 else None
    except Exception as exc:
        log.warning("ClickHouse query failed: %s", exc)
        return None


def _load_from_csv(csv_path: str) -> pd.DataFrame:
    """Load premium series from a CSV file with columns: date, price, inav, premium_pct."""
    df = pd.read_csv(csv_path, parse_dates=["date"])
    df["date"] = df["date"].dt.date
    # Compute premium_pct if not present but price+inav are
    if "premium_pct" not in df.columns and {"price", "inav"}.issubset(df.columns):
        df["premium_pct"] = (df["price"] - df["inav"]) / df["inav"] * 100
    for col in ("price", "inav", "premium_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["premium_pct"]).sort_values("date").reset_index(drop=True)
    return df


def _load_fallback(symbol: str, days: int) -> pd.DataFrame | None:
    """Fall back to historic_inav (MFAPI + yfinance) if ClickHouse is unavailable."""
    try:
        from src.tools.historic_inav import get_historic_inav
        result = get_historic_inav(symbol, days=days)
        records = result.get("records", [])
        if not records:
            return None
        rows = []
        for r in records:
            try:
                d = r.get("date") or r.get("Date")
                nav = float(r.get("nav") or r.get("inav") or 0)
                close = float(r.get("market_close") or r.get("price") or 0)
                prem = float(r.get("premium_discount_pct") or 0)
                if nav > 0 and close > 0:
                    rows.append({"date": d, "price": close, "inav": nav, "premium_pct": prem})
            except (ValueError, TypeError):
                continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)
        return df if len(df) >= 2 else None
    except Exception as exc:
        log.warning("Fallback data load failed: %s", exc)
        return None


def load_data(
    symbol: str,
    start: date,
    end: date,
    csv_path: str | None = None,
) -> pd.DataFrame:
    """
    Load daily premium series. Priority:
      1. --csv-path override (if provided)
      2. ClickHouse inav_snapshots
      3. historic_inav fallback (MFAPI + yfinance)
    """
    if csv_path:
        log.info("Loading from CSV: %s", csv_path)
        df = _load_from_csv(csv_path)
        if df.empty:
            raise ValueError(f"CSV file {csv_path} produced no usable rows")
        return df

    log.info("Querying ClickHouse for %s (%s to %s)…", symbol, start, end)
    df = _load_from_clickhouse(symbol, start, end)
    if df is not None and len(df) >= 30:
        log.info("ClickHouse: %d rows for %s", len(df), symbol)
        return df

    days = (end - start).days + 30
    log.info("ClickHouse returned insufficient data — trying fallback (days=%d)…", days)
    df = _load_fallback(symbol, days)
    if df is not None and len(df) >= 30:
        log.info("Fallback: %d rows for %s", len(df), symbol)
        return df

    raise RuntimeError(
        f"No premium data found for {symbol} between {start} and {end}.\n"
        f"Options:\n"
        f"  1. Import data: PYTHONPATH=. python src/main.py import --category etfs\n"
        f"  2. Provide a CSV: --csv-path /path/to/{symbol.lower()}_premium.csv\n"
        f"     Columns required: date, price, inav  (or date, price, inav, premium_pct)\n"
    )


def load_event_flags(event_flags_csv: str | None, dates: list[date]) -> dict[date, int]:
    """
    Load event flags from CSV (columns: date, event_flag).
    Default: all zeros (stub — plug in real SEBI-cap dates to activate).
    """
    flags: dict[date, int] = {d: 0 for d in dates}
    if event_flags_csv and Path(event_flags_csv).exists():
        df = pd.read_csv(event_flags_csv, parse_dates=["date"])
        for _, row in df.iterrows():
            d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
            flags[d] = int(row.get("event_flag", 0))
        log.info("Loaded %d event-flag dates from %s", len(df), event_flags_csv)
    else:
        if event_flags_csv:
            log.warning("Event-flag CSV not found: %s — defaulting to all zeros", event_flags_csv)
        else:
            log.info("No event-flag CSV provided — stub active (all zeros).")
    return flags


# ── Backtest engine ───────────────────────────────────────────────────────────

@dataclass
class Trade:
    entry_date:   date
    exit_date:    date | None
    entry_premium: float
    exit_premium:  float | None
    pnl_pp:       float | None   # P&L in percentage points (net of cost)
    holding_days: int


@dataclass
class BacktestResult:
    equity_curve_pp: pd.Series    # cumulative P&L in percentage points
    equity_curve_inr: pd.Series   # cumulative P&L in ₹
    daily_pnl: pd.Series          # daily P&L (pp)
    regime_log: pd.DataFrame      # date, status, exposure, adf_p, kpss_p, confidence, b_star, s_star
    trades: list[Trade]
    params: dict


def run_backtest(
    df: pd.DataFrame,
    event_flags: dict[date, int],
    notional: float = NOTIONAL_DEFAULT,
    burnin: int = BURNIN_DEFAULT,
    pen_multiplier: float = 3.0,
    min_segment: int = 30,
    c_buy_bps: float = 10.0,
    c_sell_bps: float = 10.0,
    discount_rate_annual: float = 0.05,
    floor_exposure: float = FLOOR_EXPOSURE,
    label: str = "base",
    refit_every: int = 5,
    confidence_threshold: float = 0.0,
) -> BacktestResult:
    """
    Walk-forward backtest. No parameter estimated using data after trade date.

    Parameters
    ----------
    df               : DataFrame with columns [date, premium_pct, price, inav]
    event_flags      : dict date → 0/1
    burnin           : first N rows are burn-in (no trading)
    pen_multiplier   : PELT penalty = pen_multiplier × var(premiums)
    min_segment      : min obs in PELT segment for stationarity test
    c_buy_bps        : entry transaction cost in bps
    c_sell_bps       : exit transaction cost in bps
    discount_rate_annual : annual discount rate for ZJL DP
    floor_exposure   : exposure during TRANSITION events
    label            : run label for reporting
    refit_every      : rerun PELT+ADF/KPSS+ZJL only every N days (default 5).
                       Between refits the last regime state is reused — trade
                       decisions still use today's live premium vs cached b*/s*.
                       Set to 1 for full daily refits (accurate but slow).
    confidence_threshold : only trade when confidence >= this value (0–100).
                       Default 0.0 = no gate. Recommended: 50–70.
    """
    r_daily = discount_rate_annual / 252.0
    c_buy   = c_buy_bps  / 100.0   # bps → pp
    c_sell  = c_sell_bps / 100.0

    premiums = df["premium_pct"].to_numpy(dtype=float)
    prices   = df["price"].to_numpy(dtype=float)
    dates    = list(df["date"])
    n        = len(premiums)

    # State
    exposure        = 0.0   # current position: 0.0 or 1.0 (or floor)
    cumulative_pp   = 0.0
    entry_premium   = None
    entry_date      = None
    theta_history: list[float] = []

    # Regime cache — refit only every `refit_every` days to bound runtime.
    # The regime state (b*, s*, status) is stable over days; daily premium
    # values are still used live for trade decisions.
    _cached_rs: RegimeState | None = None
    _last_refit_t: int = -1

    # Output containers
    daily_pnl_list  = []
    regime_rows     = []
    trades          = []
    equity_curve    = []

    for t in range(n):
        d          = dates[t]
        prem_t     = premiums[t]
        evt_flag   = event_flags.get(d, 0)

        # ── Detect regime on data[0..t] (strictly causal) ────────────────────
        if t < burnin:
            # Burn-in: no trading, no regime detection
            status, b_star, s_star, conf, adf_p, kpss_p = "BURNIN", None, None, 0.0, None, None
        else:
            # Only refit when: first post-burnin day, or refit_every days elapsed
            _due_for_refit = (
                _cached_rs is None
                or (t - _last_refit_t) >= refit_every
            )
            if _due_for_refit:
                _cached_rs = detect_regime(
                    premiums=premiums[:t+1],
                    dates=dates[:t+1],
                    pen_multiplier=pen_multiplier,
                    min_segment=min_segment,
                    r_daily=r_daily,
                    c_buy_bps=c_buy_bps,
                    c_sell_bps=c_sell_bps,
                    theta_history=theta_history,
                )
                _last_refit_t = t
                if _cached_rs.ou is not None:
                    theta_history = _cached_rs.theta_history

            rs = _cached_rs
            status = rs.status
            adf_p  = rs.adf_pvalue
            kpss_p = rs.kpss_pvalue
            conf   = rs.confidence
            b_star = rs.dstop.b_star if rs.dstop else None
            s_star = rs.dstop.s_star if rs.dstop else None

        # ── Decision logic (4 market states) ─────────────────────────────────
        # 1. CHEAP       : premium ≤ b* → buy aggressively
        # 2. FAIR        : b* < premium < s* → hold current position
        # 3. EXPENSIVE   : premium ≥ s* → sell
        # 4. STRUCTURAL_SHIFT / NON_STATIONARY : OU invalid → do nothing
        prev_exposure = exposure

        if t < burnin:
            target_exposure = 0.0   # no trading during burn-in
        elif evt_flag == 1:
            target_exposure = floor_exposure   # TRANSITION: event-driven floor
            status = "TRANSITION"
        elif status in (STATUS_STRUCTURAL_SHIFT,):
            # Structural shift: new PELT break detected, OU is invalid.
            # Do nothing — wait for new equilibrium to establish.
            target_exposure = exposure
        elif status in (STATUS_NON_STATIONARY, STATUS_INSUFFICIENT_DATA):
            target_exposure = exposure          # hold current (no forced trade)
        elif status == STATUS_STATIONARY and b_star is not None and s_star is not None:
            # Confidence gate: only trade when confidence exceeds threshold
            if conf < confidence_threshold:
                target_exposure = exposure       # low confidence → hold, don't enter
                status = "LOW_CONFIDENCE"
            elif exposure == 0.0 and prem_t <= b_star:
                target_exposure = 1.0           # CHEAP: buy aggressively
                status = "CHEAP"
            elif exposure == 1.0 and prem_t >= s_star:
                target_exposure = 0.0           # EXPENSIVE: sell
                status = "EXPENSIVE"
            else:
                target_exposure = exposure      # FAIR: inside the band
                status = "FAIR"
        else:
            target_exposure = exposure

        # ── P&L for today ─────────────────────────────────────────────────────
        # Premium P&L = exposure × daily change in premium
        if t == 0:
            delta_prem = 0.0
        else:
            delta_prem = prem_t - premiums[t - 1]

        # Transaction costs on position changes
        cost = 0.0
        if target_exposure != prev_exposure:
            if target_exposure > prev_exposure:   # entering
                cost = c_buy * (target_exposure - prev_exposure)
            else:                                  # exiting
                cost = c_sell * (prev_exposure - target_exposure)

        day_pnl = exposure * delta_prem - cost   # exposure locked at start-of-day
        cumulative_pp += day_pnl

        equity_curve.append(cumulative_pp)
        daily_pnl_list.append(day_pnl)

        regime_rows.append({
            "date":       d,
            "premium_pct": prem_t,
            "status":     status,
            "exposure":   exposure,
            "b_star":     b_star,
            "s_star":     s_star,
            "adf_p":      adf_p,
            "kpss_p":     kpss_p,
            "confidence": conf,
            "day_pnl_pp": day_pnl,
        })

        # Track open trade entry/exit
        if target_exposure == 1.0 and prev_exposure == 0.0:
            entry_premium = prem_t
            entry_date    = d
        elif target_exposure == 0.0 and prev_exposure == 1.0 and entry_premium is not None:
            pnl = prem_t - entry_premium - c_buy - c_sell
            holding = (d - entry_date).days
            trades.append(Trade(
                entry_date=entry_date,
                exit_date=d,
                entry_premium=entry_premium,
                exit_premium=prem_t,
                pnl_pp=round(pnl, 4),
                holding_days=holding,
            ))
            entry_premium = None
            entry_date    = None

        # Update exposure for next day
        exposure = target_exposure

    # Close open position at end
    if entry_premium is not None and n > 0:
        pnl = premiums[-1] - entry_premium - c_buy
        trades.append(Trade(
            entry_date=entry_date,
            exit_date=dates[-1],
            entry_premium=entry_premium,
            exit_premium=premiums[-1],
            pnl_pp=round(pnl, 4),
            holding_days=(dates[-1] - entry_date).days,
        ))

    idx = pd.Index(dates, name="date")
    return BacktestResult(
        equity_curve_pp=pd.Series(equity_curve, index=idx, name="cum_pnl_pp"),
        equity_curve_inr=pd.Series(
            [x * notional / 100.0 for x in equity_curve], index=idx, name="cum_pnl_inr"
        ),
        daily_pnl=pd.Series(daily_pnl_list, index=idx, name="daily_pnl_pp"),
        regime_log=pd.DataFrame(regime_rows).set_index("date"),
        trades=trades,
        params={
            "label": label,
            "pen_multiplier": pen_multiplier,
            "c_buy_bps": c_buy_bps,
            "c_sell_bps": c_sell_bps,
            "burnin": burnin,
            "notional": notional,
            "discount_rate": discount_rate_annual,
            "confidence_threshold": confidence_threshold,
        },
    )


# ── Metrics ───────────────────────────────────────────────────────────────────

def _max_drawdown(equity: np.ndarray) -> float:
    """Maximum peak-to-trough drawdown in percentage points."""
    peak = -np.inf
    mdd  = 0.0
    for v in equity:
        peak = max(peak, v)
        dd   = peak - v
        mdd  = max(mdd, dd)
    return mdd


def compute_metrics(result: BacktestResult, df: pd.DataFrame) -> dict:
    """Compute full performance metrics for a backtest result."""
    pnl = result.daily_pnl.to_numpy()
    eq  = result.equity_curve_pp.to_numpy()
    n   = len(pnl)

    # Sharpe (annualised on premium P&L stream, not index return)
    mean_daily  = float(np.mean(pnl))
    std_daily   = float(np.std(pnl, ddof=1))
    sharpe      = (mean_daily / std_daily * math.sqrt(252)) if std_daily > 1e-10 else 0.0

    # Trades
    completed   = [t for t in result.trades if t.pnl_pp is not None]
    n_trips     = len(completed)
    winners     = [t for t in completed if t.pnl_pp > 0]
    win_rate    = len(winners) / n_trips if n_trips else 0.0
    avg_pnl     = float(np.mean([t.pnl_pp for t in completed])) if completed else 0.0
    avg_hold    = float(np.mean([t.holding_days for t in completed])) if completed else 0.0

    # Drawdown
    mdd         = _max_drawdown(eq)

    # Regime breakdown
    rl = result.regime_log
    regime_counts = rl["status"].value_counts().to_dict()
    regime_pct    = {k: round(v / n * 100, 1) for k, v in regime_counts.items()}

    # Benchmark A: buy-and-hold premium P&L
    # (This is NOT the index return — it's just holding 100% exposure to the premium all the time)
    prem = df["premium_pct"].to_numpy()
    bh_pnl = float(prem[-1] - prem[0]) if len(prem) >= 2 else 0.0

    # Benchmark B: naive ±1.5σ static bands (pre-computed once on full history)
    prem_std = float(np.std(prem))
    prem_mu  = float(np.mean(prem))
    naive_b  = prem_mu - 1.5 * prem_std
    naive_s  = prem_mu + 1.5 * prem_std

    total_pnl = float(eq[-1]) if len(eq) > 0 else 0.0

    return {
        "label":            result.params["label"],
        "n_days":           n,
        "total_pnl_pp":     round(total_pnl, 4),
        "total_pnl_inr":    round(total_pnl * result.params["notional"] / 100, 2),
        "sharpe":           round(sharpe, 3),
        "max_drawdown_pp":  round(mdd, 4),
        "n_round_trips":    n_trips,
        "win_rate":         round(win_rate * 100, 1),
        "avg_pnl_pp":       round(avg_pnl, 4),
        "avg_holding_days": round(avg_hold, 1),
        "regime_pct":       regime_pct,
        "bmark_bh_pnl_pp":  round(bh_pnl, 4),
        "naive_b_star":     round(naive_b, 3),
        "naive_s_star":     round(naive_s, 3),
        "pen_multiplier":   result.params["pen_multiplier"],
        "c_buy_bps":        result.params["c_buy_bps"],
        "c_sell_bps":       result.params["c_sell_bps"],
    }


# ── Benchmark B: naive static bands ──────────────────────────────────────────

def run_naive_backtest(
    df: pd.DataFrame,
    event_flags: dict[date, int],
    notional: float = NOTIONAL_DEFAULT,
    burnin: int = BURNIN_DEFAULT,
    c_buy_bps: float = 10.0,
    c_sell_bps: float = 10.0,
    sigma_mult: float = 1.5,
) -> dict:
    """
    Benchmark B: naive strategy using fixed ±σ_mult·std bands computed once
    on the pre-burnin window (strictly no lookahead).
    """
    premiums = df["premium_pct"].to_numpy()
    dates    = list(df["date"])
    n        = len(premiums)
    c_buy    = c_buy_bps  / 100.0
    c_sell   = c_sell_bps / 100.0

    seed = premiums[:burnin]
    mu_s = float(np.mean(seed))
    sg_s = float(np.std(seed))
    b_naive = mu_s - sigma_mult * sg_s
    s_naive = mu_s + sigma_mult * sg_s

    exposure = 0.0
    cum_pnl  = 0.0
    pnls     = []

    for t in range(n):
        prem_t = premiums[t]
        prev   = exposure

        if t < burnin:
            target = 0.0
        elif exposure == 0.0 and prem_t <= b_naive:
            target = 1.0
        elif exposure == 1.0 and prem_t >= s_naive:
            target = 0.0
        else:
            target = exposure

        cost = 0.0
        if target > prev:
            cost = c_buy * (target - prev)
        elif target < prev:
            cost = c_sell * (prev - target)

        delta = (premiums[t] - premiums[t-1]) if t > 0 else 0.0
        day_pnl = exposure * delta - cost
        cum_pnl += day_pnl
        pnls.append(day_pnl)
        exposure = target

    pnl_arr = np.array(pnls)
    mean_d  = float(np.mean(pnl_arr))
    std_d   = float(np.std(pnl_arr, ddof=1))
    sharpe  = mean_d / std_d * math.sqrt(252) if std_d > 1e-10 else 0.0
    mdd     = _max_drawdown(np.cumsum(pnl_arr))

    return {
        "label":           f"naive_±{sigma_mult}σ",
        "b_naive": round(b_naive, 3),
        "s_naive": round(s_naive, 3),
        "total_pnl_pp": round(float(cum_pnl), 4),
        "total_pnl_inr": round(float(cum_pnl) * notional / 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pp": round(mdd, 4),
    }


# ── Plotting ──────────────────────────────────────────────────────────────────

def plot_results(
    symbol: str,
    df: pd.DataFrame,
    base: BacktestResult,
    sens_results: list[tuple[BacktestResult, dict]],
    output_path: Path,
) -> None:
    """
    4-panel Matplotlib chart saved as high-DPI PNG.

    Panel 1 — Premium series with regime shading + live b*/s* threshold lines
    Panel 2 — Cumulative P&L (pp) with trade markers (buy ▲ / sell ▼)
    Panel 3 — Daily P&L bar chart
    Panel 4 — Sensitivity: equity curves for different pen_multiplier values
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib.patches as mpatches
        from matplotlib.gridspec import GridSpec

        # ── colour palette (RGBA 0-1) ─────────────────────────────────────────
        _C = {
            STATUS_STATIONARY:        (0.14, 0.53, 0.21, 0.18),
            STATUS_NON_STATIONARY:    (0.85, 0.21, 0.20, 0.14),
            STATUS_INSUFFICIENT_DATA: (0.62, 0.42, 0.01, 0.15),
            STATUS_STRUCTURAL_SHIFT:  (1.00, 0.55, 0.00, 0.20),
            "TRANSITION":             (0.54, 0.34, 0.90, 0.16),
            "BURNIN":                 (0.39, 0.39, 0.39, 0.10),
            "CHEAP":                  (0.14, 0.53, 0.21, 0.25),
            "FAIR":                   (0.14, 0.53, 0.21, 0.10),
            "EXPENSIVE":              (0.97, 0.32, 0.29, 0.20),
            "LOW_CONFIDENCE":         (0.62, 0.42, 0.01, 0.12),
        }
        GREEN  = "#3fb950"
        RED    = "#f85149"
        GREY   = "#888888"

        rl        = base.regime_log
        dates_idx = list(base.equity_curve_pp.index)
        prem_df   = df.set_index("date")

        # Convert date objects to matplotlib-friendly format
        import datetime as _dt
        def _to_dt(d):
            return _dt.datetime(d.year, d.month, d.day) if not isinstance(d, _dt.datetime) else d
        x = [_to_dt(d) for d in dates_idx]

        # ── Figure layout ─────────────────────────────────────────────────────
        fig = plt.figure(figsize=(16, 20), dpi=150)
        fig.patch.set_facecolor("#0d1117")
        gs = GridSpec(4, 1, figure=fig,
                      height_ratios=[0.27, 0.25, 0.20, 0.28],
                      hspace=0.10)
        axes = [fig.add_subplot(gs[i]) for i in range(4)]

        # common dark-mode style
        for ax in axes:
            ax.set_facecolor("#161b22")
            ax.tick_params(colors="#c9d1d9", labelsize=9)
            ax.yaxis.label.set_color("#c9d1d9")
            ax.xaxis.label.set_color("#c9d1d9")
            for spine in ax.spines.values():
                spine.set_edgecolor("#30363d")
            ax.grid(axis="y", color="#21262d", linewidth=0.6, linestyle="--")
            ax.grid(axis="x", color="#21262d", linewidth=0.4, linestyle=":")
            if ax is not axes[-1]:
                ax.tick_params(labelbottom=False)

        def _shade_regimes(ax):
            prev_status, seg_start = None, None
            for d, row in rl.iterrows():
                st = row["status"]
                if st != prev_status:
                    if prev_status is not None and seg_start is not None:
                        ax.axvspan(_to_dt(seg_start), _to_dt(d),
                                   color=_C.get(prev_status, (0, 0, 0, 0)),
                                   linewidth=0, zorder=0)
                    seg_start, prev_status = d, st
            if prev_status and seg_start:
                ax.axvspan(_to_dt(seg_start), _to_dt(dates_idx[-1]),
                           color=_C.get(prev_status, (0, 0, 0, 0)),
                           linewidth=0, zorder=0)

        # ── Panel 1: Premium + b*/s* + regime shading ─────────────────────────
        ax1 = axes[0]
        premiums_series = [prem_df.loc[d, "premium_pct"] if d in prem_df.index else float("nan")
                           for d in dates_idx]
        b_vals = [rl.loc[d, "b_star"] if d in rl.index else float("nan") for d in dates_idx]
        s_vals = [rl.loc[d, "s_star"] if d in rl.index else float("nan") for d in dates_idx]

        _shade_regimes(ax1)
        ax1.plot(x, premiums_series, color="#58a6ff", linewidth=1.4, zorder=3, label="Premium %")
        ax1.plot(x, b_vals, color=GREEN, linewidth=1.3, linestyle=":", zorder=3, label="b* buy")
        ax1.plot(x, s_vals, color=RED,   linewidth=1.3, linestyle=":", zorder=3, label="s* sell")
        ax1.axhline(0, color=GREY, linewidth=0.7, linestyle="--", alpha=0.5)
        ax1.set_ylabel("Premium (%)", fontsize=9)
        ax1.set_title(f"{symbol} — Premium to iNAV (%)", fontsize=10,
                      color="#e6edf3", pad=4, loc="left")
        ax1.legend(fontsize=8, loc="upper left", framealpha=0.3,
                   labelcolor="#c9d1d9", facecolor="#161b22", edgecolor="#30363d")

        # ── Panel 2: Equity curve + trade markers ─────────────────────────────
        ax2 = axes[1]
        eq_vals = list(base.equity_curve_pp.values)
        _shade_regimes(ax2)
        ax2.plot(x, eq_vals, color=GREEN, linewidth=1.8, zorder=3, label="Premium Strategy P&L")
        ax2.fill_between(x, eq_vals, 0, where=[v >= 0 for v in eq_vals],
                         color=GREEN, alpha=0.08, zorder=2)
        ax2.fill_between(x, eq_vals, 0, where=[v < 0 for v in eq_vals],
                         color=RED,   alpha=0.08, zorder=2)
        ax2.axhline(0, color=GREY, linewidth=0.7, linestyle="--", alpha=0.5)

        buy_x  = [_to_dt(t.entry_date) for t in base.trades if t.entry_date and t.entry_date in dates_idx]
        buy_y  = [eq_vals[dates_idx.index(t.entry_date)] for t in base.trades
                  if t.entry_date and t.entry_date in dates_idx]
        sell_x = [_to_dt(t.exit_date) for t in base.trades if t.exit_date and t.exit_date in dates_idx]
        sell_y = [eq_vals[dates_idx.index(t.exit_date)] for t in base.trades
                  if t.exit_date and t.exit_date in dates_idx]
        if buy_x:
            ax2.scatter(buy_x,  buy_y,  marker="^", s=35, color=GREEN, zorder=4,
                        alpha=0.85, label="Buy entry")
        if sell_x:
            ax2.scatter(sell_x, sell_y, marker="v", s=35, color=RED,   zorder=4,
                        alpha=0.85, label="Sell exit")

        ax2.set_ylabel("Cum P&L (pp)", fontsize=9)
        ax2.set_title("Cumulative P&L (pp, premium overlay only)", fontsize=10,
                      color="#e6edf3", pad=4, loc="left")
        ax2.legend(fontsize=8, loc="upper left", framealpha=0.3,
                   labelcolor="#c9d1d9", facecolor="#161b22", edgecolor="#30363d")

        # ── Panel 3: Daily P&L bars ───────────────────────────────────────────
        ax3 = axes[2]
        dpnl = list(base.daily_pnl.values)
        bar_colors = [GREEN if v >= 0 else RED for v in dpnl]
        ax3.bar(x, dpnl, color=bar_colors, width=1.0, linewidth=0, alpha=0.75, zorder=3)
        ax3.axhline(0, color=GREY, linewidth=0.7, linestyle="--", alpha=0.5)
        ax3.set_ylabel("Daily P&L (pp)", fontsize=9)
        ax3.set_title("Daily P&L (pp)", fontsize=10, color="#e6edf3", pad=4, loc="left")
        ax3.tick_params(axis="y", labelsize=8)

        # ── Panel 4: Sensitivity ──────────────────────────────────────────────
        ax4 = axes[3]
        _shade_regimes(ax4)
        ax4.plot(x, eq_vals, color="#58a6ff", linewidth=2.0, zorder=3,
                 label=f"Base pen×{base.params['pen_multiplier']:.1f}")
        _linestyles = ["--", ":", "-."]
        _sens_colors = ["#f0883e", "#a371f7", "#79c0ff"]
        for i, (sr, _sm) in enumerate(sens_results[:3]):
            sx = [_to_dt(d) for d in sr.equity_curve_pp.index]
            sy = list(sr.equity_curve_pp.values)
            ax4.plot(sx, sy, linewidth=1.6, linestyle=_linestyles[i % 3],
                     color=_sens_colors[i % 3], zorder=3, alpha=0.85,
                     label=sr.params["label"])
        ax4.axhline(0, color=GREY, linewidth=0.7, linestyle="--", alpha=0.5)
        ax4.set_ylabel("Cum P&L (pp)", fontsize=9)
        ax4.set_xlabel("Date", fontsize=9)
        ax4.set_title("Sensitivity — penalty multiplier comparison", fontsize=10,
                      color="#e6edf3", pad=4, loc="left")
        ax4.legend(fontsize=8, loc="upper left", framealpha=0.3,
                   labelcolor="#c9d1d9", facecolor="#161b22", edgecolor="#30363d")

        # ── X-axis date formatting (bottom panel only) ────────────────────────
        ax4.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax4.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax4.xaxis.set_minor_locator(mdates.MonthLocator())
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=30, ha="right")

        # ── Suptitle ──────────────────────────────────────────────────────────
        fig.suptitle(
            f"{symbol} — ETF Premium Strategy Backtest\n"
            f"Premium overlay P&L only · Does NOT include underlying index return",
            fontsize=12, color="#e6edf3", y=0.995, va="top",
        )

        # ── Save ──────────────────────────────────────────────────────────────
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(output_path), dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        log.info("PNG chart saved: %s", output_path)

    except Exception as exc:
        log.warning("Plotting failed: %s", exc)



# ── Report printer ────────────────────────────────────────────────────────────

def print_report(
    symbol: str,
    base_metrics: dict,
    naive_metrics: dict,
    sens_metrics: list[dict],
    base_result: BacktestResult,
    notional: float,
) -> None:
    """Print the full performance report to stdout."""
    sep = "─" * 72

    print(f"\n{'═'*72}")
    print(f"  ETF PREMIUM STRATEGY BACKTEST  —  {symbol}")
    print(f"{'═'*72}\n")

    def _pct_regime(rd: dict, total: int) -> str:
        parts = []
        for k, v in sorted(rd.items(), key=lambda x: -x[1]):
            parts.append(f"{k}: {v:.1f}%")
        return "  |  ".join(parts)

    m = base_metrics

    print(f"{'STRATEGY (Base Run)':}")
    print(sep)
    print(f"  Total P&L (premium overlay)  : {m['total_pnl_pp']:+.3f} pp  "
          f"  ₹{m['total_pnl_inr']:+,.0f}  (notional ₹{notional/1e5:.1f}L)")
    print(f"  Sharpe (annualised, premium)  : {m['sharpe']:+.3f}")
    print(f"  Max drawdown                  : {m['max_drawdown_pp']:.3f} pp")
    print(f"  Round trips                   : {m['n_round_trips']}")
    print(f"  Win rate                      : {m['win_rate']:.1f}%")
    print(f"  Avg P&L per round trip        : {m['avg_pnl_pp']:+.4f} pp")
    print(f"  Avg holding period            : {m['avg_holding_days']:.1f} days")
    print(f"  Pen multiplier                : {m['pen_multiplier']:.1f}")
    conf_thr = base_result.params.get("confidence_threshold", 0.0)
    if conf_thr > 0:
        print(f"  Confidence threshold          : {conf_thr:.0f}%")
    print(f"  Regime time breakdown:")
    for k, v in sorted(m["regime_pct"].items(), key=lambda x: -x[1]):
        print(f"    {k:<28}: {v:.1f}%")

    print(f"\n{'BENCHMARKS':}")
    print(sep)
    print(f"  A — Buy-and-hold premium P&L  : {m['bmark_bh_pnl_pp']:+.3f} pp  "
          f"(NOTE: this is premium-only drift, not NASDAQ-100 index return)")
    print(f"  B — Naive ±1.5σ bands         : {naive_metrics['total_pnl_pp']:+.3f} pp  "
          f"  Sharpe {naive_metrics['sharpe']:+.3f}  MDD {naive_metrics['max_drawdown_pp']:.3f} pp")
    print(f"      (static b*={naive_metrics['b_naive']:.2f}%, s*={naive_metrics['s_naive']:.2f}%, "
          f"computed on burn-in window only)")

    print(f"\n{'SENSITIVITY RUNS':}")
    print(sep)
    hdr = f"  {'Label':<28} {'P&L (pp)':>10} {'Sharpe':>8} {'MDD':>8} {'Trips':>7} {'WinR':>7}"
    print(hdr)
    print(f"  {'-'*68}")
    for sm in [m] + sens_metrics:
        row = (f"  {sm['label']:<28} {sm['total_pnl_pp']:>+10.3f} "
               f"{sm['sharpe']:>+8.3f} {sm['max_drawdown_pp']:>8.3f} "
               f"{sm['n_round_trips']:>7} {sm['win_rate']:>6.1f}%")
        print(row)

    print(f"\n{'⚠  DISCLAIMER & ASSUMPTIONS':}")
    print(sep)
    print("""
  1. PREMIUM OVERLAY ONLY — This backtest models premium-to-iNAV P&L exclusively.
     It does NOT include the underlying NASDAQ-100 index return. The investor is
     assumed to hold NASDAQ-100 exposure via an alternative sleeve (e.g. direct
     fund at NAV) during 0%-exposure periods. Do not conflate these figures.

  2. REGULATORY EDGE — The MON100 premium exists primarily because SEBI/RBI has
     capped Indian mutual-fund overseas investment limits. This is a policy-driven
     structural mispricing. The "edge" can end abruptly if/when the cap is lifted,
     increased significantly, or a large creation window opens. Historical backtest
     performance under one regulatory regime does NOT guarantee future performance.

  3. EVENT-FLAG STUB — No real SEBI/RBI event dates have been loaded. The event-flag
     override defaults to all zeros. To activate: provide --event-flags-csv with
     columns [date, event_flag] where event_flag=1 on structural-change dates.

  4. REGIME-CONDITIONAL OU — The OU model uses a single fit on the latest PELT segment,
     regardless of whether that segment is "open" or "constrained". A more rigorous
     approach would refit OU using only days classified in the same regime sub-state.

  5. PELT PENALTY IS HEURISTIC — pen = pen_multiplier × var(premiums). This is a
     practical default. Sensitivity to this choice is shown above (vary pen_multiplier).

  6. ADF/KPSS POWER — Both tests have low power in short segments (30–60 obs). The
     confidence score accounts for this, but the stationarity classification may still
     be unreliable in the first weeks after a new PELT break.

  7. TRANSACTION COSTS — Modelled as flat bps applied at position change. Real costs
     (market impact, bid-ask spread at elevated premium) may be higher.
""")

    print("═" * 72)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETF Premium Strategy Backtest — PELT regime detection + mean-reversion optimal switching",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--symbol",           default="MON100",         help="NSE ETF symbol")
    p.add_argument("--start",            default="2023-01-01",     help="Backtest start date YYYY-MM-DD")
    p.add_argument("--end",              default=str(date.today()), help="Backtest end date YYYY-MM-DD")
    p.add_argument("--burnin",           type=int,   default=90,   help="Burn-in rows (no trading)")
    p.add_argument("--pen-multiplier",   type=float, default=3.0,  help="PELT penalty = mult × var")
    p.add_argument("--min-segment",      type=int,   default=30,   help="Min PELT segment obs")
    p.add_argument("--c-buy",            type=float, default=10.0, help="Entry cost (bps)")
    p.add_argument("--c-sell",           type=float, default=10.0, help="Exit cost (bps)")
    p.add_argument("--discount-rate",    type=float, default=0.05, help="Annual discount rate for ZJL DP")
    p.add_argument("--floor-exposure",   type=float, default=0.20, help="Exposure during TRANSITION events")
    p.add_argument("--notional",         type=float, default=NOTIONAL_DEFAULT, help="₹ notional")
    p.add_argument("--csv-path",         default=None,             help="Override: CSV premium data file")
    p.add_argument("--event-flags-csv",  default=None,             help="CSV with event flag dates")
    p.add_argument("--refit-every",      type=int,   default=5,    help="Refit PELT+OU every N days (1=daily, 5=default, 10=fast)")
    p.add_argument("--confidence-threshold", type=float, default=0.0,
                   help="Only trade when confidence >= this (0-100). Default 0=no gate. Recommended: 50-70")
    p.add_argument("--no-plot",          action="store_true",       help="Skip chart generation")
    p.add_argument("--log-level",        default="WARNING",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    setup_logging(log_level=args.log_level)
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)

    print(f"\nLoading premium data for {args.symbol}…", flush=True)
    df = load_data(args.symbol, start, end, csv_path=args.csv_path)
    print(f"  {len(df)} daily rows loaded ({df['date'].iloc[0]} → {df['date'].iloc[-1]})")

    if len(df) < args.burnin + 30:
        raise RuntimeError(
            f"Only {len(df)} rows available; need at least {args.burnin + 30}. "
            "Extend --start or reduce --burnin."
        )

    dates       = list(df["date"])
    event_flags = load_event_flags(args.event_flags_csv, dates)

    # ── Base run ──────────────────────────────────────────────────────────────
    print("Running base backtest…", flush=True)
    base = run_backtest(
        df=df, event_flags=event_flags,
        notional=args.notional, burnin=args.burnin,
        pen_multiplier=args.pen_multiplier,
        min_segment=args.min_segment,
        c_buy_bps=args.c_buy, c_sell_bps=args.c_sell,
        discount_rate_annual=args.discount_rate,
        floor_exposure=args.floor_exposure,
        label=f"base_pen{args.pen_multiplier:.1f}",
        refit_every=args.refit_every,
        confidence_threshold=args.confidence_threshold,
    )
    base_metrics = compute_metrics(base, df)

    # ── Benchmark B: naive ────────────────────────────────────────────────────
    naive = run_naive_backtest(
        df=df, event_flags=event_flags,
        notional=args.notional, burnin=args.burnin,
        c_buy_bps=args.c_buy, c_sell_bps=args.c_sell,
    )

    # ── Sensitivity runs ──────────────────────────────────────────────────────
    # (a) 2× costs  (b) pen_multiplier=1.5  (c) pen_multiplier=6.0
    sensitivity_configs = [
        dict(c_buy_bps=args.c_buy*2, c_sell_bps=args.c_sell*2,
             pen_multiplier=args.pen_multiplier, label="2x_costs"),
        dict(c_buy_bps=args.c_buy, c_sell_bps=args.c_sell,
             pen_multiplier=1.5, label="pen×1.5"),
        dict(c_buy_bps=args.c_buy, c_sell_bps=args.c_sell,
             pen_multiplier=6.0, label="pen×6.0"),
    ]
    sens_pairs: list[tuple[BacktestResult, dict]] = []
    for cfg in sensitivity_configs:
        lbl = cfg.pop("label")
        print(f"  Sensitivity: {lbl}…", flush=True)
        sr = run_backtest(
            df=df, event_flags=event_flags,
            notional=args.notional, burnin=args.burnin,
            min_segment=args.min_segment,
            discount_rate_annual=args.discount_rate,
            floor_exposure=args.floor_exposure,
            label=lbl,
            refit_every=args.refit_every,
            confidence_threshold=args.confidence_threshold,
            **cfg,
        )
        sm = compute_metrics(sr, df)
        sm["label"] = lbl
        sens_pairs.append((sr, sm))

    sens_metrics = [sm for _, sm in sens_pairs]

    # ── Report ────────────────────────────────────────────────────────────────
    print_report(
        symbol=args.symbol,
        base_metrics=base_metrics,
        naive_metrics=naive,
        sens_metrics=sens_metrics,
        base_result=base,
        notional=args.notional,
    )

    # ── Chart ─────────────────────────────────────────────────────────────────
    if not args.no_plot:
        out = OUTPUT_DIR / f"{args.symbol}_ou_regime_backtest.png"
        plot_results(args.symbol, df, base, sens_pairs, out)
        print(f"\nChart: {out}")


if __name__ == "__main__":
    main()
