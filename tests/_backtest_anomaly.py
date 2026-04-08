"""
tests/_backtest_anomaly.py
───────────────────────────
Backtest the composite anomaly pipeline (src/ml/anomaly.py) against
the full GOLDBEES daily OHLCV history stored in ClickHouse.

For every flagged regime, this script measures:
  • How many times the regime fired
  • Mean forward return at 3 / 5 / 10 trading days after the flag
  • Hit rate — the % of flags where the return matches the expected direction:
      🧨 Blow-off Top (Weak)          → expect negative 5d return
      ⚡ Flash Crash / Black Swan (EXIT) → expect positive 5d return (bounce)
      📈 Strong Trend (HODL)           → expect positive 5d return
      🔥 Volatile Breakout             → no strong directional bias (neutral)
      ✅ Normal                         → excluded from hit-rate calculation

Usage
─────
  python tests/_backtest_anomaly.py
  python tests/_backtest_anomaly.py --z-threshold 2.0 --contamination 0.05
  python tests/_backtest_anomaly.py --symbol GOLDBEES --category etfs
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# Expected direction for hit-rate calculation (positive = expect price UP, negative = expect DOWN)
_REGIME_DIRECTION: dict[str, int] = {
    "🧨 Blow-off Top (Weak)":              -1,   # bear signal → hit if return < 0
    "⚡ Flash Crash / Black Swan (EXIT)":   +1,   # bounce signal → hit if return > 0
    "📈 Strong Trend (HODL)":              +1,   # bull signal → hit if return > 0
    "🔥 Volatile Breakout":                 0,   # neutral — no directional prediction
    "✅ Normal":                             0,   # baseline — excluded from hit-rate table
}

PASS  = "\033[92m✓\033[0m"
FAIL  = "\033[91m✗\033[0m"
WARN  = "\033[93m⚠\033[0m"
RESET = "\033[0m"


def run_backtest(
    z_threshold:   float = 2.5,
    contamination: float = 0.05,
    rf_lags:       int   = 5,
    z_window:      int   = 30,
    symbol:        str   = "GOLDBEES",
    category:      str   = "etfs",
    ch_host:       str   = "localhost",
    ch_port:       int   = 8123,
    ch_user:       str   = "default",
    ch_pass:       str   = "",
) -> None:
    try:
        import clickhouse_connect
        import pandas as pd
        import numpy as np
    except ImportError as exc:
        print(f"ERROR: Missing dependency — {exc}")
        sys.exit(1)

    from src.ml.anomaly import run_composite_anomaly

    print("=" * 68)
    print(f"Anomaly Pipeline Backtest — {symbol}")
    print(f"  z_threshold   : {z_threshold}")
    print(f"  contamination : {contamination}")
    print(f"  rf_lags       : {rf_lags}")
    print(f"  z_window      : {z_window}")
    print("=" * 68)

    # ── 1. Load OHLCV from ClickHouse ─────────────────────────────────────────
    print(f"\nFetching {symbol} OHLCV from ClickHouse…")
    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port,
        username=ch_user, password=ch_pass,
        connect_timeout=10,
    )

    rows = client.query(f"""
        SELECT
            trade_date,
            argMax(open,   imported_at) AS open,
            argMax(high,   imported_at) AS high,
            argMax(low,    imported_at) AS low,
            argMax(close,  imported_at) AS close,
            argMax(volume, imported_at) AS volume
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}' AND category = '{category}'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """).result_rows
    client.close()

    if not rows:
        print(f"ERROR: No data for {symbol} in ClickHouse.")
        print(f"Run: python src/main.py import --category {category}")
        sys.exit(1)

    df = pd.DataFrame(rows, columns=["trade_date", "open", "high", "low", "close", "volume"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)

    print(f"  {len(df)} rows:  {df['trade_date'].min().date()} → {df['trade_date'].max().date()}")

    if len(df) < 60:
        print(f"ERROR: Need ≥ 60 rows for the anomaly pipeline; have {len(df)}.")
        sys.exit(1)

    # ── 2. Run anomaly pipeline ───────────────────────────────────────────────
    print("\nRunning anomaly pipeline (RF + Isolation Forest)…")
    df_result, df_flagged, r2_train = run_composite_anomaly(
        df,
        rf_lags=rf_lags,
        contamination=contamination,
        z_threshold=z_threshold,
        z_window=z_window,
    )
    print(f"  RF R² (train)    : {r2_train:.4f}")
    print(f"  Total rows       : {len(df_result)}")
    print(f"  Flagged rows     : {len(df_flagged)}  (|final_z| > {z_threshold})")

    # ── 3. Regime overview ────────────────────────────────────────────────────
    print()
    regime_counts = df_result["regime"].value_counts()
    print("Regime distribution (all rows):")
    for regime, cnt in regime_counts.items():
        pct = cnt / len(df_result) * 100
        print(f"  {regime:<42} {cnt:>4} rows  ({pct:.1f}%)")

    # ── 4. Compute forward returns ────────────────────────────────────────────
    # Index-based shifting: fwd_N means the close N rows ahead in the sorted df.
    # This naturally handles NSE holidays / trading gaps (no calendar arithmetic).
    horizons = [3, 5, 10]
    df_result = df_result.reset_index(drop=True)
    for h in horizons:
        fwd_close = df_result["close"].shift(-h)
        df_result[f"fwd_{h}d"] = (fwd_close - df_result["close"]) / df_result["close"] * 100

    # ── 5. Per-regime summary ─────────────────────────────────────────────────
    print()
    print("=" * 68)
    print("Per-Regime Forward Return Summary")
    print("=" * 68)
    col_w = 42
    header = (
        f"{'Regime':<{col_w}} {'N':>5}"
        f"  {'Ret3d':>7}  {'Ret5d':>7}  {'Ret10d':>8}  {'Hit5d':>7}"
    )
    print(header)
    print("─" * 78)

    summary_rows = []
    for regime in _REGIME_DIRECTION:
        mask = df_result["regime"] == regime
        sub  = df_result[mask].dropna(subset=["fwd_5d"])
        n    = len(sub)
        if n == 0:
            summary_rows.append((regime, 0, None, None, None, None))
            print(f"  {regime:<{col_w}} {'0':>5}  {'—':>7}  {'—':>7}  {'—':>8}  {'—':>7}")
            continue

        r3  = sub["fwd_3d"].mean()  if "fwd_3d"  in sub.columns else float("nan")
        r5  = sub["fwd_5d"].mean()
        r10 = sub["fwd_10d"].mean() if "fwd_10d" in sub.columns else float("nan")

        direction = _REGIME_DIRECTION[regime]
        if direction == 0:
            hit_rate_str = "—"
        else:
            hits     = ((sub["fwd_5d"] * direction) > 0).sum()
            hit_rate = hits / n * 100
            flag     = PASS if hit_rate >= 55 else (WARN if hit_rate >= 40 else FAIL)
            hit_rate_str = f"{flag} {hit_rate:.0f}%"

        r3_s  = f"{r3:+.2f}%"  if not (r3 != r3) else "—"
        r5_s  = f"{r5:+.2f}%"
        r10_s = f"{r10:+.2f}%" if not (r10 != r10) else "—"

        summary_rows.append((regime, n, r3, r5, r10, direction))
        print(f"  {regime:<{col_w}} {n:>5}  {r3_s:>7}  {r5_s:>7}  {r10_s:>8}  {hit_rate_str:>7}")

    print("─" * 78)
    print("  Hit5d = % of flags where 5d return matched expected direction")

    # ── 6. Flagged rows detail ─────────────────────────────────────────────────
    flagged_enriched = df_flagged.copy()
    # Attach fwd_5d from df_result (flagged rows are a subset)
    flagged_enriched = flagged_enriched.merge(
        df_result[["trade_date", "fwd_3d", "fwd_5d", "fwd_10d"]],
        on="trade_date", how="left", suffixes=("", "_r"),
    )
    # Prefer the merged columns if duplicates
    for h in horizons:
        col     = f"fwd_{h}d"
        col_dup = f"fwd_{h}d_r"
        if col_dup in flagged_enriched.columns:
            flagged_enriched[col] = flagged_enriched[col_dup]
            flagged_enriched.drop(columns=[col_dup], inplace=True)

    exclude_normal = flagged_enriched["regime"] != "✅ Normal"
    detail = flagged_enriched[exclude_normal].sort_values("trade_date")
    non_normal_count = len(detail)

    print()
    print("=" * 68)
    print(f"Flagged Events (|final_z| > {z_threshold}, excluding Normal) — {non_normal_count} events")
    print("=" * 68)

    if detail.empty:
        print("  No flagged non-normal events found.")
        print(f"  Try lowering --z-threshold (current: {z_threshold})")
    else:
        print(f"  {'Date':<12} {'Regime':<42} {'FinalZ':>7}  {'Ret3d':>7}  {'Ret5d':>7}  {'Ret10d':>8}")
        print("  " + "─" * 84)
        for _, row in detail.iterrows():
            fz    = f"{row['final_z']:+.2f}"
            r3_s  = f"{row['fwd_3d']:+.2f}%" if "fwd_3d"  in row and not _isnan(row["fwd_3d"])  else "—"
            r5_s  = f"{row['fwd_5d']:+.2f}%" if "fwd_5d"  in row and not _isnan(row["fwd_5d"])  else "—"
            r10_s = f"{row['fwd_10d']:+.2f}%" if "fwd_10d" in row and not _isnan(row["fwd_10d"]) else "—"
            d = str(row["trade_date"].date()) if hasattr(row["trade_date"], "date") else str(row["trade_date"])
            print(f"  {d:<12} {row['regime']:<42} {fz:>7}  {r3_s:>7}  {r5_s:>7}  {r10_s:>8}")

    # ── 7. Blow-off Top spotlight ─────────────────────────────────────────────
    blowoff = df_result[df_result["regime"] == "🧨 Blow-off Top (Weak)"].copy()
    print()
    print("=" * 68)
    print(f"🧨 Blow-off Top (Weak) Deep Dive — {len(blowoff)} occurrences")
    print("=" * 68)
    if blowoff.empty:
        print("  No Blow-off Top events in the dataset.")
        print("  This may indicate GOLDBEES rarely shows high-price/low-volume divergence,")
        print("  or volume data is sparse. Try --z-threshold 1.5 to surface more events.")
    else:
        blowoff = blowoff.merge(
            df_result[["trade_date", "fwd_3d", "fwd_5d", "fwd_10d"]],
            on="trade_date", how="left", suffixes=("_x", ""),
        )
        # Resolve suffix conflicts
        for h in horizons:
            col   = f"fwd_{h}d"
            col_x = f"fwd_{h}d_x"
            if col_x in blowoff.columns:
                blowoff[col] = blowoff[col].fillna(blowoff[col_x])
                blowoff.drop(columns=[col_x], inplace=True)

        neg_5d = (blowoff["fwd_5d"] < 0).sum()
        n_valid = blowoff["fwd_5d"].notna().sum()
        if n_valid > 0:
            hit_pct = neg_5d / n_valid * 100
            outcome = PASS if hit_pct >= 55 else (WARN if hit_pct >= 40 else FAIL)
            print(f"  {outcome} Hit rate (negative 5d return): {neg_5d}/{n_valid} = {hit_pct:.1f}%")
        print(f"  Mean 3d return : {blowoff['fwd_3d'].mean():+.2f}%")
        print(f"  Mean 5d return : {blowoff['fwd_5d'].mean():+.2f}%")
        print(f"  Mean 10d return: {blowoff['fwd_10d'].mean():+.2f}%")
        print()
        print(f"  {'Date':<12} {'Close':>8}  {'Z_Vol':>8}  {'Z_Rob':>8}  {'Ret5d':>8}")
        print("  " + "─" * 52)
        for _, row in blowoff.iterrows():
            d     = str(row["trade_date"].date()) if hasattr(row["trade_date"], "date") else str(row["trade_date"])
            r5_s  = f"{row['fwd_5d']:+.2f}%" if not _isnan(row["fwd_5d"]) else "—"
            zvol  = f"{row['z_volume']:+.2f}" if "z_volume" in row.index and not _isnan(row["z_volume"]) else "—"
            zrob  = f"{row['z_robust']:+.2f}" if "z_robust" in row.index and not _isnan(row["z_robust"]) else "—"
            print(f"  {d:<12} {float(row['close']):>8.2f}  {zvol:>8}  {zrob:>8}  {r5_s:>8}")

    print()
    print("Done.")


def _isnan(v) -> bool:
    try:
        import math
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backtest anomaly regime signals against OHLCV forward returns"
    )
    parser.add_argument("--z-threshold",   type=float, default=2.5,
                        help="|final_z| cutoff for flagging a day (default 2.5)")
    parser.add_argument("--contamination", type=float, default=0.05,
                        help="Isolation Forest contamination fraction (default 0.05)")
    parser.add_argument("--rf-lags",       type=int,   default=5,
                        help="Random Forest lag count (default 5)")
    parser.add_argument("--z-window",      type=int,   default=30,
                        help="Rolling window for robust Z (default 30)")
    parser.add_argument("--symbol",        type=str,   default="GOLDBEES",
                        help="Ticker symbol (default GOLDBEES)")
    parser.add_argument("--category",      type=str,   default="etfs",
                        help="ClickHouse category (default etfs)")
    parser.add_argument("--ch-host",    default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port",    type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--ch-user",    default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-pass",    default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    args = parser.parse_args()

    run_backtest(
        z_threshold=args.z_threshold,
        contamination=args.contamination,
        rf_lags=args.rf_lags,
        z_window=args.z_window,
        symbol=args.symbol,
        category=args.category,
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        ch_user=args.ch_user,
        ch_pass=args.ch_pass,
    )
