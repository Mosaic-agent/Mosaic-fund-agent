"""
tests/test_quant_signals.py
────────────────────────────
Standalone analytical script to historically validate the COT "Crowded Long"
signal against GOLDBEES price data stored in ClickHouse.

Signal definition
─────────────────
  COT MM Net / Open Interest > THRESHOLD (default 25%)

Validation
──────────
  For each signal date, check whether GOLDBEES closed more than DROP_PCT%
  lower within the following N_DAYS trading days.

  Hit Rate = (signal dates followed by a >DROP_PCT% drop) / (total signal dates)

Usage
─────
  python tests/test_quant_signals.py
  python tests/test_quant_signals.py --threshold 0.25 --drop 3.0 --days 10
"""
from __future__ import annotations

import argparse
import os
import sys

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def run_validation(
    threshold_pct: float = 25.0,
    drop_pct: float = 3.0,
    n_days: int = 10,
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_user: str = "default",
    ch_pass: str = "",
) -> None:
    """
    Validate: does a COT MM Net/OI > threshold_pct signal predict a
    >drop_pct% GOLDBEES price decline within n_days trading days?
    """
    try:
        import clickhouse_connect
        import pandas as pd
    except ImportError as exc:
        print(f"ERROR: Missing dependency — {exc}")
        sys.exit(1)

    print("=" * 64)
    print("COT Crowded Long → GOLDBEES Drop Backtest")
    print(f"  Signal:  COT MM Net/OI > {threshold_pct:.1f}%")
    print(f"  Target:  GOLDBEES price drop > {drop_pct:.1f}%")
    print(f"  Window:  next {n_days} trading days")
    print("=" * 64)

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port,
        username=ch_user, password=ch_pass,
        connect_timeout=10,
    )

    # ── Load GOLDBEES daily close ─────────────────────────────────────────────
    print("\nFetching GOLDBEES daily prices…")
    gb_rows = client.query("""
        SELECT trade_date, argMax(close, imported_at) AS close
        FROM market_data.daily_prices
        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """).result_rows

    if not gb_rows:
        print("ERROR: No GOLDBEES data in ClickHouse. Run: python src/main.py import --category etfs")
        client.close()
        return

    gb = pd.DataFrame(gb_rows, columns=["trade_date", "close"])
    gb["trade_date"] = pd.to_datetime(gb["trade_date"])
    gb = gb.sort_values("trade_date").reset_index(drop=True)
    print(f"  {len(gb)} rows:  {gb['trade_date'].min().date()} → {gb['trade_date'].max().date()}")

    # ── Load COT gold data ────────────────────────────────────────────────────
    print("\nFetching COT gold data…")
    cot_rows = client.query("""
        SELECT report_date, mm_net, open_interest
        FROM market_data.cot_gold FINAL
        ORDER BY report_date ASC
    """).result_rows
    client.close()

    if not cot_rows:
        print("ERROR: No COT data in ClickHouse. Run: python src/main.py import --category cot")
        return

    cot = pd.DataFrame(cot_rows, columns=["report_date", "mm_net", "open_interest"])
    cot["report_date"] = pd.to_datetime(cot["report_date"])
    cot = cot[cot["open_interest"] > 0].copy()
    cot["cot_pct"]     = cot["mm_net"] / cot["open_interest"] * 100
    print(f"  {len(cot)} weekly reports:  {cot['report_date'].min().date()} → {cot['report_date'].max().date()}")
    print(f"  COT PCT range: {cot['cot_pct'].min():.1f}% → {cot['cot_pct'].max():.1f}%")

    # ── Forward-fill COT onto daily GOLDBEES dates ────────────────────────────
    # Use merge_asof: each GOLDBEES date gets the last-known COT report.
    daily = pd.merge_asof(
        gb.sort_values("trade_date"),
        cot[["report_date", "cot_pct"]].sort_values("report_date"),
        left_on="trade_date",
        right_on="report_date",
        direction="backward",
    ).dropna(subset=["cot_pct"])

    # ── Identify signal dates ─────────────────────────────────────────────────
    # A new signal fires on the first day with COT_PCT > threshold after a
    # period below it (avoid counting consecutive crowded days as N signals).
    daily["signal"] = (daily["cot_pct"] > threshold_pct)
    daily["prev_signal"] = daily["signal"].shift(1).fillna(False)
    signal_dates_df = daily[(daily["signal"]) & (~daily["prev_signal"])].copy()

    if signal_dates_df.empty:
        print(f"\nNo signal dates found where COT MM Net/OI > {threshold_pct:.1f}%")
        print("Try lowering --threshold (e.g. --threshold 20)")
        return

    print(f"\n{'─'*64}")
    print(f"Signal dates (new crowded-long entries): {len(signal_dates_df)}")
    print(f"{'─'*64}")

    # ── Validate each signal ──────────────────────────────────────────────────
    gb_indexed = gb.set_index("trade_date")["close"]
    results = []

    for _, row in signal_dates_df.iterrows():
        sig_date  = row["trade_date"]
        sig_price = row["close"]
        sig_cot   = row["cot_pct"]

        # Find the prices over the next n_days trading days
        future_prices = gb_indexed[gb_indexed.index > sig_date].head(n_days)

        if len(future_prices) == 0:
            # Signal too recent — skip
            continue

        min_price   = float(future_prices.min())
        max_drop    = (sig_price - min_price) / sig_price * 100  # positive = drop
        hit         = max_drop > drop_pct
        days_to_min = int((future_prices.idxmin() - sig_date).days)

        results.append({
            "date":         sig_date.date(),
            "cot_pct":      round(sig_cot, 1),
            "gb_price":     round(sig_price, 2),
            "max_drop_pct": round(max_drop, 2),
            "days_to_min":  days_to_min,
            "hit":          hit,
        })

    if not results:
        print("No signals with sufficient follow-on data (all too recent).")
        return

    res_df = pd.DataFrame(results)
    n_signals = len(res_df)
    n_hits    = int(res_df["hit"].sum())
    hit_rate  = n_hits / n_signals * 100

    # ── Print results table ───────────────────────────────────────────────────
    print(f"\n{'Date':<12} {'COT%':>6} {'Price':>8} {'MaxDrop%':>10} {'DaysToMin':>10} {'Hit?':>6}")
    print("─" * 58)
    for r in results:
        hit_str = "YES ✓" if r["hit"] else "no"
        print(
            f"{str(r['date']):<12} {r['cot_pct']:>6.1f} "
            f"{r['gb_price']:>8.2f} {r['max_drop_pct']:>+10.2f} "
            f"{r['days_to_min']:>10}  {hit_str:>6}"
        )

    print("\n" + "=" * 64)
    print(f"SUMMARY")
    print(f"  Signals analysed:           {n_signals}")
    print(f"  Hits (drop > {drop_pct:.1f}% in {n_days}d): {n_hits}")
    print(f"  Hit Rate:                   {hit_rate:.1f}%")
    print(f"  Avg max drop (all signals): {res_df['max_drop_pct'].mean():+.2f}%")
    print(f"  Avg max drop (hits only):   "
          f"{res_df.loc[res_df['hit'], 'max_drop_pct'].mean():+.2f}%"
          if n_hits else "  Avg max drop (hits only):   N/A")
    print(f"  Avg days to minimum:        {res_df['days_to_min'].mean():.1f}")
    print("=" * 64)

    if hit_rate >= 60:
        print(f"\n✅ Strong signal: >60% of crowded-long readings preceded a >{drop_pct:.1f}% drop.")
    elif hit_rate >= 40:
        print(f"\n⚠️  Moderate signal: {hit_rate:.0f}% hit rate — useful but not definitive.")
    else:
        print(f"\n❌ Weak signal: {hit_rate:.0f}% hit rate — not reliably predictive at this threshold.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="COT Crowded Long → GOLDBEES price drop backtest"
    )
    parser.add_argument("--threshold", type=float, default=25.0,
                        help="COT MM Net/OI %% trigger (default 25.0)")
    parser.add_argument("--drop", type=float, default=3.0,
                        help="GOLDBEES price drop %% required for a hit (default 3.0)")
    parser.add_argument("--days", type=int, default=10,
                        help="Trading days window to look for the drop (default 10)")
    parser.add_argument("--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-pass", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    args = parser.parse_args()

    run_validation(
        threshold_pct=args.threshold,
        drop_pct=args.drop,
        n_days=args.days,
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        ch_user=args.ch_user,
        ch_pass=args.ch_pass,
    )
