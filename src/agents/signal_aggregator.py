"""
Signal Aggregator Agent — combines macro, news, valuation, flows, ML,
and anomaly signals into a unified per-ETF composite score.

Usage:
    from src.agents.signal_aggregator import run_signal_aggregation
    result = run_signal_aggregation(save=True, verbose=True)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime

log = logging.getLogger(__name__)

# ── The 15 core ETFs covered by all signal sources ────────────────────────────

SIGNAL_ETFS = [
    "GOLDBEES", "NIFTYBEES", "BANKBEES", "ITBEES", "JUNIORBEES",
    "SILVERBEES", "CPSEETF", "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES",
    "MON100", "MAFANG", "HNGSNGBEES", "AUTOBEES", "PHARMABEES",
    "PSUBNKBEES", "MID150BEES", "SMALL250",
]

# ── Weights for each pillar ───────────────────────────────────────────────────

WEIGHTS = {
    "macro":     0.25,
    "sentiment": 0.15,
    "valuation": 0.15,
    "flow":      0.15,
    "ml":        0.15,
    "anomaly":   0.05,
}
# Remaining 0.10 is distributed to flow (FII/DII component)


@dataclass
class ETFSignal:
    """Composite signal for a single ETF."""
    etf: str
    macro_score: float = 50.0
    sentiment_score: float = 50.0
    valuation_score: float = 50.0
    flow_score: float = 50.0
    ml_score: float = 50.0
    anomaly_flag: str = "Normal"
    composite_score: float = 50.0
    action: str = "HOLD"
    rationale: str = ""


@dataclass
class SignalReport:
    """Output of the signal aggregator."""
    as_of: date
    signals: list[ETFSignal] = field(default_factory=list)
    regime: str = "NEUTRAL"


# ── Signal collectors ─────────────────────────────────────────────────────────

def _collect_macro_scores(verbose: bool = False) -> dict[str, float]:
    """
    Run macro scanner → convert etf_net_signal to 0–100 scores.
    Net signal range roughly −8 to +8; normalize to 0–100.
    """
    try:
        from src.tools.macro_event_scanner import scan_macro_events
        report = scan_macro_events(max_per_theme=3)
        scores = {}
        for etf in SIGNAL_ETFS:
            net = report.etf_net_signal.get(etf, 0)
            # Clamp to [-8, +8] then linear map to [0, 100]
            clamped = max(-8, min(8, net))
            scores[etf] = round(50 + (clamped / 8) * 50, 1)
        if verbose:
            log.info("Macro scores: %s themes detected, net signals for %d ETFs",
                     len(report.themes_detected), len(scores))
        return scores
    except Exception as e:
        log.warning("Macro signal collection failed: %s", e)
        return {etf: 50.0 for etf in SIGNAL_ETFS}


def _collect_sentiment_scores(verbose: bool = False) -> dict[str, float]:
    """
    Query news_articles from last 7 days → positive/negative ratio per ETF → 0–100.
    """
    try:
        import clickhouse_connect
        c = clickhouse_connect.get_client(host="localhost", port=8123, connect_timeout=5)
        result = c.query(
            "SELECT etfs_impacted, sentiment "
            "FROM market_data.news_articles "
            "WHERE fetched_at >= now() - INTERVAL 7 DAY"
        )
        # Count positive/negative mentions per ETF
        pos_count: dict[str, int] = {}
        neg_count: dict[str, int] = {}
        for row in result.result_rows:
            etfs_str, sentiment = row[0], row[1]
            for etf in etfs_str.split(","):
                etf = etf.strip()
                if etf in SIGNAL_ETFS:
                    if sentiment == "POSITIVE":
                        pos_count[etf] = pos_count.get(etf, 0) + 1
                    elif sentiment == "NEGATIVE":
                        neg_count[etf] = neg_count.get(etf, 0) + 1
        c.close()
        scores = {}
        for etf in SIGNAL_ETFS:
            p = pos_count.get(etf, 0)
            n = neg_count.get(etf, 0)
            total = p + n
            if total == 0:
                scores[etf] = 50.0
            else:
                scores[etf] = round((p / total) * 100, 1)
        if verbose:
            log.info("Sentiment scores computed from %d news rows", len(result.result_rows))
        return scores
    except Exception as e:
        log.warning("Sentiment signal collection failed: %s", e)
        return {etf: 50.0 for etf in SIGNAL_ETFS}


def _collect_valuation_scores(verbose: bool = False) -> dict[str, float]:
    """
    Run domestic ETF scanner → Z-score premium/discount → 0–100.
    Negative Z (discount) = high score (buy opportunity).
    """
    try:
        from src.tools.domestic_etf_scanner import scan_domestic_etfs
        etf_data = scan_domestic_etfs(symbols=SIGNAL_ETFS, lookback_days=30)
        scores = {}
        for item in etf_data:
            sym = item.get("symbol", "")
            z = item.get("z_score")
            if sym in SIGNAL_ETFS and z is not None:
                # Z ≤ -2 → 100 (deep discount), Z ≥ +2 → 0 (steep premium)
                clamped = max(-2, min(2, z))
                scores[sym] = round(50 - (clamped / 2) * 50, 1)
        for etf in SIGNAL_ETFS:
            if etf not in scores:
                scores[etf] = 50.0
        if verbose:
            log.info("Valuation scores: %d ETFs with Z-scores", len([s for s in scores.values() if s != 50.0]))
        return scores
    except Exception as e:
        log.warning("Valuation signal collection failed: %s", e)
        return {etf: 50.0 for etf in SIGNAL_ETFS}


def _collect_flow_scores(verbose: bool = False) -> dict[str, float]:
    """
    FII/DII 5-day rolling net → uniform score for all equity ETFs.
    FII net buying → bullish for equity, bearish for gold (rotation).
    """
    try:
        import clickhouse_connect
        c = clickhouse_connect.get_client(host="localhost", port=8123, connect_timeout=5)
        result = c.query(
            "SELECT sum(fii_net_cr) AS fii_5d, sum(dii_net_cr) AS dii_5d "
            "FROM market_data.fii_dii_flows FINAL "
            "WHERE trade_date >= today() - 5"
        )
        c.close()
        if result.result_rows:
            fii_5d = float(result.result_rows[0][0] or 0)
            dii_5d = float(result.result_rows[0][1] or 0)
        else:
            fii_5d, dii_5d = 0.0, 0.0

        # FII + DII net combined: range approx -20000 to +20000 Cr over 5 days
        net = fii_5d + dii_5d
        clamped = max(-15000, min(15000, net))
        equity_score = round(50 + (clamped / 15000) * 50, 1)

        scores = {}
        # Equity ETFs benefit from inflows
        equity_etfs = {"NIFTYBEES", "BANKBEES", "ITBEES", "JUNIORBEES", "CPSEETF",
                       "AUTOBEES", "PHARMABEES", "PSUBNKBEES", "MID150BEES", "SMALL250"}
        # Safe-haven ETFs inversely affected
        haven_etfs = {"GOLDBEES", "SILVERBEES", "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES"}
        # International ETFs neutral to domestic flows
        intl_etfs = {"MON100", "MAFANG", "HNGSNGBEES"}

        for etf in SIGNAL_ETFS:
            if etf in equity_etfs:
                scores[etf] = equity_score
            elif etf in haven_etfs:
                scores[etf] = round(100 - equity_score, 1)  # inverse
            elif etf in intl_etfs:
                scores[etf] = 50.0  # neutral
            else:
                scores[etf] = 50.0

        if verbose:
            log.info("Flow scores: FII 5d=%.0f Cr, DII 5d=%.0f Cr, equity_score=%.1f",
                     fii_5d, dii_5d, equity_score)
        return scores
    except Exception as e:
        log.warning("Flow signal collection failed: %s", e)
        return {etf: 50.0 for etf in SIGNAL_ETFS}


def _collect_ml_scores(verbose: bool = False) -> dict[str, float]:
    """
    Query latest ML prediction from ClickHouse → score for GOLDBEES.
    Other ETFs get neutral (50) until multi-ETF ML is implemented.
    """
    try:
        import clickhouse_connect
        c = clickhouse_connect.get_client(host="localhost", port=8123, connect_timeout=5)
        result = c.query(
            "SELECT expected_return_pct "
            "FROM market_data.ml_predictions FINAL "
            "ORDER BY as_of DESC LIMIT 1"
        )
        c.close()
        scores = {etf: 50.0 for etf in SIGNAL_ETFS}
        if result.result_rows:
            pred = float(result.result_rows[0][0])
            # Map predicted return to 0–100: -3% → 0, 0% → 50, +3% → 100
            clamped = max(-3, min(3, pred))
            scores["GOLDBEES"] = round(50 + (clamped / 3) * 50, 1)
            if verbose:
                log.info("ML score for GOLDBEES: pred=%.2f%% → score=%.1f", pred, scores["GOLDBEES"])
        return scores
    except Exception as e:
        log.warning("ML signal collection failed: %s", e)
        return {etf: 50.0 for etf in SIGNAL_ETFS}


def _collect_anomaly_flags(verbose: bool = False) -> dict[str, str]:
    """
    Run anomaly detection on GOLDBEES (primary) and return regime flags.
    Other ETFs: 'Normal' until we extend anomaly detection.
    """
    flags = {etf: "Normal" for etf in SIGNAL_ETFS}
    try:
        import clickhouse_connect
        import pandas as pd
        c = clickhouse_connect.get_client(host="localhost", port=8123, connect_timeout=5)
        result = c.query(
            "SELECT trade_date, argMax(open, imported_at) AS open, "
            "argMax(high, imported_at) AS high, argMax(low, imported_at) AS low, "
            "argMax(close, imported_at) AS close, argMax(volume, imported_at) AS volume "
            "FROM market_data.daily_prices "
            "WHERE symbol = 'GOLDBEES' AND category = 'etfs' "
            "GROUP BY trade_date ORDER BY trade_date ASC"
        )
        c.close()
        if len(result.result_rows) < 60:
            return flags
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        from src.ml.anomaly import run_composite_anomaly
        _, df_flagged, _ = run_composite_anomaly(df, z_threshold=2.0)
        if not df_flagged.empty:
            last_regime = df_flagged.iloc[-1].get("regime", "Normal")
            flags["GOLDBEES"] = str(last_regime)
        if verbose:
            log.info("Anomaly: GOLDBEES regime=%s, %d flagged days",
                     flags["GOLDBEES"], len(df_flagged))
    except Exception as e:
        log.warning("Anomaly signal collection failed: %s", e)
    return flags


# ── Composite scoring ─────────────────────────────────────────────────────────

def _compute_composite(
    macro: dict, sentiment: dict, valuation: dict,
    flow: dict, ml: dict, anomaly: dict,
) -> list[ETFSignal]:
    """Compute weighted composite score and action for each ETF."""
    signals = []
    for etf in SIGNAL_ETFS:
        m = macro.get(etf, 50)
        s = sentiment.get(etf, 50)
        v = valuation.get(etf, 50)
        f = flow.get(etf, 50)
        ml_s = ml.get(etf, 50)
        a_flag = anomaly.get(etf, "Normal")

        # Weighted composite
        composite = (
            m * WEIGHTS["macro"]
            + s * WEIGHTS["sentiment"]
            + v * WEIGHTS["valuation"]
            + f * (WEIGHTS["flow"] + 0.10)  # flow gets extra 10% from the remaining
            + ml_s * WEIGHTS["ml"]
        )

        # Anomaly override: boost contrarian if Flash Crash
        if "Flash Crash" in a_flag and composite < 40:
            composite = min(composite + 15, 60)
        # Blow-off top: dampen bullish signal
        elif "Blow-off" in a_flag and composite > 60:
            composite = max(composite - 10, 55)

        composite = round(composite, 1)

        # Action thresholds
        if composite >= 75:
            action = "BUY"
        elif composite >= 60:
            action = "ACCUMULATE"
        elif composite >= 40:
            action = "HOLD"
        elif composite >= 25:
            action = "TRIM"
        else:
            action = "AVOID"

        signals.append(ETFSignal(
            etf=etf,
            macro_score=m,
            sentiment_score=s,
            valuation_score=v,
            flow_score=f,
            ml_score=ml_s,
            anomaly_flag=a_flag,
            composite_score=composite,
            action=action,
        ))

    signals.sort(key=lambda s: s.composite_score, reverse=True)
    return signals


# ── Main entry point ──────────────────────────────────────────────────────────

def run_signal_aggregation(
    save: bool = False,
    verbose: bool = False,
) -> SignalReport:
    """
    Run all signal collectors, compute composite, and optionally save to DB.

    Returns a SignalReport with per-ETF composite scores.
    """
    log.info("Starting signal aggregation for %d ETFs...", len(SIGNAL_ETFS))

    # Collect all signals
    macro = _collect_macro_scores(verbose)
    sentiment = _collect_sentiment_scores(verbose)
    valuation = _collect_valuation_scores(verbose)
    flow = _collect_flow_scores(verbose)
    ml = _collect_ml_scores(verbose)
    anomaly = _collect_anomaly_flags(verbose)

    # Compute composite
    signals = _compute_composite(macro, sentiment, valuation, flow, ml, anomaly)

    today = date.today()
    report = SignalReport(as_of=today, signals=signals)

    # Determine overall regime from top/bottom signals
    top_actions = [s.action for s in signals[:5]]
    if top_actions.count("BUY") >= 3:
        report.regime = "RISK_ON"
    elif top_actions.count("AVOID") + top_actions.count("TRIM") >= 3:
        report.regime = "RISK_OFF"
    else:
        report.regime = "MIXED"

    # Save to DB
    if save:
        try:
            from src.importer.clickhouse import ClickHouseImporter
            ch = ClickHouseImporter()
            ch.ensure_schema()
            rows = [
                {
                    "as_of": today,
                    "etf_symbol": s.etf,
                    "macro_score": s.macro_score,
                    "sentiment_score": s.sentiment_score,
                    "valuation_score": s.valuation_score,
                    "flow_score": s.flow_score,
                    "ml_score": s.ml_score,
                    "anomaly_flag": s.anomaly_flag,
                    "composite_score": s.composite_score,
                    "action": s.action,
                    "rationale": s.rationale,
                }
                for s in signals
            ]
            n = ch.insert_signal_composite(rows)
            ch.close()
            log.info("Saved %d signal composite rows to ClickHouse", n)
        except Exception as e:
            log.warning("Failed to save signal composite: %s", e)

    log.info("Signal aggregation complete: regime=%s, %d ETFs scored", report.regime, len(signals))
    return report


def print_signal_report(report: SignalReport) -> None:
    """Print the signal report to terminal using Rich."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()

    # Header
    regime_color = {"RISK_ON": "green", "RISK_OFF": "red", "MIXED": "yellow"}.get(report.regime, "white")
    console.print(Panel(
        f"[bold]Signal Aggregator[/bold]\n"
        f"[dim]As of {report.as_of} · {len(report.signals)} ETFs · "
        f"Regime: [{regime_color}]{report.regime}[/{regime_color}][/dim]",
        border_style="cyan",
    ))

    # Table
    table = Table(title="ETF Signal Composite", show_lines=True)
    table.add_column("ETF", style="bold", width=14)
    table.add_column("Macro", justify="right", width=7)
    table.add_column("Sent.", justify="right", width=7)
    table.add_column("Val.", justify="right", width=7)
    table.add_column("Flow", justify="right", width=7)
    table.add_column("ML", justify="right", width=7)
    table.add_column("Anomaly", width=12)
    table.add_column("Score", justify="right", style="bold", width=7)
    table.add_column("Action", width=12)

    ACTION_STYLE = {
        "BUY": "[bold green]BUY[/bold green]",
        "ACCUMULATE": "[green]ACCUMULATE[/green]",
        "HOLD": "[yellow]HOLD[/yellow]",
        "TRIM": "[red]TRIM[/red]",
        "AVOID": "[bold red]AVOID[/bold red]",
    }

    def _score_color(v: float) -> str:
        if v >= 70: return f"[green]{v:.0f}[/green]"
        if v >= 55: return f"[bright_green]{v:.0f}[/bright_green]"
        if v >= 45: return f"[yellow]{v:.0f}[/yellow]"
        if v >= 30: return f"[red]{v:.0f}[/red]"
        return f"[bold red]{v:.0f}[/bold red]"

    for s in report.signals:
        table.add_row(
            s.etf,
            _score_color(s.macro_score),
            _score_color(s.sentiment_score),
            _score_color(s.valuation_score),
            _score_color(s.flow_score),
            _score_color(s.ml_score),
            s.anomaly_flag,
            _score_color(s.composite_score),
            ACTION_STYLE.get(s.action, s.action),
        )

    console.print(table)

    # Top picks
    buys = [s for s in report.signals if s.action in ("BUY", "ACCUMULATE")]
    if buys:
        console.print(Panel(
            "\n".join(f"  [green]▲[/green] {s.etf}: {s.composite_score:.0f}/100 → {s.action}" for s in buys[:5]),
            title="Top Picks", border_style="green",
        ))

    avoids = [s for s in report.signals if s.action in ("TRIM", "AVOID")]
    if avoids:
        console.print(Panel(
            "\n".join(f"  [red]▼[/red] {s.etf}: {s.composite_score:.0f}/100 → {s.action}" for s in avoids[:5]),
            title="Avoid / Trim", border_style="red",
        ))
