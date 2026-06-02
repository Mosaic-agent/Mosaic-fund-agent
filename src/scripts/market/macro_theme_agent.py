"""
scripts/macro_theme_agent.py
────────────────────────────
Long/Short Macro Theme Agent — classifies macro news into tradeable stances.
Wraps macro_event_scanner.py with a quantitative overlay (DXY, Yields, VIX).

Usage:
    python scripts/macro_theme_agent.py
    python scripts/macro_theme_agent.py --max-per-theme 6 --json
"""

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Any

import yfinance as yf
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.rule import Rule
from rich import box

# ── Root path setup ──────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

try:
    from tools.macro_event_scanner import scan_macro_events, MacroEvent, MacroReport, MacroFundamentals
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

_THEME_BIAS: dict[str, str] = {
    "Geopolitical / War":                  "RISK_OFF",
    "Central Bank Policy (Fed / RBI)":     "SENTIMENT_DRIVEN",
    "Crude Oil Shock":                     "RISK_OFF",
    "Currency / INR Move":                 "SENTIMENT_DRIVEN",
    "Trade War / Tariffs":                 "RISK_OFF",
    "India Macro (GDP / Budget / Policy)": "RISK_ON",
    "Gold / Commodity Specific":           "COMMODITY",
    "Global Risk-Off / Equity Sell-Off":   "RISK_OFF",
}

# Quant thresholds
_VIX_RISK_OFF   = 20.0   # VIX ≥ this → risk-off confirmed
_DXY_FALL_PCT   = -0.5   # 5D DXY % ≤ this → dollar weak (Gold+)
_DXY_RISE_PCT   = +0.5
_YIELD_FALL_PTS = -0.05  # 5D US10Y Δ ≤ this → dovish (Gold+)
_YIELD_RISE_PTS = +0.05

# ── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class QuantContext:
    dxy_now: Optional[float] = None
    dxy_5d_chg_pct: Optional[float] = None
    us10y_now: Optional[float] = None
    us10y_5d_chg: Optional[float] = None
    vix_now: Optional[float] = None
    as_of: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

@dataclass
class ThemeStance:
    theme: str
    icon: str
    direction: str                  # LONG / SHORT / MIXED
    conviction: str                # HIGH / MEDIUM / LOW
    conviction_score: float        # 0–100
    long_basket: list[str]
    short_basket: list[str]
    headline_count: int
    top_headlines: list[str]
    sentiment_split: dict[str, int]
    quant_confirms: list[str]
    quant_conflicts: list[str]
    narrative: str

@dataclass
class MacroThemeReport:
    as_of: str
    long_themes: list[ThemeStance] = field(default_factory=list)
    short_themes: list[ThemeStance] = field(default_factory=list)
    mixed_themes: list[ThemeStance] = field(default_factory=list)
    net_etf_signal: dict[str, int] = field(default_factory=dict)
    conflicts: list[str] = field(default_factory=list)
    quant: Optional[QuantContext] = None
    macro_fundamentals: Optional[Any] = None   # MacroFundamentals from WB/IMF

# ── Logic ────────────────────────────────────────────────────────────────────

def _fetch_quant_context() -> QuantContext:
    """Fetch DXY, US10Y, and VIX to provide backdrop."""
    ctx = QuantContext()
    try:
        # DX-Y.NYB (Dollar), ^TNX (10Y Yield), ^VIX (Volatility)
        data = yf.download(["DX-Y.NYB", "^TNX", "^VIX"], period="12d", progress=False, auto_adjust=True)
        if data.empty:
            return ctx

        # Extract latest values
        close = data['Close']
        if 'DX-Y.NYB' in close:
            ctx.dxy_now = float(close['DX-Y.NYB'].iloc[-1])
            dxy_5d = float(close['DX-Y.NYB'].iloc[-6]) if len(close) >= 6 else ctx.dxy_now
            ctx.dxy_5d_chg_pct = ((ctx.dxy_now / dxy_5d) - 1) * 100

        if '^TNX' in close:
            ctx.us10y_now = float(close['^TNX'].iloc[-1])
            us10y_5d = float(close['^TNX'].iloc[-6]) if len(close) >= 6 else ctx.us10y_now
            ctx.us10y_5d_chg = ctx.us10y_now - us10y_5d

        if '^VIX' in close:
            ctx.vix_now = float(close['^VIX'].iloc[-1])

    except Exception as e:
        log.warning(f"Failed to fetch quant context: {e}")
    return ctx

def _resolve_direction(bias: str, pos_n: int, neg_n: int, bullish_etfs: list[str], bearish_etfs: list[str]):
    """
    Decide if a theme is a LONG or SHORT opportunity based on its inherent bias 
    and the observed sentiment split.
    """
    total = pos_n + neg_n
    if total == 0:
        return "MIXED", [], [], "Neutral development"

    # Predominant sentiment
    is_neg = neg_n >= pos_n
    
    if bias == "RISK_OFF":
        # RISK_OFF (War/Crash) usually means negative news. 
        # Negative news for a RISK_OFF theme = SHORT Equities, LONG Gold/Liquid.
        if is_neg:
            return "SHORT", bullish_etfs, bearish_etfs, "Risk-off escalation triggered."
        else:
            return "LONG", bearish_etfs, bullish_etfs, "Risk-off subsiding; recovery play."

    if bias == "RISK_ON":
        # RISK_ON (GDP/Budget) usually means positive news.
        if not is_neg:
            return "LONG", bullish_etfs, bearish_etfs, "Positive domestic growth tailwinds."
        else:
            return "SHORT", bearish_etfs, bullish_etfs, "Macro headwinds detected."

    if bias == "COMMODITY":
        # Gold specific news.
        if not is_neg:
            return "LONG", bullish_etfs, [], "Bullish commodity momentum."
        else:
            return "SHORT", [], bullish_etfs, "Commodity correction / profit taking."

    if bias == "SENTIMENT_DRIVEN":
        # Fed/RBI or Currency moves.
        if pos_n > neg_n:
            return "LONG", bullish_etfs, bearish_etfs, "Supportive policy/currency backdrop."
        elif neg_n > pos_n:
            return "SHORT", bearish_etfs, bullish_etfs, "Tightening / currency pressure."
        else:
            return "MIXED", bullish_etfs, bearish_etfs, "Conflicting policy signals."

    return "MIXED", [], [], "Undefined impact"

def _quant_assessment(theme: str, direction: str, quant: QuantContext, mf=None):
    """Adjust conviction based on market data and macro fundamentals."""
    confirms, conflicts = [], []
    adj = 0

    # Risk-off assessment
    if _THEME_BIAS.get(theme) == "RISK_OFF" and direction == "SHORT":
        if quant.vix_now and quant.vix_now >= _VIX_RISK_OFF:
            confirms.append(f"VIX High ({quant.vix_now:.1f})")
            adj += 10
        elif quant.vix_now:
            conflicts.append(f"VIX Low ({quant.vix_now:.1f})")
            adj -= 5

    # Gold/Commodity assessment
    if theme in ["Gold / Commodity Specific", "Geopolitical / War", "Crude Oil Shock"]:
        if quant.dxy_5d_chg_pct and quant.dxy_5d_chg_pct <= _DXY_FALL_PCT:
            confirms.append(f"DXY Weak ({quant.dxy_5d_chg_pct:+.1f}%)")
            adj += 10
        elif quant.dxy_5d_chg_pct and quant.dxy_5d_chg_pct >= _DXY_RISE_PCT:
            conflicts.append(f"DXY Strong ({quant.dxy_5d_chg_pct:+.1f}%)")
            adj -= 8

        if quant.us10y_5d_chg and quant.us10y_5d_chg <= _YIELD_FALL_PTS:
            confirms.append(f"Yields Falling ({quant.us10y_5d_chg:+.2f} pts)")
            adj += 8
        elif quant.us10y_5d_chg and quant.us10y_5d_chg >= _YIELD_RISE_PTS:
            conflicts.append(f"Yields Rising ({quant.us10y_5d_chg:+.2f} pts)")
            adj -= 6

        # High inflation = real-return erosion → gold bid confirmed
        if mf and mf.cpi_pct is not None:
            if mf.cpi_pct > 6.0:
                confirms.append(f"Elevated CPI {mf.cpi_pct:.1f}% (gold inflation hedge)")
                adj += 8
            elif mf.cpi_pct > 5.0:
                confirms.append(f"Rising CPI {mf.cpi_pct:.1f}%")
                adj += 4

    # India Macro — use World Bank/IMF fundamentals as the primary signal
    if theme == "India Macro (GDP / Budget / Policy)":
        if quant.vix_now and quant.vix_now < _VIX_RISK_OFF:
            confirms.append("Stable global backdrop")
            adj += 8
        elif quant.vix_now:
            conflicts.append("High global volatility")
            adj -= 8

        if mf:
            if mf.gdp_growth_pct is not None:
                if mf.gdp_growth_pct >= 7.0:
                    confirms.append(f"GDP +{mf.gdp_growth_pct:.1f}% (strong, above 7%)")
                    adj += 15
                elif mf.gdp_growth_pct >= 6.0:
                    confirms.append(f"GDP +{mf.gdp_growth_pct:.1f}% (solid, 6–7% range)")
                    adj += 8
                elif mf.gdp_growth_pct < 5.5:
                    conflicts.append(f"GDP +{mf.gdp_growth_pct:.1f}% (below trend — headwind)")
                    adj -= 10

            if mf.gdp_forecast_pct is not None:
                yr = mf.forecast_year or "fwd"
                if mf.gdp_forecast_pct >= 6.5:
                    confirms.append(f"IMF {yr} GDP forecast +{mf.gdp_forecast_pct:.1f}%")
                    adj += 5
                elif mf.gdp_forecast_pct < 5.5:
                    conflicts.append(f"IMF {yr} GDP forecast +{mf.gdp_forecast_pct:.1f}% (weak outlook)")
                    adj -= 8

            if mf.cpi_pct is not None:
                if mf.cpi_pct > 6.0:
                    conflicts.append(f"CPI {mf.cpi_pct:.1f}% — stagflation risk, RBI constrained")
                    adj -= 10
                elif mf.cpi_pct > 5.0:
                    conflicts.append(f"CPI {mf.cpi_pct:.1f}% above RBI target")
                    adj -= 5
                elif mf.cpi_pct <= 4.5:
                    confirms.append(f"CPI {mf.cpi_pct:.1f}% — within RBI comfort zone")
                    adj += 5

            if mf.ca_balance_pct is not None and mf.ca_balance_pct < -3.0:
                conflicts.append(
                    f"CA deficit {mf.ca_balance_pct:.1f}% GDP — INR/import pressure"
                )
                adj -= 8

            if mf.fiscal_balance_pct is not None and mf.fiscal_balance_pct < -5.5:
                conflicts.append(
                    f"Fiscal deficit {mf.fiscal_balance_pct:.1f}% GDP — crowding-out risk"
                )
                adj -= 5

    # Central Bank Policy — CPI informs rate-cut probability
    if theme == "Central Bank Policy (Fed / RBI)" and mf:
        if mf.cpi_forecast_pct is not None:
            yr = mf.forecast_year or "fwd"
            if mf.cpi_forecast_pct <= 4.5:
                confirms.append(f"IMF {yr} CPI forecast {mf.cpi_forecast_pct:.1f}% → rate-cut room")
                adj += 5
            elif mf.cpi_forecast_pct > 5.5:
                conflicts.append(f"IMF {yr} CPI forecast {mf.cpi_forecast_pct:.1f}% → sticky inflation")
                adj -= 5

    return confirms, conflicts, adj

def _classify_theme(theme_name: str, events: list[MacroEvent], quant: QuantContext, mf=None) -> ThemeStance:
    icon = events[0].icon
    bias = _THEME_BIAS.get(theme_name, "SENTIMENT_DRIVEN")
    
    # Extract impact maps (all events in a theme share the same baseline impact map)
    impact_map = events[0].impact
    bullish_etfs = [etf for etf, d in impact_map.items() if d == +1]
    bearish_etfs = [etf for etf, d in impact_map.items() if d == -1]

    # Count sentiments
    counts = Counter(e.sentiment for e in events)
    pos_n, neg_n = counts["POSITIVE"], counts["NEGATIVE"]
    
    # Resolve direction
    direction, long_basket, short_basket, narrative_prefix = _resolve_direction(
        bias, pos_n, neg_n, bullish_etfs, bearish_etfs
    )

    # Base score
    total = len(events)
    base_score = min(30, total * 10)
    tier_bonus = {"HIGH": 30, "MEDIUM": 20, "LOW": 10}.get(events[0].conviction, 10)
    
    # Quant overlay (market data + WB/IMF fundamentals)
    confirms, conflicts, adj = _quant_assessment(theme_name, direction, quant, mf)
    
    score = max(0, min(100, base_score + tier_bonus + adj))
    
    if score >= 70:    conviction = "HIGH"
    elif score >= 45:  conviction = "MEDIUM"
    else:              conviction = "LOW"

    # Narrative
    headlines = [e.headline for e in events[:3]]
    sent_desc = f"{pos_n} pos / {neg_n} neg"
    narrative = (
        f"{narrative_prefix} {theme_name} is showing {conviction.lower()} conviction "
        f"({score:.0f} pts) based on {total} recent headlines ({sent_desc})."
    )

    return ThemeStance(
        theme=theme_name,
        icon=icon,
        direction=direction,
        conviction=conviction,
        conviction_score=score,
        long_basket=long_basket,
        short_basket=short_basket,
        headline_count=total,
        top_headlines=headlines,
        sentiment_split={"POSITIVE": pos_n, "NEGATIVE": neg_n, "NEUTRAL": counts["NEUTRAL"]},
        quant_confirms=confirms,
        quant_conflicts=conflicts,
        narrative=narrative
    )

def _detect_conflicts(long_themes: list[ThemeStance], short_themes: list[ThemeStance]) -> list[str]:
    conflicts = []
    long_map = {}
    for t in long_themes:
        for etf in t.long_basket:
            long_map[etf] = t.theme
    
    for t in short_themes:
        for etf in t.short_basket:
            if etf in long_map:
                conflicts.append(f"{etf} — LONG from [{long_map[etf]}] vs SHORT from [{t.theme}]")
    
    return conflicts

# ── Runner ───────────────────────────────────────────────────────────────────

def run_macro_theme_agent(max_per_theme: int = 4, progress_cb=None) -> MacroThemeReport:
    """Run scanner and classify themes into tradeable stances."""
    def _cb(msg: str) -> None:
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    _cb("🚀 Starting macro theme agent…")
    report = scan_macro_events(max_per_theme=max_per_theme, progress_cb=progress_cb)
    _cb("📊 Fetching DXY / US10Y / VIX context…")
    quant  = _fetch_quant_context()

    # MacroFundamentals from World Bank / IMF WEO (auto-imported if stale)
    mf = report.quant.macro_fundamentals
    _cb("🧠 Classifying themes + scoring conviction…")
    
    by_theme = defaultdict(list)
    for ev in report.events:
        by_theme[ev.theme].append(ev)
    
    stances = []
    for theme_name, events in by_theme.items():
        stances.append(_classify_theme(theme_name, events, quant, mf))
    
    # Sort by conviction score
    stances.sort(key=lambda s: -s.conviction_score)
    
    long_themes  = [s for s in stances if s.direction == "LONG"]
    short_themes = [s for s in stances if s.direction == "SHORT"]
    mixed_themes = [s for s in stances if s.direction == "MIXED"]
    
    # Net ETF Signal
    net_etf = {}
    for s in stances:
        weight = 2 if s.conviction == "HIGH" else 1
        for etf in s.long_basket:
            net_etf[etf] = net_etf.get(etf, 0) + weight
        for etf in s.short_basket:
            net_etf[etf] = net_etf.get(etf, 0) - weight
            
    return MacroThemeReport(
        as_of=report.as_of,
        long_themes=long_themes,
        short_themes=short_themes,
        mixed_themes=mixed_themes,
        net_etf_signal=net_etf,
        conflicts=_detect_conflicts(long_themes, short_themes),
        quant=quant,
        macro_fundamentals=mf,
    )


# ── Rich Printer ──────────────────────────────────────────────────────────────

def print_macro_theme_report(report: MacroThemeReport) -> None:
    console = Console()

    # 1. Header
    q = report.quant
    q_str = ""
    if q and q.dxy_now:
        q_str = (
            f"  [dim]DXY: {q.dxy_now:.2f} ({q.dxy_5d_chg_pct:+.1f}%)  |  "
            f"US10Y: {q.us10y_now:.2f}% ({q.us10y_5d_chg:+.2f} pts)  |  "
            f"VIX: {q.vix_now:.1f}[/dim]"
        )

    console.print(Panel(
        f"[bold cyan]🤖 Long/Short Macro Theme Agent[/bold cyan]\n"
        f"[dim]{report.as_of}[/dim]{q_str}",
        border_style="cyan",
    ))

    def _print_stances(stances, title, color):
        if not stances: return
        console.print(f"\n[bold {color}]{title}[/bold {color}]")
        console.print(Rule(style=color))
        
        for s in stances:
            conv_style = "bold red" if s.conviction == "HIGH" else "yellow" if s.conviction == "MEDIUM" else "dim"
            console.print(f"{s.icon} [bold]{s.theme}[/bold]  [{conv_style}]{s.conviction} ({s.conviction_score:.0f})[/]")
            console.print(f"   [italic dim]{s.narrative}[/italic dim]")
            
            if s.long_basket:
                console.print(f"   [green]BUY :[/green] {' '.join(s.long_basket)}")
            if s.short_basket:
                console.print(f"   [red]REDUCE :[/red] {' '.join(s.short_basket)}")
            
            if s.quant_confirms:
                console.print(f"   [dim]✅ Confirms: {', '.join(s.quant_confirms)}[/dim]")
            if s.quant_conflicts:
                console.print(f"   [dim]⚠️ Conflicts: {', '.join(s.quant_conflicts)}[/dim]")
            
            # Headlines
            for h in s.top_headlines[:2]:
                console.print(f"   [dim]• {h}[/dim]")
            console.print()

    # 2. Themes
    _print_stances(report.long_themes, "🟢 LONG / OPPORTUNITY THEMES", "green")
    _print_stances(report.short_themes, "🔴 RISK / SHORT THEMES", "red")
    _print_stances(report.mixed_themes, "🟡 MIXED / MONITOR THEMES", "yellow")

    # 3. Net Portfolio
    console.print(Panel("[bold]📊 Aggregated Portfolio Stance (Weighted)[/bold]", border_style="white", expand=False))
    if report.net_etf_signal:
        tbl = Table(box=box.SIMPLE, show_header=True)
        tbl.add_column("ETF", style="cyan")
        tbl.add_column("Headline Impact Index", justify="right")
        tbl.add_column("Action", justify="center")
        tbl.add_column("Strength")

        sorted_net = sorted(report.net_etf_signal.items(), key=lambda x: -x[1])
        for etf, score in sorted_net:
            if score >= 2:     action, color = "BUY", "bold green"
            elif score > 0:    action, color = "ACCUMULATE", "green"
            elif score <= -2:  action, color = "SELL", "bold red"
            elif score < 0:    action, color = "REDUCE", "red"
            else:              action, color = "HOLD", "dim"
            
            bar = "█" * abs(score) if score != 0 else "─"
            tbl.add_row(etf, f"{score:+d}", f"[{color}]{action}[/{color}]", f"[{color.split()[-1]}]{bar}[/]")
        console.print(tbl)
        console.print(
            "[bold yellow]⚠ Heuristic Disclaimer:[/bold yellow] [dim]Headline Impact Index represents narrative volume "
            "scores based on Google News RSS indexing and qualitative event classification. Mapped asset relationships are historical averages "
            "and highly context-dependent in reality (e.g., markets often rally during wars, gold and equities can rise together, and "
            "rate cuts can be bearish if driven by recession fears). It does not represent backtested statistical signals, return expectations, "
            "or model forecasts. For quantitative volatility-scaled sizing, refer to the GOLDBEES ML pipeline.[/dim]\n"
        )

    # 4. Macro Fundamentals panel (World Bank actuals + IMF WEO forecasts)
    mf = report.macro_fundamentals
    if mf and (mf.gdp_growth_pct is not None or mf.gdp_forecast_pct is not None):
        yr_label = f" ({mf.actual_year})" if mf.actual_year else ""
        lines = []

        parts: list[str] = []
        if mf.gdp_growth_pct is not None:
            col = "green" if mf.gdp_growth_pct >= 6.0 else "red"
            parts.append(f"GDP [{col}]{mf.gdp_growth_pct:+.1f}%{yr_label}[/{col}]")
        if mf.cpi_pct is not None:
            col = "green" if mf.cpi_pct <= 5.0 else "yellow"
            parts.append(f"CPI [{col}]{mf.cpi_pct:+.1f}%[/{col}]")
        if mf.ca_balance_pct is not None:
            col = "green" if mf.ca_balance_pct >= -2.0 else "red"
            parts.append(f"CA [{col}]{mf.ca_balance_pct:+.1f}% GDP[/{col}]")
        if mf.fiscal_balance_pct is not None:
            col = "green" if mf.fiscal_balance_pct >= -4.5 else "red"
            parts.append(f"Fiscal [{col}]{mf.fiscal_balance_pct:+.1f}% GDP[/{col}]")
        if parts:
            lines.append("  [bold]Actuals:[/bold]  " + "   ".join(parts))

        fcast_parts: list[str] = []
        if mf.gdp_forecast_pct is not None:
            fcast_parts.append(f"GDP {mf.gdp_forecast_pct:+.1f}%")
        if mf.cpi_forecast_pct is not None:
            fcast_parts.append(f"CPI {mf.cpi_forecast_pct:+.1f}%")
        if fcast_parts:
            yr_f = mf.forecast_year or "fwd"
            lines.append(f"  [bold]IMF {yr_f} forecast:[/bold]  " + "   ".join(fcast_parts))

        if lines:
            console.print()
            console.print(Panel(
                "[bold]🌐 India Macro Fundamentals — World Bank / IMF WEO[/bold]\n"
                + "\n".join(lines),
                border_style="blue",
                expand=False,
            ))

    # 5. Conflicts
    if report.conflicts:
        console.print(f"\n[bold yellow]⚠️ Theme Conflicts Detected[/bold yellow]")
        for c in report.conflicts:
            console.print(f"  [dim]• {c}[/dim]")
        console.print(f"  [italic dim]Note: Net signal resolved using weighted priority.[/italic dim]")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Long/Short Macro Theme Agent")
    parser.add_argument("--max-per-theme", type=int, default=4, help="Max headlines per theme")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))
    
    report = run_macro_theme_agent(max_per_theme=args.max_per_theme)
    
    if args.json:
        import json
        print(json.dumps(asdict(report), indent=2))
    else:
        print_macro_theme_report(report)

if __name__ == "__main__":
    main()
