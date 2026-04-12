
import os
import sys
import logging
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

try:
    from config.settings import settings
    from tools.quant_scorecard import compute_gold_scorecard
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

# Set up logging to be less noisy
logging.basicConfig(level=logging.ERROR)
console = Console()

def run_scorecard():
    console.print("[bold gold1]🥇 Running Gold Quant Scorecard (GOLDBEES)...[/bold gold1]")
    
    scorecard = compute_gold_scorecard(
        ch_host=settings.clickhouse_host,
        ch_port=settings.clickhouse_port,
        ch_user=settings.clickhouse_user,
        ch_pass=settings.clickhouse_password,
        ch_database=settings.clickhouse_database
    )

    if scorecard.get("error"):
        console.print(f"[red]Error:[/red] {scorecard['error']}")
        # We can still show partial data
    
    # ── Display Summary Table ────────────────────────────────────────────────
    summary_table = Table(title="Gold Quantitative Scorecard (GOLDBEES)", show_header=True, header_style="bold magenta")
    summary_table.add_column("Pillar", style="dim")
    summary_table.add_column("Score (0-100)", justify="center")
    summary_table.add_column("Weight", justify="center")
    summary_table.add_column("Impact", justify="center")

    def get_impact(score):
        if score is None: return "N/A"
        if score >= 75: return "[bold green]Strong Bullish[/bold green]"
        if score >= 60: return "[green]Bullish[/green]"
        if score >= 40: return "[yellow]Neutral[/yellow]"
        if score >= 25: return "[red]Bearish[/red]"
        return "[bold red]Strong Bearish[/bold red]"

    summary_table.add_row("Macro (DXY/Yield)", f"{scorecard['macro_score']:.1f}" if scorecard['macro_score'] else "N/A", "30%", get_impact(scorecard['macro_score']))
    summary_table.add_row("Flows (COT/OI)", f"{scorecard['flows_score']:.1f}" if scorecard['flows_score'] else "N/A", "30%", get_impact(scorecard['flows_score']))
    summary_table.add_row("Valuation (iNAV)", f"{scorecard['valuation_score']:.1f}" if scorecard['valuation_score'] else "N/A", "20%", get_impact(scorecard['valuation_score']))
    summary_table.add_row("Momentum (ML)", f"{scorecard['momentum_score']:.1f}" if scorecard['momentum_score'] else "N/A", "20%", get_impact(scorecard['momentum_score']))
    
    summary_table.add_section()
    comp_score = scorecard['composite_score']
    summary_table.add_row("[bold]COMPOSITE SCORE[/bold]", f"[bold]{comp_score:.1f}[/bold]" if comp_score else "N/A", "100%", get_impact(comp_score))
    
    console.print(summary_table)

    # ── Display Raw Signals ──────────────────────────────────────────────────
    signals = scorecard["signals"]
    signal_table = Table(title="Underlying Quant Signals", show_header=True, header_style="bold cyan")
    signal_table.add_column("Signal", style="dim")
    signal_table.add_column("Value")
    signal_table.add_column("Status")

    signal_table.add_row("DXY Level", f"{signals['dxy_level']:.2f}" if signals['dxy_level'] else "N/A", "Lower = Bullish")
    signal_table.add_row("US Real Yield (Est)", f"{signals['real_yield_level']:.2f}%" if signals['real_yield_level'] else "N/A", "Lower = Bullish")
    signal_table.add_row("Real Yield 5D Δ", f"{signals['real_yield_delta5']:+.2f}%" if signals['real_yield_delta5'] else "N/A", "Drop = Bullish")
    signal_table.add_row("COT Spec % of OI", f"{signals['cot_pct_oi']:.1f}%" if signals['cot_pct_oi'] else "N/A", "20-35% Range")
    signal_table.add_row("iNAV Premium/Disc", f"{signals['inav_disc_pct']:+.2f}%" if signals['inav_disc_pct'] else "N/A", "Disc = Bullish")
    signal_table.add_row("LightGBM Pred", f"{signals['lgbm_return_pct']:+.2f}%" if signals['lgbm_return_pct'] else "N/A", "Next 5D Return")

    console.print(signal_table)

if __name__ == "__main__":
    run_scorecard()
