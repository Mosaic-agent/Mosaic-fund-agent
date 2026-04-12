"""
tests/test_macro_theme_agent.py
──────────────────────────────
Quick test for the Long/Short Macro Theme Agent.
Run with: .venv/bin/python tests/test_macro_theme_agent.py
"""
import sys, os
from dataclasses import asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

def test_macro_theme_agent():
    print("\n" + "="*60)
    print("TEST: Long/Short Macro Theme Agent")
    print("="*60)
    
    from scripts.macro_theme_agent import run_macro_theme_agent, MacroThemeReport
    
    print("  Fetching macro themes (max 1 per theme)...")
    report = run_macro_theme_agent(max_per_theme=1)
    
    assert isinstance(report, MacroThemeReport)
    assert report.as_of is not None
    
    total_themes = len(report.long_themes) + len(report.short_themes) + len(report.mixed_themes)
    print(f"  ✓ Report generated with {total_themes} active themes")
    
    if report.quant:
        print(f"  ✓ Quant context: VIX={report.quant.vix_now}, DXY={report.quant.dxy_now}")
    
    # Test JSON serialization
    data = asdict(report)
    assert "long_themes" in data
    assert "net_etf_signal" in data
    print("  ✓ JSON serialization successful")
    
    if total_themes > 0:
        first_stance = (report.long_themes or report.short_themes or report.mixed_themes)[0]
        print(f"  ✓ Sample theme: {first_stance.theme} ({first_stance.direction})")
        assert first_stance.conviction_score >= 0 and first_stance.conviction_score <= 100
        assert len(first_stance.top_headlines) > 0
    else:
        print("  ⚠ No themes detected (check internet connection or news sources)")

if __name__ == "__main__":
    try:
        test_macro_theme_agent()
        print("\n[bold green]✓ All Macro Theme Agent tests passed![/bold green]")
    except Exception as e:
        print(f"\n[bold red]✗ Test failed: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)
