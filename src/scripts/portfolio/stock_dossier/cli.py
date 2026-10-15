"""
src/scripts/portfolio/stock_dossier/cli.py
─────────────────────────────────────────────
Entry point for `python src/main.py stock-research <SYMBOL>`.

Generic orchestrator: builds the DossierContext once, then runs every
enabled() section in REGISTRY concurrently via the Strategy pattern's
`.run()` template method — it has no knowledge of what any individual section
does. Adding a capability means adding a file under sections/ and one line in
registry.py; this file does not change.
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from .context import build_context  # noqa: E402
from .registry import REGISTRY  # noqa: E402
from .report import assemble_report, save_report  # noqa: E402


def run_dossier(query: str, days: int = 120, live: bool = False, artifact_dir: str | None = None) -> str:
    ctx = build_context(query, days=days, live=live)

    active = [s for s in REGISTRY if s.enabled(ctx)]
    print(f"\nRunning {len(active)} dossier sections...")
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, len(active))) as ex:
        futures = {ex.submit(section.run, ctx): section for section in active}
        for fut in as_completed(futures):
            section = futures[fut]
            results[section.key] = fut.result()
            print(f" ✓ {section.key}")

    report_md = assemble_report(ctx, results)
    saved_path = save_report(ctx.symbol, report_md, artifact_dir)
    print(f"\n💾 SUCCESS: Report saved to: {saved_path}")
    return report_md


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolidated institutional stock research dossier")
    parser.add_argument("symbol", type=str, help="Stock symbol or company name, e.g. SEDEMAC")
    parser.add_argument("--days", type=int, default=120, help="Lookback window in trading days")
    parser.add_argument("--live", action="store_true", help="Include live Shoonya order-book imbalance (market hours only)")
    parser.add_argument("--artifact-dir", type=str, default=None, help="Override output directory")
    args = parser.parse_args()

    report = run_dossier(args.symbol, days=args.days, live=args.live, artifact_dir=args.artifact_dir)
    print(report)
