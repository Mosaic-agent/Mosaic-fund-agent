---
name: stock-research
description: Generate a 360° institutional equity research dossier, multi-year financial statements, balance sheet analysis, promoter evaluation, mutual fund & sovereign whale tracking, valuation benchmarking, and terminal ASCII charts for any Indian stock.
---

# Institutional Stock Research & Deep Search Skill (`/stock-research`)

This skill generates an exhaustive, 360-degree institutional equity research and technological deep-dive dossier for any Indian stock, SME, or emerging franchise. It integrates:

1. **Executive Profile, Capital Structure & Ownership:**
   - Market cap, shares outstanding, active free float, promoter vs. institutional ownership.
   - Promoter pedigree, academic/technical founding background, executive remuneration, board independence, and skin in the game.
   - PE/VC exits vs. ongoing marquee institutional backing.

2. **Core Business Architecture & Technical Complexity:**
   - Deep dive into actual solutions/products sold, system-level architecture, software vs. hardware integration.
   - Underlying physics/engineering complexity (e.g. sensor-free / sensorless control, sliding-mode observers, high-frequency signal injection, magnetic saliency, motor control algorithms).
   - What makes the offering hard to replicate vs. commodity hardware.

3. **Proprietary Patents & Intellectual Property (IP) Moat:**
   - Registered patents, application numbers, priority dates, and global jurisdictions (US, EP, IN, CN).
   - Specific patent claims, technological defensibility, and protection against reverse-engineering.

4. **Talent Structure & R&D Capability:**
   - Employee base breakdown: % of engineering/R&D staff vs. manufacturing/admin.
   - R&D center locations, prototyping & Hardware-in-the-Loop (HIL) testing infrastructure, dynamometers.
   - R&D expenditure as % of revenue and capitalization policies.

5. **Upcoming Product Pipeline & Addressable TAM Expansion:**
   - Next 24–36 month product roadmap and commercialization milestones.
   - Content expansion per unit/vehicle (e.g. ICE to Hybrid/EV content multiplier).
   - Cross-industry applications and addressable market expansion.

6. **Pricing Power & Cost Pass-Through Mechanics:**
   - Ability to pass on raw material/semiconductor/commodity inflation.
   - Contractual price escalation and indexation clauses with OEMs.
   - Gross margin resiliency during supply-chain shocks.

7. **Global Competition & The China Moat:**
   - Direct competitor mapping: Global Tier-1s (Bosch, Denso, Continental), domestic JVs, and Chinese component makers.
   - Algorithmic/software IP moat vs. low-cost Chinese hardware dumping.
   - Regulatory barriers: PLI (Production Linked Incentive) localization mandates, Phased Manufacturing Programme (PMP), customs tariffs.
   - "China+1" export tailwinds and international OEM qualification cycles.

8. **Multi-Year Financial Statements & Balance Sheet Quality:**
   - 4-year P&L progression: Revenue CAGR, EBITDA margin expansion, operational leverage, PAT CAGR.
   - Balance Sheet Capitalization: Gross Debt, Cash & Equivalents, Net Debt trajectory (de-leveraging trends), Working Capital cycles.
   - Cash Flow Dynamics: Operating Cash Flow (OCF) vs. Capex, Free Cash Flow (FCF) conversion rate.
   - Annual reports, DRHP/RHP filings, and management commentary takeaways (MD&A, order book visibility).

9. **Institutional Whales, Block Deals & Mutual Fund Tracking:**
   - Live query of `market_data.mf_holdings`: All mutual funds holding the stock, Scheme names, holding value (₹ Cr), % of AUM, holding duration.
   - Real-time `market_data.bulk_block_deals`: Exact block date, buyer vs. seller identity, price, volume, and total deal size.
   - Sovereign wealth funds (e.g. GIC, MAS), marquee global institutional investors, and domestic AMC consensus.

10. **Valuation Spectrum & Buy/Wait Strategy ("Cheap vs. Expensive Price Range"):**
    - Multiples: Trailing & Forward P/E, P/S, EV/EBITDA, P/B, ROCE, ROE, PEG ratio.
    - Defined Valuation Spectrum Brackets:
      * **Cheap / High-Margin of Safety Zone:** Price range, implied multiples, and institutional block floor anchor.
      * **Fair Value / Base Case Zone:** Price range, steady-state multiple matching earnings growth.
      * **Fully Priced / Expensive Zone:** Price range, stretched growth expectations where upside is limited.
      * **Severe Overvaluation / Bubble Zone:** Levels where risk-reward turns negative.
    - Clear, definitive verdict: **"Should I buy it now or wait based on valuation?"**

11. **Major Risks & Structural Investment Thesis:**
    - Core structural bull thesis (multi-year tailwinds, unit economics, expanding TAM).
    - Bear case & fragility factors (OEM customer concentration, key-person risk, tech obsolescence, multiple derating).

12. **Tactical Execution Playbook:**
    - Concrete 2-tranche entry zones (starter tranche vs. core accumulation limit orders near block floors).
    - Hard invalidation stop-loss level.
    - 12–24 month upside price targets and asymmetric risk-reward ratio.

13. **Terminal Visual Suite:**
    - High-density ASCII plotext charts (price/volume history with anomaly/corporate-action markers).
    - A publication-grade PDF is NOT generated automatically for arbitrary symbols — see the
      `generate_sedemac_pdf.py` note under CLI Execution below.

---

## Trigger Phrases & Inquiry Vectors

Recommend or trigger this skill when the user asks any of the following questions:
- "research [STOCK]" / "deep dive on [STOCK]" / "analyse [STOCK]"
- "deep down on its solution, complexity in their offering, competitor get detail about its patent"
- "any detail available about its sensor free / proprietary tech for [STOCK]"
- "research patents, employee base, upcoming products for [STOCK]"
- "does [STOCK] have ability to pass on cost?"
- "competition from china for [STOCK]"
- "major risk and thesis for [STOCK]"
- "run valuation metrics identify which mf owns it"
- "should i buy it or wait based on valuation for [STOCK]"
- "cheap vs expensive price range for [STOCK]"
- "did company publish any annual report or commentary for [STOCK]"
- "who is buying [STOCK] in bulk/block deals?"
- "add complete report and chart in pdf for [STOCK]"
- or invokes `/stock-research <SYMBOL>`.

---

## CLI Execution & Pipeline Tools

Run the consolidated institutional research dossier (single command — replaces the
former 3-script manual pipeline of stock_research_report.py /
stock_quant_deepdive_cli.py / run_stock_quant_workflow.py, which duplicated each
other's work and were never registered as CLI commands):
```bash
# 1. Full institutional dossier: financials, DCF, dilution audit, ISIN-keyed MF/whale
#    ownership, bulk/block deals, valuation universe, anomalies + news correlation,
#    NSE announcements, MA crossover backtest, Qdrant RAG news, ASCII charts, and
#    the 2-tranche tactical execution playbook:
python src/main.py stock-research <SYMBOL> [--days 120] [--live] [--artifact-dir <DIR>]

# 2. Segment-wide (not single-symbol) mutual fund institutional accumulation screen —
#    use alongside the dossier above, not instead of it:
python src/scripts/portfolio/smallcap_pattern_analyzer.py --amc all
```

A publication-grade PDF generator for a single named company
(`generate_sedemac_pdf.py`) exists in the repo, but it is a one-off hardcoded case
study for SEDEMAC Mechatronics specifically — not a generic template. Do not invoke
it for other symbols; building a parameterized PDF export is a separate, not-yet-done
piece of work.

---

## Architecture & Extending

`src/main.py stock-research` is backed by `src/scripts/portfolio/stock_dossier/`, a
Strategy-pattern package (same shape as `SignalSource` in
`src/agents/signal_sources.py` and `Fetcher` in
`src/data_importer/fetchers/adapters.py`):

- `context.py` — builds `DossierContext` once per run: symbol resolution, freshness
  check, canonical ClickHouse-first price history, one shared yfinance snapshot.
- `base.py` — `DossierSection` ABC: `enabled(ctx)` (gate, e.g. OBI only runs with
  `--live`), `fetch(ctx)` (returns this section's markdown body), `run(ctx)`
  (template method — isolates one section's failure from the rest of the report).
- `sections/` — one file, one class per report segment (profile, chart, financials,
  momentum, whale, deals, valuation, anomalies, nse_events, news, crossover, dcf,
  dilution, tactical, obi).
- `registry.py` — `REGISTRY: list[DossierSection]`, the single extension point.
  List order is the report's section order; `report.py` numbers headings from that
  order, so inserting or reordering a section needs no renumbering elsewhere.
- `cli.py` / `report.py` — generic orchestrator + assembler; neither has any
  knowledge of what an individual section does.

**To add a new dossier capability:** create `sections/<name>.py` with a
`DossierSection` subclass, then append an instance to `REGISTRY` in `registry.py`.
No other file needs to change. Do not add a new standalone script for stock research
— that recreates the duplication this package was built to remove.

---

## Output Artifacts

* Prints the complete formatted dossier with ASCII visual charts to `stdout`.
* Automatically generates a persistent markdown report at:
  `output/<symbol lowercased>_institutional_research.md`
* A PDF version is not generated for arbitrary symbols (see CLI Execution note above).

---

## Computational & Verification Mandates

1. **No LLM Calculations:** Never calculate, estimate, or derive ratios internally. Always cite figures verified by ClickHouse, regulatory filings, or Python tools.
2. **Verify Dilution Before Flagging Promoter Sale:** Cross-reference changes in equity capital / total shares before assuming open-market promoter offloading.
3. **Institutional Cost Anchor:** Always anchor the "Cheap Zone" against the volume-weighted average price (VWAP) of recent sovereign and institutional bulk/block deals.
4. **Data Provenance:** Always cite exact data sources (ClickHouse `market_data.mf_holdings FINAL`, `market_data.bulk_block_deals FINAL`, SEBI filings, patent office registries).
