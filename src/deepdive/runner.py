"""
src/deepdive/runner.py
───────────────────────
Orchestration entry point for the `deepdive` CLI command.

Phases implemented:
  1. EDGAR  — company metadata + filing download (MappingApi + EDGAR submissions API)
  2. XBRL   — 5-year financials from 10-K (XbrlApi)
  3. Careers — job postings via platform-specific adapter (get_careers_adapter)
  4. Market  — valuation multiples + peer medians (yfinance)
  5. Extract — SEC section text (ExtractorApi) + exec comp (ExecCompApi)

ClickHouse persistence:
  All fetched data is written to market_data.deepdive_* tables with filing references.
  Watermark table (deepdive_watermarks) prevents re-fetching data already imported
  for the same (ticker, report_date). Analysis phases read from ClickHouse first.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config.settings import settings
from src.deepdive.clickhouse import DeepDiveStore
from src.deepdive.models import AnnualFinancials, CompanyDataset, FilingRefs
from src.deepdive.sources import edgar, xbrl
from src.deepdive.sources.careers import get_adapter as get_careers_adapter
from src.deepdive.sources.market import (  # noqa: E402 (keep with other imports)
    ADSK_PEERS, build_valuation_snapshot, fetch_market_snapshot, fetch_price_history,
)
from src.deepdive.extract import sections as sec_extract
from src.deepdive.extract.compensation import get_exec_comp, summarise_exec_comp

log = logging.getLogger(__name__)
console = Console()


# ── Path helpers ───────────────────────────────────────────────────────────────

def _run_date(date_str: str | None) -> str:
    return date_str or date.today().isoformat()


def _cache_dir(ticker: str, run_date: str) -> Path:  # noqa: ARG001
    # Cache is shared across all run_dates for the same ticker so that
    # large SEC filings and job data are not re-downloaded on each new date.
    return Path(settings.output_dir) / "deepdive" / "cache" / ticker


def _output_dir(ticker: str, run_date: str) -> Path:
    return Path(settings.output_dir) / "deepdive" / ticker / run_date


# ── Main entry point ───────────────────────────────────────────────────────────

def run_deepdive(
    ticker: str,
    date: str | None = None,
    skip_fetch: bool = False,
    section: str | None = None,
) -> None:
    """
    Orchestrate the company deep-dive for the given ticker.

    Args:
        ticker:     Uppercase US ticker symbol e.g. "ADSK"
        date:       Report date YYYY-MM-DD (default: today)
        skip_fetch: If True, only read from cache; skip all network calls
        section:    If set, re-generate only the named section (phases 6–7)
    """
    run_date = _run_date(date)
    cache_dir = _cache_dir(ticker, run_date)
    out_dir = _output_dir(ticker, run_date)

    cache_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    console.print(
        Panel(
            f"[bold cyan]Company Deep-Dive[/bold cyan]  [white]{ticker}[/white]  "
            f"[dim]{run_date}[/dim]",
            expand=False,
        )
    )

    api_key = settings.sec_api_key
    if not api_key:
        console.print("[red]ERROR:[/red] SEC_API_KEY is not set in .env. Cannot fetch SEC data.")
        return

    # ── Init ClickHouse store (non-fatal if CH unavailable) ───────────────────
    ch = DeepDiveStore()
    ch_available = ch._ready
    if ch_available:
        console.print("  [dim]ClickHouse: connected — data will be persisted[/dim]")
    else:
        console.print("  [dim]ClickHouse: unavailable — running without persistence[/dim]")

    # ── Phase 1: EDGAR ────────────────────────────────────────────────────────
    console.print("[bold]Phase 1:[/bold] Fetching SEC filings…")

    if skip_fetch:
        # Load cached metadata only
        meta_path = cache_dir / "company_meta.json"
        meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        filings_path = cache_dir / "filings_index.json"
        filings = json.loads(filings_path.read_text()) if filings_path.exists() else []
        downloaded: dict = {}
    else:
        result = edgar.fetch_standard_filings(
            ticker=ticker,
            cache_dir=cache_dir,
            api_key=api_key,
            scrape_delay=settings.scrape_delay_seconds,
        )
        meta = result["company_meta"]
        filings = result["filings"]
        downloaded = result["downloaded"]

    company_name = meta.get("name", ticker)
    console.print(f"  Company : [cyan]{company_name}[/cyan]  CIK: {meta.get('cik', '?')}")
    console.print(f"  Filings : {len(filings)} found")

    # Persist filings index to ClickHouse
    ch.insert_filings(ticker, run_date, filings, meta)

    # Build FilingRefs from the filings index
    ten_k_filings = [f for f in filings if f["form_type"] == "10-K"]
    ten_q_filings = [f for f in filings if f["form_type"] == "10-Q"]
    def14a_filings = [f for f in filings if f["form_type"] == "DEF 14A"]

    filing_refs = FilingRefs(
        annual_10k_url=ten_k_filings[0]["filing_url"] if ten_k_filings else "",
        annual_10k_filed=ten_k_filings[0]["filed_date"] if ten_k_filings else "",
        quarterly_10q_urls=[f["filing_url"] for f in ten_q_filings],
        proxy_def14a_url=def14a_filings[0]["filing_url"] if def14a_filings else "",
        accession_no_10k=ten_k_filings[0]["accession_no"] if ten_k_filings else "",
    )

    # ── Phase 2: XBRL ─────────────────────────────────────────────────────────
    console.print("[bold]Phase 2:[/bold] Fetching XBRL financials…")

    xbrl_json: dict = {}
    financials_rows: list[dict] = []

    # Check CH first — skip XBRL API call if already imported for this run_date
    if ch.is_imported(ticker, "financials", run_date):
        financials_rows = ch.load_financials(ticker, run_date)
        console.print(f"  XBRL    : [dim]{len(financials_rows)} rows loaded from ClickHouse[/dim]")
    elif filing_refs.annual_10k_url and filing_refs.accession_no_10k:
        xbrl_json = xbrl.get_financials(
            htm_url=filing_refs.annual_10k_url,
            accession_no=filing_refs.accession_no_10k,
            cache_dir=cache_dir,
            api_key=api_key,
        )
        if xbrl_json:
            financials_rows = xbrl.build_financials(xbrl_json, years=5)
            console.print(
                f"  XBRL    : {len(financials_rows)} annual periods extracted"
            )
        else:
            console.print("  [yellow]XBRL data unavailable — skipping financials[/yellow]")
    else:
        console.print("  [yellow]No 10-K URL found — skipping XBRL[/yellow]")

    # ── Build CompanyDataset ───────────────────────────────────────────────────
    financials = [AnnualFinancials(**r) for r in financials_rows if r.get("revenue_usd_m")]

    # Persist financials to ClickHouse (skip if already imported from CH above)
    if financials and xbrl_json:  # only write when freshly fetched from XBRL API
        tenk_ref = {
            "accession_no": filing_refs.accession_no_10k,
            "filing_url": filing_refs.annual_10k_url,
            "form_type": "10-K",
            "filed_date": filing_refs.annual_10k_filed,
        }
        ch.insert_financials(ticker, run_date, financials, tenk_ref)

    # Derive top-level R&D / CapEx from most-recent fiscal year
    rd_spend: float | None = None
    rd_pct: float | None = None
    capex: float | None = None
    if financials:
        latest = financials[-1]
        rd_spend = latest.rd_expense_usd_m
        capex = latest.capex_usd_m
        if rd_spend and latest.revenue_usd_m:
            rd_pct = round(rd_spend / latest.revenue_usd_m * 100, 1)

    dataset = CompanyDataset(
        ticker=ticker,
        company_name=company_name,
        report_date=run_date,
        fiscal_year_end=financials[-1].fiscal_year if financials else "",
        cik=meta.get("cik", ""),
        sic=meta.get("sic", ""),
        exchange=meta.get("exchange", ""),
        filing_refs=filing_refs,
        financials=financials,
        rd_spend_usd_m=rd_spend,
        rd_pct_of_revenue=rd_pct,
        capex_usd_m=capex,
        sources=[
            {
                "field": "financials",
                "file": str(cache_dir / f"xbrl_{filing_refs.accession_no_10k.replace('-', '')}.json"),
                "locator": "StatementsOfIncome",
            }
        ]
        if xbrl_json
        else [],
    )

    dataset_path = out_dir / "dataset.json"

    # ── Print financials table ─────────────────────────────────────────────────
    if financials:
        _print_financials_table(financials)

    # ── Phase 3: Workday careers ─────────────────────────────────────
    console.print("[bold]Phase 3:[/bold] Fetching Workday job postings…")

    jobs_cache = cache_dir / "workday_jobs_raw.json"
    jobs_raw: list[dict] = []

    if ch.is_jobs_imported_this_month(ticker):
        console.print("  Jobs    : [dim]already fetched this month — skipping[/dim]")
    elif skip_fetch and not jobs_cache.exists():
        console.print("  [dim]--skip-fetch: no jobs cache found, skipping[/dim]")
    else:
        try:
            adapter = get_careers_adapter(ticker)
            if adapter is None:
                console.print(f"  [dim]No careers adapter registered for {ticker} — skipping[/dim]")
            else:
                jobs_raw = adapter.fetch_all_jobs(cache_path=jobs_cache)
                console.print(f"  Jobs    : {len(jobs_raw)} postings fetched")
        except Exception as exc:
            log.warning("careers fetch failed: %s", exc)
            console.print(f"  [yellow]Jobs fetch failed: {exc}[/yellow]")

    # ── Phase 4: Market data ───────────────────────────────────────────────────
    console.print("[bold]Phase 4:[/bold] Fetching market data & peer multiples…")

    market_cache = cache_dir / "market_snapshot.json"
    valuation = None

    if ch.is_imported(ticker, "valuation", run_date):
        val_row = ch.load_valuation(ticker, run_date)
        if val_row:
            from src.deepdive.models import ValuationSnapshot  # noqa: PLC0415
            valuation = ValuationSnapshot(**val_row)
            console.print("  Market  : [dim]valuation loaded from ClickHouse[/dim]")
    elif skip_fetch and not market_cache.exists():
        console.print("  [dim]--skip-fetch: no market cache found, skipping[/dim]")
    else:
        try:
            peers = ADSK_PEERS if ticker == "ADSK" else []
            market_data = fetch_market_snapshot(
                ticker=ticker,
                peers=peers,
                cache_path=market_cache,
                report_date=run_date,
            )
            valuation = build_valuation_snapshot(market_data)
            ch.insert_valuation(ticker, run_date, valuation)
            console.print(
                f"  Market cap : [cyan]${valuation.market_cap_usd_b:.1f}B[/cyan]  "
                f"P/E (trail): {valuation.pe_trailing or '—'}  "
                f"EV/Rev: {valuation.ev_revenue or '—'}  "
                f"FCF yield: {valuation.fcf_yield_pct or '—'}%"
            )
            console.print(
                f"  Peer medians → P/E: {valuation.peer_pe_median or '—'}  "
                f"EV/EBITDA: {valuation.peer_ev_ebitda_median or '—'}  "
                f"EV/Rev: {valuation.peer_ev_revenue_median or '—'}"
            )
        except Exception as exc:
            log.warning("market fetch failed: %s", exc)
            console.print(f"  [yellow]Market fetch failed: {exc}[/yellow]")

    # ── Price history (2 years, daily OHLCV) ──────────────────────────────────
    price_cache = cache_dir / "price_history.json"
    if ch.prices_up_to_date(ticker):
        console.print("  Prices  : [dim]2y history loaded from ClickHouse[/dim]")
    elif skip_fetch and not price_cache.exists():
        console.print("  [dim]--skip-fetch: no price cache, skipping[/dim]")
    else:
        try:
            price_rows = fetch_price_history(ticker, cache_path=price_cache, years=2)
            if price_rows:
                ch.insert_prices(ticker, price_rows)
                console.print(f"  Prices  : [cyan]{len(price_rows)}[/cyan] daily bars (2y)")
        except Exception as exc:
            log.warning("price history fetch failed: %s", exc)
            console.print(f"  [yellow]Price history failed: {exc}[/yellow]")

    # ── Update dataset with phases 3+4 data ───────────────────────────────────
    dataset.valuation = valuation

    if jobs_raw:
        from src.deepdive.extract.jobs_signal import bucket_jobs  # noqa: PLC0415
        dataset.jobs = bucket_jobs(jobs_raw)
        ch.insert_jobs(ticker, run_date, dataset.jobs)
    elif ch.is_imported(ticker, "jobs", run_date):
        dataset.jobs = ch.load_jobs(ticker, run_date)

    if market_cache.exists():
        dataset.sources.append({
            "field": "valuation",
            "file": str(market_cache),
            "locator": "yfinance.Ticker.info",
        })
    if jobs_cache.exists():
        _ad = get_careers_adapter(ticker)
        locator = (
            getattr(type(_ad), "sitemap_url", None)
            or f"{type(_ad).__name__} careers API"
            if _ad else "careers cache"
        )
        dataset.sources.append({
            "field": "jobs",
            "file": str(jobs_cache),
            "locator": locator,
        })

    # Re-write dataset.json now that phases 3+4 are populated
    dataset_path.write_text(
        dataset.model_dump_json(indent=2, exclude_none=False), encoding="utf-8"
    )
    console.print(f"\n[green]✓[/green] dataset.json updated → [dim]{dataset_path}[/dim]")

    if valuation:
        _print_valuation_table(valuation, ticker)

    # ── Phase 5: Section extraction + exec comp ───────────────────────────────
    console.print("[bold]Phase 5:[/bold] Extracting 10-K sections & exec comp…")

    ten_k_url = filing_refs.annual_10k_url

    # 5a: Business section (Item 1) — headcount + competition text
    biz_cache = cache_dir / "section1_business.txt"
    if ch.is_imported(ticker, "headcount", run_date):
        hc_rows = ch.load_headcount(ticker, run_date)
        if hc_rows:
            from src.deepdive.models import HeadcountData  # noqa: PLC0415
            dataset.headcount = [HeadcountData(
                period=r["period"],
                total_headcount=r["total_headcount"],
                notes=r.get("notes", ""),
            ) for r in hc_rows]
            console.print(f"  Headcount : [dim]{hc_rows[0]['total_headcount']:,} loaded from ClickHouse[/dim]")
        # Still parse competition text from local cache if available
        if biz_cache.exists():
            business_text = biz_cache.read_text(encoding="utf-8", errors="replace")
            hc = sec_extract.extract_headcount_from_text(business_text)
            competition_text = sec_extract.extract_competition_text(business_text)
            dataset.filing_excerpts.competition_section_text = competition_text[:3000]
            dataset.filing_excerpts.headcount_notes_text = hc.get("notes", "")
    else:
        business_text = sec_extract.get_business_section(ten_k_url, biz_cache, api_key)
        if business_text:
            hc = sec_extract.extract_headcount_from_text(business_text)
            if hc.get("total_headcount"):
                from src.deepdive.models import HeadcountData  # noqa: PLC0415
                dataset.headcount = [HeadcountData(
                    period=dataset.fiscal_year_end,
                    total_headcount=hc["total_headcount"],
                    notes=hc.get("notes", ""),
                )]
                console.print(f"  Headcount : [cyan]{hc['total_headcount']:,}[/cyan] employees")
                ch.insert_headcount(ticker, run_date, dataset.headcount, {
                    "accession_no": filing_refs.accession_no_10k,
                    "filing_url": filing_refs.annual_10k_url,
                    "form_type": "10-K",
                    "filed_date": filing_refs.annual_10k_filed,
                })
            competition_text = sec_extract.extract_competition_text(business_text)
            dataset.filing_excerpts.competition_section_text = competition_text[:3000]
            dataset.filing_excerpts.headcount_notes_text = hc.get("notes", "")
            console.print(f"  Business  : {len(business_text):,} chars extracted")
        else:
            console.print("  [yellow]Business section unavailable[/yellow]")

    # 5b: MD&A section (Item 7) — segment revenue table
    mda_cache = cache_dir / "section7_mda.html"
    if ch.is_imported(ticker, "segments", run_date):
        segs = ch.load_segments(ticker, run_date)
        if segs:
            from src.deepdive.models import SegmentRevenue  # noqa: PLC0415
            dataset.segments = [SegmentRevenue(**s) for s in segs]
            console.print(f"  Segments  : [dim]{len(segs)} rows loaded from ClickHouse[/dim]")
        if mda_cache.exists():
            dataset.filing_excerpts.segment_table_text = mda_cache.read_text(
                encoding="utf-8", errors="replace")[:500]
    else:
        mda_html = sec_extract.get_segment_section(ten_k_url, mda_cache, api_key)
        if mda_html:
            segments_raw = sec_extract.parse_segment_table(mda_html)
            if segments_raw:
                from src.deepdive.models import SegmentRevenue  # noqa: PLC0415
                dataset.segments = [SegmentRevenue(**s) for s in segments_raw]
                console.print(f"  Segments  : {len(segments_raw)} rows — " +
                              ", ".join(f"{s['name']} ${s['revenue_usd_m']:,.0f}M" for s in segments_raw[:4]))
                ch.insert_segments(
                    ticker, run_date,
                    dataset.fiscal_year_end,
                    dataset.segments,
                    {
                        "accession_no": filing_refs.accession_no_10k,
                        "filing_url": filing_refs.annual_10k_url,
                        "form_type": "10-K",
                        "filed_date": filing_refs.annual_10k_filed,
                    },
                )
            else:
                console.print("  [yellow]No segment table parsed from MD&A[/yellow]")
            dataset.filing_excerpts.segment_table_text = mda_html[:500]  # trimmed ref
        else:
            console.print("  [yellow]MD&A section unavailable[/yellow]")

    # 5c: Executive compensation
    comp_cache = cache_dir / "exec_comp.json"
    if ch.is_imported(ticker, "exec_comp", run_date):
        summaries = ch.load_exec_comp(ticker, run_date)
        if summaries:
            console.print(f"  Exec comp : [dim]{len(summaries)} NEOs loaded from ClickHouse[/dim]")
            dataset.sources.append({
                "field": "exec_comp",
                "file": "clickhouse://market_data.deepdive_exec_comp",
                "locator": f"ticker={ticker} report_date={run_date}",
            })
    else:
        exec_rows = get_exec_comp(ticker, comp_cache, api_key)
        if exec_rows:
            summaries = summarise_exec_comp(exec_rows)
            console.print(f"  Exec comp : {len(summaries)} NEOs — CEO total " +
                          next((f"${r['total_usd_m']:.1f}M" for r in summaries
                                if "chief executive" in r.get("position", "").lower()), "?"))
            # DEF 14A filing reference for exec comp
            def14a_ref = {
                "accession_no": def14a_filings[0]["accession_no"] if def14a_filings else "",
                "filing_url": filing_refs.proxy_def14a_url,
                "form_type": "DEF 14A",
                "filed_date": def14a_filings[0]["filed_date"] if def14a_filings else "",
            }
            ch.insert_exec_comp(ticker, run_date, summaries, def14a_ref)
            dataset.sources.append({
                "field": "exec_comp",
                "file": str(comp_cache),
                "locator": "ExecCompApi.get_data",
            })

    # Add section sources
    if biz_cache.exists():
        dataset.sources.append({"field": "filing_excerpts.competition_section_text",
                                "file": str(biz_cache), "locator": "Item 1 Business"})
    if mda_cache.exists():
        dataset.sources.append({"field": "segments",
                                "file": str(mda_cache), "locator": "Item 7 MD&A"})

    # Final dataset write with all phases populated
    dataset_path.write_text(
        dataset.model_dump_json(indent=2, exclude_none=False), encoding="utf-8"
    )
    console.print(f"\n[green]✓[/green] dataset.json final → [dim]{dataset_path}[/dim]")

    if dataset.segments:
        _print_segments_table(dataset.segments, ticker)

    # ── Phase 6: Emit assembled prompts for Gemini CLI ────────────────────────
    # This command is intended to be run directly by Gemini CLI as a tool.
    # Phase 6 assembles each section prompt (static rules + dataset JSON + task)
    # and prints them to stdout.  Gemini CLI reads this output and generates the
    # narrative in its own response — no subprocess or API call needed here.
    console.print("[bold]Phase 6:[/bold] Assembling section prompts for Gemini CLI…")

    from src.deepdive.analyze.gemini_cli import PROMPTS_DIR, SECTION_KEYS, assemble_prompt  # noqa: PLC0415

    dataset_json = dataset_path.read_text(encoding="utf-8")
    targets = [section] if section and section in SECTION_KEYS else SECTION_KEYS

    prompts_dir = out_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)

    for key in targets:
        try:
            full_prompt = assemble_prompt(key, dataset_json, PROMPTS_DIR)
        except FileNotFoundError as exc:
            console.print(f"  [yellow]⚠  {exc}[/yellow]")
            continue

        # Save assembled prompt to disk (Gemini CLI can reference these files)
        prompt_out = prompts_dir / f"{key}_assembled.txt"
        prompt_out.write_text(full_prompt, encoding="utf-8")

        # Print a clearly-delimited block to stdout so Gemini CLI can read and
        # respond to each section in sequence.
        print(f"\n{'='*72}")
        print(f"SECTION: {key}")
        print(f"{'='*72}")
        print(full_prompt)
        print(f"{'='*72}\nEND SECTION: {key}\n")

    console.print(
        f"  Prompts   : {len(targets)} assembled → [dim]{prompts_dir}[/dim]"
    )

    # ── Phase 7: Report assembly ───────────────────────────────────────────────
    # Sections are written to sections/<key>.md by the caller (Claude CLI) after
    # reading the Phase 6 prompt blocks from stdout and generating the content.
    # This phase assembles whatever section files are present into report.md.
    console.print("[bold]Phase 7:[/bold] Assembling report.md + sources.md…")

    from src.deepdive.report import SECTION_ORDER, assemble_report  # noqa: PLC0415

    sections_dir = out_dir / "sections"

    # Report which sections are present / missing before assembly
    present = [k for k, _ in SECTION_ORDER if (sections_dir / f"{k}.md").exists()
               and (sections_dir / f"{k}.md").stat().st_size > 100]
    missing = [k for k, _ in SECTION_ORDER if k not in present]
    if present:
        console.print(f"  Sections  : {len(present)} ready — {', '.join(present)}")
    if missing:
        console.print(f"  [yellow]Missing   : {', '.join(missing)}[/yellow]")

    try:
        report_path, sources_path = assemble_report(
            out_dir=out_dir,
            cache_dir=cache_dir,
        )
        report_kb = report_path.stat().st_size / 1024
        sources_kb = sources_path.stat().st_size / 1024
        console.print(
            f"  [green]✓[/green] report.md   → [dim]{report_path}[/dim] "
            f"([cyan]{report_kb:.1f} KB[/cyan])"
        )
        console.print(
            f"  [green]✓[/green] sources.md  → [dim]{sources_path}[/dim] "
            f"([cyan]{sources_kb:.1f} KB[/cyan])"
        )

        # Persist report to ClickHouse so the UI reads from CH, not the filesystem
        sections_payload: dict[str, tuple[str, str]] = {}
        for key, heading in SECTION_ORDER:
            section_file = sections_dir / f"{key}.md"
            if section_file.exists():
                sections_payload[key] = (heading, section_file.read_text(encoding="utf-8"))
        ch.insert_report(
            ticker, run_date,
            sections_payload,
            full_report_md=report_path.read_text(encoding="utf-8"),
            sources_md=sources_path.read_text(encoding="utf-8"),
        )
    except Exception as exc:
        log.error("report assembly failed: %s", exc)
        console.print(f"  [red]✗ Report assembly failed: {exc}[/red]")


# ── Rich table helper ──────────────────────────────────────────────────────────

def _print_financials_table(financials: list[AnnualFinancials]) -> None:
    table = Table(title="Financials (USD millions)", show_lines=True, expand=False)
    table.add_column("FY", style="cyan", no_wrap=True)
    table.add_column("Revenue", justify="right")
    table.add_column("Gross Profit", justify="right")
    table.add_column("Op. Income", justify="right")
    table.add_column("Net Income", justify="right")
    table.add_column("FCF", justify="right")
    table.add_column("R&D", justify="right")
    table.add_column("GM%", justify="right")

    for f in financials:
        def _fmt(v: float | None) -> str:
            return f"{v:,.1f}" if v is not None else "—"

        table.add_row(
            f.fiscal_year,
            _fmt(f.revenue_usd_m),
            _fmt(f.gross_profit_usd_m),
            _fmt(f.operating_income_usd_m),
            _fmt(f.net_income_usd_m),
            _fmt(f.free_cash_flow_usd_m),
            _fmt(f.rd_expense_usd_m),
            f"{f.gross_margin_pct:.1f}%" if f.gross_margin_pct is not None else "—",
        )

    console.print(table)


def _print_valuation_table(valuation: "ValuationSnapshot", ticker: str) -> None:  # noqa: F821
    from src.deepdive.models import ValuationSnapshot  # local import avoids circular
    table = Table(title=f"Valuation — {ticker} vs Peers", show_lines=True, expand=False)
    table.add_column("Metric", style="cyan")
    table.add_column(ticker, justify="right")
    table.add_column("Peer Median", justify="right")

    def _f(v: float | None) -> str:
        return f"{v:.1f}x" if v is not None else "—"

    def _pct(v: float | None) -> str:
        return f"{v:.1f}%" if v is not None else "—"

    rows = [
        ("Market Cap ($B)", f"${valuation.market_cap_usd_b:.1f}B" if valuation.market_cap_usd_b else "—", "—"),
        ("P/E (trailing)", _f(valuation.pe_trailing), _f(valuation.peer_pe_median)),
        ("P/E (forward)", _f(valuation.pe_forward), "—"),
        ("EV / Revenue", _f(valuation.ev_revenue), _f(valuation.peer_ev_revenue_median)),
        ("EV / EBITDA", _f(valuation.ev_ebitda), _f(valuation.peer_ev_ebitda_median)),
        ("FCF Yield", _pct(valuation.fcf_yield_pct), "—"),
    ]
    for metric, primary, peer in rows:
        table.add_row(metric, primary, peer)

    console.print(table)


def _print_segments_table(segments: list, ticker: str) -> None:
    from src.deepdive.models import SegmentRevenue
    table = Table(title=f"Segment Revenue — {ticker}", show_lines=True, expand=False)
    table.add_column("Segment", style="cyan")
    table.add_column("Revenue ($M)", justify="right")
    table.add_column("YoY %", justify="right")

    for s in segments:
        table.add_row(
            s.name,
            f"{s.revenue_usd_m:,.1f}" if s.revenue_usd_m is not None else "—",
            f"{s.yoy_growth_pct:+.1f}%" if s.yoy_growth_pct is not None else "—",
        )

    console.print(table)
