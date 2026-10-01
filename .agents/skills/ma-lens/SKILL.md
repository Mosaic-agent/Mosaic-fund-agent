---
name: ma-lens
description: Screen any NSE symbol through a DSP Multi Asset fund manager's lens - five gates (liquidity, cap rank, ownership breadth, valuation, drawdown) derived live from DSP_MULTI_ASSET's own book, plus a price-neutral accumulation test showing whether DSP is actually buying or just being marked up. Use when the user asks "should I buy X", "is X attractive", "screen X", "what would a multi-asset manager do with X", "which of these to invest in", or invokes /ma-lens.
---

# Skill: Multi Asset Manager Lens

Screen one or more NSE symbols the way a DSP Multi Asset fund manager would: structural
constraints first, narrative never.

## Trigger

Use when the user asks:
- "Should I invest in X?" / "Is X attractive?" / "Screen X"
- "Which of these should I buy?" (pass all the symbols)
- "What would a multi-asset manager do with X?"
- "/ma-lens SYMBOL [SYMBOL...]"

Do **not** use for ETFs or index funds - the gates assume a single-name equity with an
AMFI cap classification. Use `/goldbees-pipeline` or `/etf-premium-discount` for those.

## Usage

```bash
./mosaic.sh src/scripts/portfolio/ma_lens_screen.py NUVAMA
./mosaic.sh src/scripts/portfolio/ma_lens_screen.py NUVAMA GODIGIT BECTORFOOD
./mosaic.sh src/scripts/portfolio/ma_lens_screen.py NUVAMA --no-chart --months 6
```

Options: `--days N` price window (default 420), `--months N` accumulation lookback
(default 4), `--no-chart`.

## Steps to execute

Run the script once with every symbol the user named, then narrate its output verbatim.
**Do not recompute any number in your head** - the script owns all arithmetic.

If the user asks "which one", read the SUMMARY table at the bottom: rank by gates passed,
then break ties with the house signal (`DSP ADDING` > `DSP HOLDING` > `NO DSP POSITION` >
`DSP DISTRIBUTING`).

## The five gates

Thresholds are **derived live** from `DSP_MULTI_ASSET`'s holdings, not hardcoded, and are
compared against the sleeve matching the symbol's own AMFI cap category.

| Gate | Test | Where the threshold comes from |
|------|------|-------------------------------|
| G1 | Median daily turnover >= floor | A template-sized position must exit in <= 3 median days |
| G2 | AMFI rank <= deepest rank owned | The deepest rank the fund actually holds in that sleeve |
| G3 | >=3 funds, >=3 AMCs, top AMC <60% | Independent corroboration, not one house's thesis |
| G4 | Trailing PE <= 50 | Missing data = FAIL (unscoreable), never a remembered number |
| G5 | Drawdown from 52w high <= -10% | Buying a correction, not chasing a high |

Verdicts: **BUY** (5/5 and DSP adding) - **STARTER** (5/5, DSP not adding; size at floor,
by elimination not endorsement) - **WATCH** (4/5) - **REJECT** (<=3/5) -
**OUT OF SCOPE** (no ISIN or no AMFI cap classification).

`OUT OF SCOPE` is distinct from `REJECT` on purpose: REJECT asserts the name was evaluated
and failed, whereas an unresolved symbol could not be placed in a sleeve or
ownership-checked at all. In that case the house signal reads `NOT CHECKED`, never
`NO DSP POSITION` - reporting an absence of holders from a failed lookup would invent a
fact. ETFs land here (GOLDBEES does); send those to `/goldbees-pipeline`.

## Why the pipeline does what it does

Each step exists because the naive version produces a specific wrong answer:

- **Split back-adjustment** - an unadjusted 5:1 split reads as a fake -80% drawdown.
  BECTORFOOD showed -84.7% raw vs -26.1% adjusted.
- **Per-fund latest filing** - AMC calendars are ragged. A single global
  `max(as_of_month)` collapses to the earliest-filing AMC: GODIGIT showed 1 fund /
  Rs 35.8 Cr instead of 17 funds / Rs 1,493.9 Cr.
- **Price-neutral accumulation** - a rising market value is usually just price. DSP's
  BECTORFOOD value rose 32% over four months while the implied share count was *exactly*
  flat. Value change is not position change.
- **AMC normalisation + fund dedup** - "4 AMCs" meant nothing for NUVAMA when 90% of the
  money is Invesco, and `..._MIDCAP_FUND` vs `..._MID_CAP_FUND` double-counted one holder.
- **ISIN keying** - `security_name` has spelling drift (GODIGIT files under three
  variants) and a loose name match pulls in bonds of affiliated NBFCs that
  `asset_type='equity'` misclassifies.

## Charts

Every symbol renders a 12-month split-adjusted plotext chart: cyan close line, **red**
horizontal 52-week high, **orange** horizontal SMA200. Pass `--no-chart` to suppress.

Read the **SMA200 relationship, not the drawdown**, to tell a correction from a trend -
ranked on drawdown alone the worst chart often looks the most attractive. A worked example:

| Symbol | DD 52wHi | vs SMA200 | Shape |
|---|---|---|---|
| NUVAMA | -14.8% | **+10.8%** | uptrend + pullback |
| BECTORFOOD | -26.1% | +2.3% | base, fading bounce |
| GODIGIT | **-31.6%** | **-18.9%** | sustained downtrend |

GODIGIT has the deepest drawdown and is the only one actually breaking down.

The chart x-axis carries its own `as-of` date, and a `!! STALE` / `split-adjusted xN`
stamp prints on the line directly above the chart, so charts shown side by side cannot be
mistaken for same-dated. (The stamp is deliberately not in `plt.title()` - plotext
silently drops a title that exceeds the plot width.)

## Warnings the script emits - surface all of them

- `STALE PRICE` - latest bar more than 5 days old; all price figures are as-of that date
- `RAGGED DISCLOSURE` - filing months differ, so the ownership total is not single-date
- `CONCENTRATED OWNERSHIP` - one AMC is >=60% of institutional money
- `duplicate fund_name spelling(s) collapsed`
- `unadjusted split/bonus gap(s) detected and back-adjusted`
- `[UNRESOLVED_SECURITY: ...]` - a lookup gap, **not** an absence of holders

## Known limitations - state them when relevant

- `daily_prices` covers ~61 stocks. Anything else returns no price data; import first.
- `amfi_market_cap` has an `nse_symbol` for only ~2,041 of 5,177 small caps, and
  `security_symbol_map` has 29 rows, so resolution often fails.
- `stock_valuation` has ROE / D-E / beta NULL for most names - G4 uses PE/PB only.
- `pct_of_nav` in `mf_holdings` is corrupt for some rows; the script relies on
  `market_value_cr` and implied share count instead.
- **Kotak rows are cross-assigned** in `mf_holdings`: for RELIANCE @ 2026-10-01,
  `KOTAK_BALANCED_ADVANTAGE` and `KOTAK_EQUITY_OPPORTUNITIES` carry byte-identical values
  (1400.8052 / 4.01957), as do `KOTAK_BLUECHIP` and `KOTAK_SMALL_CAP` (0.9324 / 3.52377).
  `fund_key()` cannot collapse these because the names are genuinely different funds, so
  G3 fund counts and institutional totals are inflated for large caps. Treat a large-cap
  G3 pass as directional, not exact.
- Holdings lag prices by roughly a month, so recent fund activity is invisible.
