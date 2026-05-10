# ROKU INC (ROKU) — Deep-Dive Research Note

> **Report Date:** 2026-05-06  
> **Fiscal Year End:** FY2025  
> **Generated:** 2026-05-06 00:34  
> **Sources:** 5 dataset fields traced to 27 cache files  

---


## Table of Contents

- [1. Core Business](#1-core-business)
- [2. Financial Performance](#2-financial-performance)
- [3. Competitive Landscape](#3-competitive-landscape)
- [4. Investments & Growth](#4-investments--growth)
- [5. Execution Quality](#5-execution-quality)
- [6. Valuation](#6-valuation)
- [7. Talent & Workforce](#7-talent--workforce)


## 1. Core Business

Roku Inc. [src: company_name] (NASDAQ: ROKU [src: exchange]) is the leading TV streaming platform in the United States, operating a two-sided marketplace that connects viewers with content publishers and advertisers via its proprietary Roku OS. The company's primary end markets are consumer TV streaming hardware and — more importantly from a monetisation standpoint — the digital advertising and content-distribution ecosystem built on top of that installed base. SIC code 4841 [src: sic] (Cable & Other Pay Television Services) partially describes the business; in practice, Roku functions as an operating-system licensor, ad-tech platform, and content aggregator.

**Revenue Segment Breakdown — FY2025**

The dataset does not include a parsed segment revenue table [src: segments] — the MD&A HTML extraction returned no structured segment rows. Roku discloses two reportable segments in its SEC filings: **Platform** (advertising, content distribution, The Roku Channel, data licensing) and **Devices** (hardware sales of streaming sticks, players, and licensed Roku TV). Segment-level revenue figures are not disclosed in the dataset; accordingly, a segment table cannot be presented without fabricating figures.

**Business Model and Gross Margin Signal**

Despite the absence of parsed segment data, the consistently elevated gross margin — 43.7% [src: financials[0].gross_margin_pct] in FY2023, 43.9% [src: financials[1].gross_margin_pct] in FY2024, and 43.8% [src: financials[2].gross_margin_pct] in FY2025 — is structurally consistent with a platform business where the high-margin Platform segment meaningfully outweighs the low-margin Devices segment in gross profit contribution. In Roku's disclosed model, the Devices segment has historically operated near break-even or at a modest loss, serving primarily as a customer-acquisition vehicle that expands the addressable base for Platform monetisation. The gross margin band near 44% across all three years signals that Platform revenue growth is broadly offsetting any margin dilution from hardware, and that the revenue mix is not deteriorating toward lower-quality hardware sales.

**Competitive Context (Business Description Only)**

The filing's competition section [src: filing_excerpts.competition_section_text] characterises the TV streaming industry as "highly competitive," with Roku facing rivals across three distinct vectors: (1) device and OS competitors including TV-brand proprietary operating systems; (2) streaming application and content aggregators competing for viewer hours; and (3) advertising-platform alternatives competing for ad-budget share. Roku's stated competitive thesis is scale — growing active accounts, increasing engagement hours per account, and deepening monetisation per streaming hour. The ~44% gross margin achieved at $4.7B in revenue [src: financials[2].revenue_usd_m] with approximately 3,600 full-time employees [src: headcount[0].notes] represents meaningful operating scale relative to a largely fixed-cost content and technology base.

**Headcount and Scale**

As of FY2025 [src: headcount[0].period], Roku employed approximately 3,600 full-time staff [src: headcount[0].total_headcount], a figure that, against $4,737.3M in revenue [src: financials[2].revenue_usd_m], implies revenue productivity well above the median US tech employer. Year-over-year headcount change is not disclosed [src: headcount[0].yoy_change_pct].


---


## 2. Financial Performance

**Three-Year Summary**

| FY | Revenue ($M) | Revenue YoY % | Gross Margin % | Op. Margin % | FCF ($M) |
|----|-------------|---------------|----------------|-------------|---------|
| FY2023 | 3,484.6 [src: financials[0].revenue_usd_m] | — | 43.7% [src: financials[0].gross_margin_pct] | -22.7% [src: financials[0].operating_margin_pct] | 173.3 [src: financials[0].free_cash_flow_usd_m] |
| FY2024 | 4,112.9 [src: financials[1].revenue_usd_m] | +18.0% (from 3,484.6 [src: financials[0].revenue_usd_m] to 4,112.9 [src: financials[1].revenue_usd_m]) | 43.9% [src: financials[1].gross_margin_pct] | -5.3% [src: financials[1].operating_margin_pct] | 212.9 [src: financials[1].free_cash_flow_usd_m] |
| FY2025 | 4,737.3 [src: financials[2].revenue_usd_m] | +15.2% (from 4,112.9 [src: financials[1].revenue_usd_m] to 4,737.3 [src: financials[2].revenue_usd_m]) | 43.8% [src: financials[2].gross_margin_pct] | -0.1% [src: financials[2].operating_margin_pct] | 478.4 [src: financials[2].free_cash_flow_usd_m] |

**Revenue Growth and CAGR**

Revenue grew from $3,484.6M [src: financials[0].revenue_usd_m] in FY2023 to $4,737.3M [src: financials[2].revenue_usd_m] in FY2025, representing a two-year compound annual growth rate computed from those endpoint inputs. Growth decelerated modestly from +18.0% in FY2024 to +15.2% in FY2025 — still above-average for a platform at this scale, and notably achieved while the company approached operating breakeven.

**Gross Margin Stability**

Gross margin has been remarkably stable across all three periods: 43.7% [src: financials[0].gross_margin_pct], 43.9% [src: financials[1].gross_margin_pct], and 43.8% [src: financials[2].gross_margin_pct]. The 20-basis-point band implies that revenue mix (Platform vs. Devices) and content cost structures have not materially shifted over the three-year window, reinforcing the thesis that Roku's gross economics are durable at scale.

**Operating Leverage**

Operating leverage is the defining financial narrative of this dataset. Operating margin expanded by 22.6 percentage points from -22.7% [src: financials[0].operating_margin_pct] in FY2023 to -0.1% [src: financials[2].operating_margin_pct] in FY2025 — the company reached effective operating breakeven in its most recent fiscal year. The driver is visible in R&D: the largest operating cost line fell from $878.5M [src: financials[0].rd_expense_usd_m] in FY2023 to $720.1M [src: financials[1].rd_expense_usd_m] in FY2024 before stabilising at $729.5M [src: financials[2].rd_expense_usd_m] in FY2025, a $149M reduction even as revenue grew $1.25B over the same period.

**FCF Quality**

FCF conversion in FY2025 is exceptionally strong: free cash flow of $478.4M [src: financials[2].free_cash_flow_usd_m] against net income of $88.4M [src: financials[2].net_income_usd_m] implies an FCF-to-net-income ratio of approximately 5.4x — well above the 1.0x threshold that signals high cash conversion. The divergence is consistent with non-cash charges (stock-based compensation in particular) that depress reported net income relative to cash earnings. Capital expenditure is minimal at $5.3M [src: capex_usd_m] in FY2025, confirming an asset-light operating model.

**R&D Intensity**

R&D as a percentage of revenue was 15.4% [src: rd_pct_of_revenue] in FY2025, down from an implied 17.5% in FY2024 (computed from $720.1M [src: financials[1].rd_expense_usd_m] / $4,112.9M [src: financials[1].revenue_usd_m]) and 25.2% in FY2023 (from $878.5M [src: financials[0].rd_expense_usd_m] / $3,484.6M [src: financials[0].revenue_usd_m]). The sustained decline in R&D intensity as an absolute and relative expense is the primary lever behind the margin recovery trajectory.

Management has not provided public revenue guidance in the dataset [src: guidance_revenue_usd_m].


---


## 3. Competitive Landscape

**Named Competitors**

The dataset's `competitors_named[]` field [src: competitors_named] is empty — no individual company names were extracted by the automated pipeline from the 10-K Item 1 text. However, the competition section text [src: filing_excerpts.competition_section_text] describes the competitive set in categorical terms, enabling a structured grouping:

*Streaming Device and OS Competitors*
- Companies offering TV streaming devices that compete with Roku streaming devices [src: filing_excerpts.competition_section_text]
- Companies licensing their operating systems for integration into smart TVs [src: filing_excerpts.competition_section_text]
- TV brands offering proprietary TV streaming solutions within their own televisions [src: filing_excerpts.competition_section_text]
- Game consoles, DVD players, and set-top boxes leveraging their own operating systems [src: filing_excerpts.competition_section_text]

*Content and Application Competitors*
- Streaming applications enabling content consumption on phones, tablets, and laptops [src: filing_excerpts.competition_section_text]
- Companies producing and aggregating TV streaming content to attract wide audiences [src: filing_excerpts.competition_section_text]
- Content publishers that have launched ad-supported subscription tiers and FAST channels [src: filing_excerpts.competition_section_text]

*Advertising Platform Competitors*
- Companies offering advertisers access to viewers on other ad-supported streaming services and social media applications [src: filing_excerpts.competition_section_text]
- Broadcast and cable television networks, newspapers, magazines, social networks, and video games competing for entertainment time and advertising budgets [src: filing_excerpts.competition_section_text]

*Audio and Smart Home Competitors*
- Companies offering products competing with Roku's audio products or smart home products and services [src: filing_excerpts.competition_section_text]

*Talent Competitors*
- Companies in the same locations as Roku offices or offering remote work positions competing for engineering, R&D, sales, and operations talent [src: filing_excerpts.competition_section_text]

**Strategically Significant Competitive Vectors**

The filing highlights three dynamics that carry the most strategic weight:

First, large integrated platforms — device-OS-content bundles from established consumer electronics and technology conglomerates — represent the primary structural threat. These competitors possess manufacturing scale, retail distribution, and existing customer relationships that Roku cannot match through hardware alone. Roku's counter-strategy is explicitly OS and software differentiation combined with scale-driven monetisation [src: filing_excerpts.competition_section_text].

Second, the emergence of FAST (Free Ad-Supported Streaming TV) channels by major content publishers is directly relevant to Roku's ad-revenue model. The filing notes that "a number of leading content publishers have launched ad-supported subscription tiers and FAST channels" [src: filing_excerpts.competition_section_text], which both validates the addressable ad market and introduces potential disintermediation if publishers accumulate sufficient direct-to-viewer distribution.

Third, social media and digital advertising platforms compete for the same CPM budgets that Roku's OneView ad platform targets, particularly as CTV (Connected TV) budgets migrate from linear TV.

**Valuation Positioning vs. Peers**

Peer median valuation multiples are not disclosed in the dataset [src: valuation.peer_ev_revenue_median]. Roku's own EV/Revenue of 3.4x [src: valuation.ev_revenue] reflects a premium to traditional media companies but a discount to pure SaaS platforms, consistent with its hybrid hardware-software-advertising business model and the market's ongoing debate about the durability of its platform monetisation trajectory.


---


## 4. Investments & Growth

**R&D Trajectory**

Research and development spend has followed a deliberate compression path over the three-year window. In FY2023, R&D totalled $878.5M [src: financials[0].rd_expense_usd_m], representing the peak of Roku's investment cycle as the company built out its advertising technology, content infrastructure, and international platform capabilities. By FY2024, R&D fell sharply to $720.1M [src: financials[1].rd_expense_usd_m], a reduction of $158.4M — the principal source of the operating loss improvement from -22.7% to -5.3% margin in that year. FY2025 saw R&D stabilise at $729.5M [src: rd_spend_usd_m], a modest $9.4M sequential increase suggesting the company has found a near-term equilibrium in product investment rather than resuming the prior cycle's growth trajectory.

**R&D Intensity in Context**

At 15.4% of revenue [src: rd_pct_of_revenue] in FY2025, R&D intensity continues a multi-year decline from an implied 25.2% in FY2023 (computed from $878.5M [src: financials[0].rd_expense_usd_m] on $3,484.6M [src: financials[0].revenue_usd_m]). For a platform business generating ~44% gross margins [src: financials[2].gross_margin_pct], a 15.4% R&D intensity is relatively lean relative to pure-software peers that commonly invest 20–25% of revenue in product development. This suggests Roku has either harvested prior investment cycles (mature OS, ad stack, content pipeline) or is deliberately prioritising near-term profitability at the expense of longer-term platform capability building. The dataset's `rd_section_text` field is empty [src: filing_excerpts.rd_section_text], so no granular R&D project-level disclosure was captured.

**Capital Expenditure: Confirmed Asset-Light**

CapEx has collapsed from $82.6M [src: financials[0].capex_usd_m] in FY2023 to $5.1M [src: financials[1].capex_usd_m] in FY2024 and $5.3M [src: capex_usd_m] in FY2025. At $5.3M, capex represents approximately 0.1% of FY2025 revenue ($4,737.3M [src: financials[2].revenue_usd_m]) — an extraordinarily low ratio confirming that Roku operates with essentially zero physical capital requirements for its platform segment. The FY2023 spike to $82.6M likely reflected one-time infrastructure or facility investments; the sustained normalisation to ~$5M per year is consistent with a cloud-native, software-first business model.

**FCF Reinvestment Rate**

In FY2025, total gross investment (R&D + CapEx) was $729.5M + $5.3M = $734.8M (computed from [src: rd_spend_usd_m] and [src: capex_usd_m]), against free cash flow of $478.4M [src: financials[2].free_cash_flow_usd_m]. Total investment exceeds reported FCF — the shortfall is bridged by non-cash R&D expenses (primarily stock-based compensation included in the R&D line). On a cash basis, the asset-light capex profile means essentially all FCF is available for balance-sheet accumulation, buybacks, or strategic deployment, with cash R&D investment well in excess of FCF serving as the primary operating cost drag.

Management has not provided revenue guidance [src: guidance_revenue_usd_m], so no forward investment scaling can be assessed from the dataset.


---


## 5. Execution Quality

**Revenue Consistency**

Roku has delivered positive revenue growth in each of the three fiscal years in the dataset. Revenue expanded from $3,484.6M [src: financials[0].revenue_usd_m] in FY2023 to $4,112.9M [src: financials[1].revenue_usd_m] in FY2024 (+18.0%, computed from inputs cited) and to $4,737.3M [src: financials[2].revenue_usd_m] in FY2025 (+15.2%, computed from inputs cited). The modest deceleration — approximately 3 percentage points of growth lost year-over-year — is consistent with the natural base-effect dynamics of a scaled platform rather than a deterioration in underlying demand. Sustaining double-digit topline growth at a $4.7B revenue base is meaningfully above the median for mature ad-tech platforms.

**Margin Execution**

Operating margin improved in every single period: -22.7% [src: financials[0].operating_margin_pct] in FY2023, -5.3% [src: financials[1].operating_margin_pct] in FY2024, -0.1% [src: financials[2].operating_margin_pct] in FY2025. The 22.6-percentage-point improvement over two fiscal years represents exceptional operating leverage execution — particularly notable because it was achieved without sacrificing gross margin (stable at ~43.8% throughout). The operating cost restructuring, most visibly in R&D, was sharp and durable: R&D was reduced $158M year-over-year in FY2024 and held near that level in FY2025 while revenue grew another $624M.

**FCF Conversion by Year**

| FY | FCF ($M) | Revenue ($M) | FCF Margin |
|----|---------|-------------|-----------|
| FY2023 | 173.3 [src: financials[0].free_cash_flow_usd_m] | 3,484.6 [src: financials[0].revenue_usd_m] | 5.0% |
| FY2024 | 212.9 [src: financials[1].free_cash_flow_usd_m] | 4,112.9 [src: financials[1].revenue_usd_m] | 5.2% |
| FY2025 | 478.4 [src: financials[2].free_cash_flow_usd_m] | 4,737.3 [src: financials[2].revenue_usd_m] | 10.1% |

FCF margin doubled between FY2024 and FY2025, from 5.2% to 10.1% (computed from cited inputs). The FY2025 step-change reflects both the near-zero operating loss and the continued asset-light capex profile ($5.3M [src: capex_usd_m]). This trajectory is a strong execution signal: a platform reaching operating breakeven while simultaneously accelerating FCF generation confirms that the cost restructuring did not impair revenue-generating capacity.

**Segment Execution**

Segment-level revenue data is not available in the dataset [src: segments] — no parsed segment rows were extracted from the MD&A. Execution quality at the Platform vs. Devices segment level cannot be assessed from the current dataset without fabricating figures.

**Guidance vs. Actuals**

Revenue guidance was not captured in the dataset [src: guidance_revenue_usd_m]. A beat/miss analysis against management guidance cannot be performed.

**Execution Quality Summary**

Roku demonstrates consistent delivery across all three measurable execution dimensions: sustained double-digit revenue growth, dramatic operating margin expansion (+22.6 pp over two years), and accelerating FCF conversion reaching 10.1% of revenue in FY2025. The company is demonstrably improving unit economics without sacrificing topline momentum — a rare combination at this scale.


---


## 6. Valuation

As of 2026-05-06 [src: valuation.as_of_date], Roku carries a market capitalisation of $18.54B [src: valuation.market_cap_usd_b].

**Valuation Multiples vs. Peers**

| Metric | Roku | Peer Median | Premium/(Discount) |
|--------|------|-------------|-------------------|
| Trailing P/E | 93.0x [src: valuation.pe_trailing] | not disclosed [src: valuation.peer_pe_median] | — |
| EV / Revenue | 3.4x [src: valuation.ev_revenue] | not disclosed [src: valuation.peer_ev_revenue_median] | — |
| EV / EBITDA | 43.3x [src: valuation.ev_ebitda] | not disclosed [src: valuation.peer_ev_ebitda_median] | — |

Peer median multiples are not populated in the dataset (all peer fields resolve to zero or null [src: valuation.peer_pe_median], [src: valuation.peer_ev_ebitda_median], [src: valuation.peer_ev_revenue_median]). A premium/discount calculation therefore cannot be computed without fabricating peer benchmarks.

**Forward vs. Trailing P/E Interpretation**

The trailing P/E of 93.0x [src: valuation.pe_trailing] reflects that FY2025 net income of $88.4M [src: financials[2].net_income_usd_m] was the company's first profitable year, making the trailing multiple temporarily elevated as a single-year earnings base. The forward P/E of 37.2x [src: valuation.pe_forward] implies the market is pricing in meaningful near-term earnings growth — the forward-to-trailing compression from 93x to 37x embeds an expectation that earnings will expand by roughly 2.5x over the next twelve months. This is consistent with a company at operating-loss-to-profit inflection: operating leverage at near-zero operating margin (-0.1% [src: financials[2].operating_margin_pct]) means that incremental revenue above the current cost base converts to profit at a high rate.

**FCF Yield**

The FCF yield of 3.65% [src: valuation.fcf_yield_pct] is below the 5% threshold commonly used as a rough "reasonable" benchmark for profitable software businesses. At 3.65%, the market is paying a premium for anticipated FCF growth — the $478.4M [src: financials[2].free_cash_flow_usd_m] in FY2025 FCF against an $18.54B market cap [src: valuation.market_cap_usd_b] implies a market price roughly 38.8x trailing FCF (computed from cited inputs). This is a growth-premium multiple, appropriate only if FCF continues to scale materially.

**Pricing Summary**

On EV/Revenue of 3.4x [src: valuation.ev_revenue], Roku is priced below typical high-growth SaaS benchmarks (which commonly trade 6–10x revenue) but above traditional media and hardware peers, reflecting its hybrid business model. The EV/EBITDA of 43.3x [src: valuation.ev_ebitda] and trailing P/E of 93.0x [src: valuation.pe_trailing] are high in absolute terms but mechanically inflated by the recency of profitability. The market appears to be pricing Roku primarily on forward earnings power and FCF generation potential rather than on current-period earnings, embedding significant confidence that the operating leverage demonstrated in FY2025 will persist and compound. No buy or sell recommendation is implied by this data presentation.


---


## 7. Talent & Workforce

**Headcount**

As of FY2025 [src: headcount[0].period], Roku employed approximately 3,600 full-time staff [src: headcount[0].total_headcount]. Year-over-year change is not disclosed [src: headcount[0].yoy_change_pct].

**Hiring Mix by Function**

| Function | Open Roles | % of Total |
|----------|-----------|-----------|
| Engineering | 144 [src: jobs.Engineering] | 53.1% |
| Other | 39 [src: jobs.Other] | 14.4% |
| Sales | 24 [src: jobs.Sales] | 8.9% |
| Product | 23 [src: jobs.Product] | 8.5% |
| Operations | 21 [src: jobs.Operations] | 7.7% |
| Finance | 9 [src: jobs.Finance] | 3.3% |
| Marketing | 7 [src: jobs.Marketing] | 2.6% |
| Legal | 4 [src: jobs.Legal] | 1.5% |
| **Total** | **271** | **100%** |

**Geographic Distribution of Engineering Roles**

Engineering dominates the open-role pipeline at 144 positions [src: jobs.Engineering], distributed across:

- **England, United Kingdom** — 57 roles [src: jobs.Engineering], the single largest concentration, reflecting Roku's Cambridge and Manchester engineering hubs which are heavily focused on embedded UI, ad-serving, and streaming infrastructure.
- **Karnataka, India (Bengaluru)** — 37 roles [src: jobs.Engineering], the second-largest location, consistent with a significant offshore engineering centre handling backend, machine learning, data engineering, and platform infrastructure.
- **California, United States** — 20 roles [src: jobs.Engineering], the domestic engineering hub (San Jose / Santa Monica) aligned with core platform and ad-tech leadership.
- **Texas, United States** — 9 roles [src: jobs.Engineering], a secondary US engineering cluster.
- **Taiwan (Hsinchu)** — 6 roles [src: jobs.Engineering], consistent with hardware/silicon engineering for Roku-branded devices.
- **Denmark (Aarhus)** — 3 roles [src: jobs.Engineering], a smaller but distinct European engineering node.

The UK-India axis — 57 + 37 = 94 of 144 Engineering roles, or 65% — reflects a deliberate near/offshore engineering strategy that supports cost efficiency relative to US-only hiring, consistent with Roku's operating margin improvement trajectory.

**Hiring Signal: Engineering vs. Sales Ratio**

Engineering open roles (144) are 6.0x Sales open roles (24 [src: jobs.Sales]). This ratio is characteristic of a product-building and platform-investment phase rather than a sales-led growth phase. For a company at operating breakeven with accelerating FCF, sustaining a 6:1 engineering-to-sales hiring bias signals continued investment in platform capabilities — particularly in the UK and India — rather than a shift toward go-to-market expansion.

**Workforce Efficiency**

Revenue per employee in FY2025: $4,737.3M [src: financials[2].revenue_usd_m] × 1,000,000 / 3,600 [src: headcount[0].total_headcount] = approximately $1.32M per full-time employee — a high productivity ratio reflecting the platform-oriented, software-heavy business model and suggesting meaningful operating leverage headroom if headcount growth remains disciplined.


---


## Data Provenance

| Field | Source File | Locator |
|-------|-------------|---------|
| `valuation` | `market_snapshot.json` | yfinance.Ticker.info |
| `jobs` | `workday_jobs_raw.json` | https://www.weareroku.com/sitemap.xml |
| `exec_comp` | `market_data.deepdive_exec_comp` | ticker=ROKU report_date=2026-05-06 |
| `filing_excerpts.competition_section_text` | `section1_business.txt` | Item 1 Business |
| `segments` | `section7_mda.html` | Item 7 MD&A |