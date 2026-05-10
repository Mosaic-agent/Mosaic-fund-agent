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
