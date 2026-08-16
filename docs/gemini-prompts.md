# Gemini CLI Prompts for Mosaic Fund Agent

20 ready-to-use prompts covering every major capability of the platform.

**How to use:**

```bash
# One-shot mode
gemini -p "paste any prompt below"

# Interactive session
gemini
> paste any prompt below
```

> All prompts reference real CLI commands, ClickHouse tables, and ETF symbols from the codebase. No API keys required for macro/news prompts (Google News RSS + Yahoo Finance).

---

## A. Daily Workflow

### 1. Morning Pre-Market Briefing

Run before Indian market open (9:15 AM IST) to get overnight COMEX signals and macro context.

```
Run the COMEX pre-market command and the macro scanner, then synthesize a morning briefing:

1. Run: python src/main.py comex
2. Run: python src/main.py macro --max 3

From the combined output, tell me:
- Which commodities moved overnight and what signal they carry (STRONG BULLISH / BULLISH / NEUTRAL / BEARISH / STRONG BEARISH)
- Which of the 8 macro themes have active events (Geopolitical, Central Bank, Currency, Crude, Trade War, India Macro, Commodity, Risk-Off)
- Which ETFs from the 18-ETF universe are most affected this morning
- A concrete 1-2 sentence "what to watch today" summary
```

### 2. Why Did the Market Fall Today? — Post-Mortem

Run after a broad equity decline (NIFTYBEES or BANKBEES down > 0.5%) to attribute the cause.

```
Run the 5-step market-fall post-mortem for today.

Step 1 — Institutional flow check (who sold?):

  SELECT trade_date, fii_net_cr, dii_net_cr, (fii_net_cr + dii_net_cr) AS combined_net_cr
  FROM market_data.fii_dii_flows FINAL
  ORDER BY trade_date DESC LIMIT 5;

  Interpret:
  - fii_net_cr < -2000  → large institutional outflow (FII-driven sell-off)
  - dii_net_cr > 0 while fii_net_cr < 0 → domestic funds absorbing the blow (cushion)
  - Both negative → panic selling across participant types

Step 2 — Macro theme scan (what event triggered it?):

  python src/main.py macro --max 4

  Look for active bearish themes: Global Risk-Off, Fed/RBI Policy, Crude Oil Shock,
  Trade War/Tariffs, Geopolitical. Net macro score ≤ -8 = macro-driven decline.
  Report: which themes are active and their net ETF score from the output.

Step 3 — ETF price damage map (what fell most and by how much?):

  SELECT
      d1.symbol,
      round((d1.close - d2.close) / d2.close * 100, 2) AS day_return_pct,
      d1.close AS today_close,
      d2.close AS prev_close
  FROM (
      SELECT symbol, argMax(close, imported_at) AS close
      FROM market_data.daily_prices FINAL
      WHERE symbol IN ('NIFTYBEES','BANKBEES','ITBEES','GOLDBEES','SILVERBEES',
                       'MIDSELECT','CPSEETF','MON100','HNGSNGBEES','MAFANG')
        AND trade_date = today()
      GROUP BY symbol
  ) d1
  JOIN (
      SELECT symbol, argMax(close, imported_at) AS close
      FROM market_data.daily_prices FINAL
      WHERE symbol IN ('NIFTYBEES','BANKBEES','ITBEES','GOLDBEES','SILVERBEES',
                       'MIDSELECT','CPSEETF','MON100','HNGSNGBEES','MAFANG')
        AND trade_date = (
            SELECT max(trade_date) FROM market_data.daily_prices FINAL
            WHERE trade_date < today()
        )
      GROUP BY symbol
  ) d2 ON d1.symbol = d2.symbol
  ORDER BY day_return_pct ASC;

  Safe-haven read:
  - GOLDBEES UP + equities DOWN → global risk-off (fear bid) — macro or geopolitical shock
  - GOLDBEES DOWN + equities DOWN → broad liquidity squeeze or margin-call selling
  - GOLDBEES FLAT + equities DOWN → domestic event (RBI, budget, election, earnings miss)

Step 4 — News sentiment (what is the headline driver?):

  python src/main.py etf-news --max 4 --save

  Look for NEGATIVE-tagged articles. If the top 3 share a keyword (e.g., RBI, crude,
  tariff, earnings miss, INR) → that keyword IS the headline driver. Quote those
  headlines verbatim from the output; do not paraphrase or invent.

Step 5 — USDINR pressure (currency-linked selling?):

  SELECT trade_date, argMax(close, imported_at) AS usdinr
  FROM market_data.daily_prices FINAL
  WHERE symbol = 'USDINR=X' AND trade_date >= today() - 5
  GROUP BY trade_date
  ORDER BY trade_date DESC LIMIT 5;

  INR weakening > 0.3% in 1 day → FII USD mark-to-market loss → forced selling.
  Also triggers imported-inflation fears → bearish for equities, bullish for GOLDBEES.

---
From all five outputs, write the post-mortem covering exactly these points:

**Primary cause**: one sentence naming the trigger (FII outflow / macro theme / INR shock /
  earnings / liquidity), using only numbers visible in the output above.

**Confirming signals**: which of the other 4 steps corroborate the primary cause.

**Contradicting signals**: any signal that does NOT fit the narrative — state it explicitly
  rather than omitting it.

**Safe-haven verdict**: GOLDBEES direction + what it reveals about the fall's nature
  (global fear vs domestic event vs liquidity crunch).

**DII cushion**: was domestic money absorbing the FII selling, absent, or also selling?

**Actionable takeaway**: one sentence — hold, add on weakness, or reduce exposure —
  grounded in signal data only. Never use training knowledge for market levels.
```

### 3. End-of-Day Review

Run after 3:30 PM IST market close to capture the full day's picture.

```
Run the full signal aggregator and ETF news scanner to produce an end-of-day review:

1. Run: python src/main.py signals --save --verbose
2. Run: python src/main.py etf-news --max 3 --save

From the output, produce a summary covering:
- The top 3 ETFs by composite score (0-100) and their action (BUY/ACCUMULATE/HOLD/TRIM/AVOID)
- The bottom 3 ETFs and why they scored low
- Any ETF whose signal changed direction today vs the general trend
- Key news headlines that drove sentiment shifts
- One concrete action item for tomorrow's session
```

### 4. Weekend Data Maintenance

Saturday/Sunday data pipeline refresh and validation.

```
Run the weekend data maintenance cycle for the Mosaic Fund Agent. Execute these in order and report any errors:

1. Delta-sync all market data:
   python src/main.py import --category etfs,stocks,commodities,indices,fx_rates
2. Update FII/DII institutional flows:
   python src/main.py import --category fii_dii
3. If today is Saturday, update COT positioning (CFTC publishes Friday evening):
   python src/main.py import --category cot
4. Refresh MF NAV data:
   python src/main.py import --category mf
5. Run the LightGBM trend predictor:
   python src/ml/trend_predictor.py

After each step, confirm success or flag any failures. At the end, summarize: total rows imported, any watermark gaps, and whether the ML model trained successfully (check predictions_log.jsonl for the latest entry).
```

---

## B. Signal Analysis & Trading

### 5. Signal Composite Drill-Down

Deep-dive into why a specific ETF has its current composite score.

```
Run the signal aggregator with verbose output:
  python src/main.py signals --save --verbose

Then pick the ETF with the highest composite score and the one with the lowest. For each, break down the 6 signal sources:
- Macro (25% weight): What themes drove it? Which direction?
- Sentiment (15%): What news sentiment score did it get?
- Valuation/iNAV (15%): Is it trading at premium or discount to NAV?
- FII/DII Flows (25%): Are institutions net buying or selling?
- ML LightGBM (15%): What is the predicted 5-day return and regime (BUY/WATCH_LONG/HOLD/WATCH_SHORT/SELL)?
- Anomaly (5%): Any flag (Flash Crash, Blow-off Top, Strong Trend, Volatile Breakout, Normal)?

Explain which pillar is the strongest contributor and which is dragging the score.
```

### 6. Gold vs Silver Rotation Decision

Decide whether to hold GOLDBEES or rotate into SILVERBEES.

```
I need to decide whether to hold GOLDBEES or rotate into SILVERBEES. Analyze:

1. Run the Quant Scorecard for GOLDBEES (see src/tools/quant_scorecard.py):
   - Macro pillar (30%): DXY level vs 100-110 range, US 10Y real yield delta
   - Flows pillar (30%): COT mm_net / open_interest (crowded if > 35%)
   - Valuation pillar (20%): GOLDBEES iNAV premium/discount
   - Momentum pillar (20%): LightGBM expected_return_pct

2. Query the Gold-Silver Ratio from ClickHouse:
   SELECT trade_date, argMax(close, imported_at) as close
   FROM market_data.daily_prices
   WHERE symbol IN ('GOLD', 'SILVER')
     AND trade_date >= today() - 90
   GROUP BY symbol, trade_date
   ORDER BY trade_date;

3. Check the Who Is Selling regime (src/tools/who_is_selling_agent.py):
   Is it RETAIL_PANIC, INSTITUTIONAL_EXIT, OVERLEVERED_LONGS, CB_ACCUMULATION, or NEUTRAL?

Based on all three inputs, recommend: stay in GOLDBEES, rotate to SILVERBEES, or split allocation.
```

### 7. International ETF Scarcity Premium Entry

Check if MAFANG, HNGSNGBEES, or MON100 are at attractive entry points due to the RBI $7B overseas cap.

```
Run the premium alerts scanner for all international ETFs:
  python src/main.py premium-alerts --lookback 30 --z-threshold -1.5

Then run it again with a tighter threshold for "good entry" signals:
  python src/main.py premium-alerts --lookback 14 --z-threshold -1.0

For any ETF showing SCREAMING BUY or GOOD ENTRY:
- Show the current premium vs the 30-day mean premium
- Explain the RBI $7B cap mechanism and why a Z-score dip below -1.5 suggests a snap-back
- Check if there are macro headwinds: python src/main.py macro --max 2
- Give a concrete recommendation: buy now, wait for confirmation, or skip
```

---

## C. News & Macro Monitoring

### 8. Sector-Specific News Deep Dive

Focus news scanning on specific sectors when there's a developing story.

```
Run a targeted ETF news scan for banking and IT sectors:
  python src/main.py etf-news --category "Bank ETFs,IT ETFs" --max 6 --save

From the output:
- List every article with its sentiment (POSITIVE/NEGATIVE/NEUTRAL) and affected ETFs
- Calculate the net sentiment for BANKBEES vs ITBEES
- Identify any RBI policy or Fed rate news that affects both sectors differently
- If sentiments diverge (one positive, one negative), explain the pair trade opportunity
- Cross-reference with FII/DII flows: query the last 5 days from market_data.fii_dii_flows to see if institutions are rotating between sectors
```

### 9. Macro Theme Impact Matrix

When a macro event breaks, trace its impact across the full ETF universe.

```
Run the macro scanner with extended article coverage:
  python src/main.py macro --max 6 --save

From the output, build an impact matrix:
- For each active macro theme (out of 8: Geopolitical/War, Central Bank Policy, Currency/INR, Crude Oil, Trade War/Tariffs, India Macro, Commodity Gold/Silver, Global Risk-Off):
  - List the transmission mechanism (e.g., "Crude oil shock -> inflation -> RBI rate hike -> broad market bearish")
  - Show affected ETFs and their expected direction (bullish/bearish/neutral)
  - Show the conviction level (HIGH/MEDIUM/LOW)
- Calculate the net_signal per ETF across ALL active themes
- Identify the 3 ETFs with the strongest bullish net signal and the 3 with the strongest bearish
```

### 10. Single Stock News Sentiment

Before adding a new position, run deep news analysis on a specific symbol.

```
Run multi-source news sentiment for RELIANCE (replace with your target symbol):
  python src/main.py news RELIANCE --company "Reliance Industries"

From the output:
- Show the overall sentiment score and breakdown (positive/negative/neutral percentages)
- List the top 3 positive and top 3 negative headlines
- Note the source split: how many articles came from NewsAPI vs Google News?
- Compare this sentiment to the sector ETF: if RELIANCE sentiment is negative but NIFTYBEES signals are positive, explain the divergence
- Rate the news quality: are these substantive articles about earnings/policy, or generic market commentary?
```

---

## D. Data Management

### 11. ClickHouse Data Health Audit

Verify data completeness and freshness across all tables.

```
Run a data health audit across all ClickHouse tables in the market_data database. For each table, run:

  SELECT count() as rows, min(trade_date) as earliest, max(trade_date) as latest
  FROM market_data.<table_name> FINAL;

Check these tables: daily_prices (split by category: etfs, stocks, commodities, indices), mf_nav, inav_snapshots, cot_gold, cb_gold_reserves, etf_aum, fx_rates, fii_dii_flows, fii_dii_monthly, mf_holdings, ml_predictions, news_articles, signal_composite.

For each, report:
- Row count, earliest date, latest date
- Gap in days between latest date and today (flag if > 3 days for daily data, > 7 for weekly COT, > 35 for monthly reserves)

Then check the watermarks:
  SELECT source, symbol, last_date FROM market_data.import_watermarks FINAL ORDER BY last_date ASC LIMIT 20;

Flag the 5 most stale watermarks and suggest the import commands to refresh them.
```

### 12. Historical Backfill for a New ETF

When adding a new ETF to the tracking universe, walk through the full process.

```
I want to add a new ETF to the Mosaic Fund Agent tracking universe. Walk me through the full process:

1. Check if the symbol exists in src/data_importer/registry.py under the ETFS list and INAV_SYMBOLS list.
2. If not present, show me exactly what lines to add in registry.py.
3. Then run these imports in sequence:
   python src/main.py import --category etfs --full --lookback 3650
   python src/main.py import --category mf --full
   python src/main.py import --category inav
4. Verify the data landed:
   SELECT symbol, count() as rows, min(trade_date), max(trade_date)
   FROM market_data.daily_prices FINAL
   WHERE symbol = '<NEW_SYMBOL>'
   GROUP BY symbol;
5. Check minimum row requirements: anomaly detection needs >= 60 rows, ML needs >= 120.
6. Add the ETF to SIGNAL_ETFS in src/agents/signal_aggregator.py so it gets composite scores.
```

---

## E. Portfolio Analysis

### 13. Full Portfolio Health Check

Comprehensive portfolio analysis with sector concentration and risk assessment.

```
Run the full portfolio analysis:

1. Test run first: python src/main.py analyze --demo --max 3
2. Live run: python src/main.py analyze

From the output:
- What is the overall portfolio health score and diversification rating?
- Calculate sector concentration: is any single sector > 30% of the portfolio?
- Which holdings have the highest risk scores?
- Which ETF holdings are trading at a premium vs discount to iNAV?
- Show the COMEX linkage: which commodity signals affect which of my ETF holdings?
- List the top 3 actionable rebalancing signals
```

### 14. Portfolio Q&A Session

Interactive investigation using the ReAct agent loop.

```
Run these portfolio questions in sequence using the ask command:

1. python src/main.py ask "Which holdings have the worst news sentiment this week?"
2. python src/main.py ask "Am I overexposed to any single sector? Show percentage breakdown."
3. python src/main.py ask "Which of my ETFs are trading at a premium to their iNAV, and by how much?"
4. python src/main.py ask "Based on COMEX signals, should I increase or decrease my GOLDBEES allocation?"
5. python src/main.py ask "What is my total portfolio P&L and which holding is the biggest drag?"

After all 5 answers, synthesize the findings into a single "portfolio situation report" with the 3 most urgent action items.
```

---

## F. ML & Anomaly Detection

### 15. Multi-ETF Anomaly Scan

Run the 3-step composite anomaly pipeline to find unusual price action.

```
Run the composite anomaly detection on these key ETFs: GOLDBEES, NIFTYBEES, BANKBEES, SILVERBEES, ITBEES.

The pipeline (src/ml/anomaly.py) runs 3 steps:
- Step 1: Robust Z-Score (MAD) on daily returns and range
- Step 2: Random Forest residual Z-score (unexpected price moves)
- Step 3: Isolation Forest confidence multiplier

Report any days in the last 30 where final_z > 2.5, and classify the regime:
- Strong Trend: high z_robust, low z_resid — trend is predictable
- Flash Crash: low z_robust, high z_resid — unexpected shock
- Volatile Breakout: high z_robust, high z_resid — caution
- Blow-off Top: high z_robust, positive return, low volume — potential reversal
- Normal: nothing unusual

For any Flash Crash or Blow-off Top flags, cross-reference with news:
  python src/main.py etf-news --category "<relevant category>" --max 3
```

### 16. LightGBM Forecast Accuracy Review

Audit the ML model's prediction accuracy and feature importance.

```
Review the LightGBM trend predictor performance:

1. Query the latest predictions from ClickHouse:
   SELECT as_of, horizon_days, expected_return_pct, regime_signal, cv_r2_mean, n_training_rows
   FROM market_data.ml_predictions FINAL
   ORDER BY as_of DESC LIMIT 10;

2. Compare predicted vs actual returns:
   SELECT p.as_of, p.expected_return_pct, p.regime_signal,
          (d.close - p.goldbees_close) / p.goldbees_close * 100 as actual_return_pct
   FROM market_data.ml_predictions FINAL p
   JOIN market_data.daily_prices FINAL d ON d.symbol = 'GOLDBEES'
     AND d.trade_date = addDays(p.as_of, p.horizon_days)
   ORDER BY p.as_of DESC LIMIT 20;

3. Check predictions_log.jsonl for feature importance rankings.

Report: hit rate (predicted direction matched actual), average R-squared, top 3 features by importance, and whether the model needs more training data (< 120 rows is insufficient).
```

### 17. Fresh ML Prediction + Cross-Reference

Generate a new prediction and validate it against other signal sources.

```
Run a fresh LightGBM 5-day forecast for GOLDBEES:
  python src/ml/trend_predictor.py

From the output, explain:
- The predicted 5-day forward return (%) and confidence
- The regime signal: BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL
- The walk-forward CV R-squared (positive = model has skill, negative = worse than guessing)
- Number of training rows used

Then cross-reference with 3 other signal sources:
1. Who Is Selling regime (src/tools/who_is_selling_agent.py): retail panic, institutional exit, overlevered longs, CB accumulation, or neutral?
2. Quant Scorecard composite (src/tools/quant_scorecard.py): is the 0-100 score above or below 50?
3. COMEX signal for Gold: python src/main.py comex

Do all three agree with the ML prediction? If they diverge, explain which to trust more and why.
```

---

## G. Institutional Flow Analysis

### 18. FII/DII Flow Trend Analysis

Understand institutional buying/selling patterns and market implications.

```
Analyze recent FII/DII institutional flow data:

1. Query the last 20 trading days:
   SELECT trade_date, fii_net_cr, dii_net_cr
   FROM market_data.fii_dii_flows FINAL
   ORDER BY trade_date DESC LIMIT 20;

2. Query monthly aggregates for trend context:
   SELECT month_date, fii_net_cr, dii_net_cr, nifty_close
   FROM market_data.fii_dii_monthly FINAL
   ORDER BY month_date DESC LIMIT 6;

From the data:
- How many consecutive days has FII been a net seller or buyer?
- What is the 5-day rolling sum of FII net flows?
- Is DII absorbing FII selling (DII positive when FII negative)?
- Compare the flow pattern to NIFTYBEES and BANKBEES prices: are ETFs falling in line with FII selling or diverging?
- Based on the inst_net_momentum feature (used in the ML model), what does the current flow regime predict for the next 5 days?
```

### 19. Who Is Selling GOLDBEES — Full Attribution

When GOLDBEES drops, determine if it's retail panic, institutional exit, speculator unwinding, or central bank dynamics.

```
Run the Who Is Selling analysis for GOLDBEES. Check all 4 signal streams:

1. RETAIL PANIC (India-specific):
   - USDINR 60-day change from market_data.fx_rates (trigger: > +3%)
   - GOLDBEES discount to AMFI NAV from daily_prices + mf_nav (trigger: < -1%)

2. INSTITUTIONAL EXIT (Western hedge funds):
   - GLD shares outstanding 30-day rolling change (trigger: < -3%)

3. SPECULATOR OVER-LEVERAGE (COMEX futures):
   - COT Managed Money Net / Open Interest from market_data.cot_gold (crowded: > 25%)

4. CENTRAL BANK STRENGTH (China + Middle East):
   - USDCNY 30-day change from market_data.fx_rates (stable: < +1.5%)
   - WTI Crude Oil price (accumulation signal if > $80)

Report:
- Which signals are currently firing?
- Composite regime: RETAIL_PANIC / INSTITUTIONAL_EXIT / OVERLEVERED_LONGS / CB_ACCUMULATION / NEUTRAL / MIXED
- Plain-English recommendation
- Whether central bank buying is absorbing Western selling (historically bullish divergence)
```

---

## H. Ad-Hoc Investigation

### 20. Cross-Asset Correlation Check

Investigate whether traditional Gold-Nifty negative correlation still holds.

```
Run a cross-asset correlation check between GOLDBEES and NIFTYBEES:

1. Fetch the last 90 days of daily closes:
   SELECT symbol, trade_date, argMax(close, imported_at) as close
   FROM market_data.daily_prices
   WHERE symbol IN ('GOLDBEES', 'NIFTYBEES')
     AND trade_date >= today() - 90
   GROUP BY symbol, trade_date
   ORDER BY trade_date;

2. Calculate the 30-day rolling Pearson correlation of daily returns.

3. Interpret:
   - Correlation < 0: "True Hedge" — GOLDBEES is working as intended
   - Correlation 0 to 0.5: Partial diversification
   - Correlation > 0.5: "Diversification Failure" — both moving together (risk!)

4. Extend the analysis to SILVERBEES vs NIFTYBEES, and BANKBEES vs ITBEES.

5. If diversification has failed (correlation > 0.5), recommend which ETF from the universe (GILT5YBEES, LIQUIDBEES, HNGSNGBEES, MON100) to add for restoring negative correlation.
```

### 21. MF Holdings "Smart Money" Tracker

Check what institutional multi-asset funds (DSP, Quant, ICICI) are holding and detect allocation shifts.

```
Check the latest DSP Multi Asset holdings:

1. Query the most recent month's top holdings:
   SELECT fund_name, as_of_month, security_name, pct_of_nav, market_value_cr, asset_type
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
     AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                        WHERE fund_name = 'DSP_MULTI_ASSET')
   ORDER BY pct_of_nav DESC
   LIMIT 20;

2. Check asset class allocation breakdown:
   SELECT asset_type, sum(pct_of_nav) as total_pct, count() as num_securities
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
     AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                        WHERE fund_name = 'DSP_MULTI_ASSET')
   GROUP BY asset_type
   ORDER BY total_pct DESC;

3. Compare two months to detect allocation shifts:
   SELECT h1.security_name, h1.pct_of_nav as current_pct, h2.pct_of_nav as prev_pct,
          (h1.pct_of_nav - h2.pct_of_nav) as change_pct
   FROM market_data.mf_holdings FINAL h1
   LEFT JOIN market_data.mf_holdings FINAL h2
     ON h1.fund_name = h2.fund_name AND h1.isin = h2.isin
     AND h2.as_of_month = addMonths(h1.as_of_month, -1)
   WHERE h1.fund_name = 'DSP_MULTI_ASSET'
     AND h1.as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                           WHERE fund_name = 'DSP_MULTI_ASSET')
   ORDER BY abs(change_pct) DESC
   LIMIT 15;

4. From the holdings data:
   - What % is allocated to equity vs debt vs gold vs international?
   - Are any tracked ETFs (GOLDBEES, NIFTYBEES, BANKBEES) held directly?
   - Which positions grew/shrank the most month-over-month?
   - Is there a consensus "smart money" signal? (e.g., increased gold = bullish on commodities)

5. Cross-reference with signal aggregator: python src/main.py signals
   Do the fund managers' allocations agree with the composite scores?

NOTE: Columns are security_name, pct_of_nav, market_value_cr — NOT name/weight_pct.
```

---

## I. DSP Fund Analysis

### 22. DSP Fund Month-over-Month Return Analysis

Analyse NAV returns for any DSP fund over any lookback period.

```
Run the MoM return analyser for DSP Multi Asset Direct Growth (scheme 152056):

  python src/scripts/portfolio/fund_mom_returns.py --scheme 152056 --months 12

From the output, report:
- The NAV at the start and end of the period
- Month-by-month return with direction (positive/negative)
- Best and worst calendar months with the return %
- Average MoM return and count of positive vs negative months
- Overall period return %

Then compare with DSP Multi Asset Omni FoF Direct Growth (scheme 154167):
  python src/scripts/portfolio/fund_mom_returns.py --scheme 154167 --months 12

Which fund delivered better risk-adjusted returns over the period?
Note: DSP Multi Asset Omni FoF launched in late 2025 — limited history is expected.
```

### 23. DSP Holdings X-Ray — What's Inside the Fund

Deep-dive into a DSP fund's portfolio composition across multiple months.

```
Run a holdings X-ray on DSP Multi Asset (fund_name = 'DSP_MULTI_ASSET'):

1. Get available history (months with data):
   SELECT DISTINCT as_of_month, count() as num_holdings
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
   GROUP BY as_of_month
   ORDER BY as_of_month DESC;

2. Top 10 holdings in the latest month:
   SELECT security_name, isin, asset_type, pct_of_nav, market_value_cr
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
     AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                        WHERE fund_name = 'DSP_MULTI_ASSET')
   ORDER BY pct_of_nav DESC
   LIMIT 10;

3. Asset class breakdown (equity / debt / gold / international / cash):
   SELECT asset_type, round(sum(pct_of_nav), 2) as alloc_pct, count() as n_securities
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
     AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                        WHERE fund_name = 'DSP_MULTI_ASSET')
   GROUP BY asset_type ORDER BY alloc_pct DESC;

4. 6-month allocation trend (equity vs debt vs gold):
   SELECT as_of_month, asset_type, round(sum(pct_of_nav), 2) as alloc_pct
   FROM market_data.mf_holdings FINAL
   WHERE fund_name = 'DSP_MULTI_ASSET'
     AND as_of_month >= addMonths(today(), -6)
   GROUP BY as_of_month, asset_type
   ORDER BY as_of_month, alloc_pct DESC;

From the data:
- Is the fund running higher equity or debt/gold allocation vs its stated mandate?
- Which single security has the highest concentration risk (pct_of_nav)?
- Has the gold allocation increased or decreased over 6 months?
- Cross-reference the equity holdings vs signal aggregator: python src/main.py signals
```

### 24. DSP Fund Comparison — Pick the Best Multi-Asset Option

Compare DSP Multi Asset vs DSP Multi Asset Omni FoF side by side.

```
Compare DSP Multi Asset (DSP_MULTI_ASSET, scheme 152056) vs
DSP Multi Asset Omni FoF (DSP_MULTI_ASSET_OMNI_FOF, scheme 154167):

Step 1 — MoM Return comparison:
  python src/scripts/portfolio/fund_mom_returns.py --scheme 152056 --months 12
  python src/scripts/portfolio/fund_mom_returns.py --scheme 154167 --months 12

Step 2 — Holdings comparison (latest month):
  SELECT fund_name, asset_type, round(sum(pct_of_nav), 2) as alloc_pct
  FROM market_data.mf_holdings FINAL
  WHERE fund_name IN ('DSP_MULTI_ASSET', 'DSP_MULTI_ASSET_OMNI_FOF')
    AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL)
  GROUP BY fund_name, asset_type
  ORDER BY fund_name, alloc_pct DESC;

Step 3 — Answer these questions from the data:
- Which fund has the higher 12-month return?
- Which has a higher equity allocation? Higher gold/commodity?
- DSP Multi Asset Omni FoF is a Fund-of-Funds — does its holdings data show underlying funds
  or direct securities?
- Given the limited history of DSP_MULTI_ASSET_OMNI_FOF (launched late 2025), is
  DSP_MULTI_ASSET the safer choice for long-term allocation analysis?
- Recommended choice for a conservative multi-asset investor and why.
```

### 25. Fund Lookup by Name — Resolve Scheme Code & Compute Returns

For any fund (not just DSP/pre-known scheme codes) — resolve its AMFI scheme code by name, then compute NAV returns.

```
I don't know the scheme code for "Abakkus Small Cap Fund". Resolve it and compute returns:

Step 1 — Search for matching scheme codes (may list both Direct and Regular Plan):
  python src/scripts/portfolio/fund_mom_returns.py --search "Abakkus Small Cap Fund"

Step 2 — Compute MoM returns for the Direct Plan-Growth code found above:
  python src/scripts/portfolio/fund_mom_returns.py --scheme <CODE> --months 24

Report: period return, best/worst month, average MoM return, positive vs negative month count.

Note: NAV is fetched live from mfapi.in — no ClickHouse import is needed or possible for this
fund (only a fixed ETF/index watchlist is imported into market_data.mf_nav). Expense ratio is
NOT available from mfapi.in or anywhere else in this codebase — don't estimate it, state it's
unavailable if asked.
```
