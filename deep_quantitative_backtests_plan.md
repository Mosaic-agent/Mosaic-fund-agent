# Plan: Deep Quantitative Backtests on DSP Strategies

## 1. Objective
To construct and run deep quantitative backtests that simulate the "Relative Value (RV) Tactical Pivot" and "Contrarian Mean-Reversion" strategies observed in the top-performing DSP Mutual Funds. By applying these rules to historical data (June 2022 – March 2026), we will quantify the alpha generation, risk-adjusted returns (Sharpe), and drawdown protection of these "Smart Money" approaches compared to static benchmarks.

## 2. Strategies to Backtest

### Strategy A: The GSR Mean-Reversion Scalper (Macro/Hybrid Focus)
**Inspiration:** DSP Multi Asset / Aggressive Hybrid funds.
**Logic:** Use the Gold-Silver Ratio (GSR) as the primary tactical allocation lever between Equities and Precious Metals.
- **Baseline:** Hold 60% Equity (e.g., NIFTYBEES) / 20% Bonds (e.g., LIQUIDBEES) / 20% Gold (e.g., GOLDBEES).
- **Tactical Bearish Pivot (Short Gold):** When GSR drops below a critical threshold (e.g., `< 60`, meaning Gold is highly expensive relative to Silver), reduce Gold allocation to 0% (or simulate a short via -10% weight) and shift capital to Equity or Cash.
- **Tactical Bullish Pivot (Long Gold):** When GSR rises above a high threshold (e.g., `> 85`), increase Gold allocation to 30-40% by reducing Equity/Bonds.

### Strategy B: Contrarian Value & Momentum Fade (Equity Focus)
**Inspiration:** DSP Small Cap, Value, and Midcap funds.
**Logic:** Trade *against* extreme momentum. Front-run reversals when internal quantitative momentum models (or broader market indicators) flash extreme panic or extreme greed.
- **Baseline:** 100% Equity (Nifty 500 equivalent).
- **Buy the Panic (Value Fade):** When the short-term market momentum score drops below the bottom 10th percentile (extreme bearishness), aggressively overweight high-beta assets (e.g., Small Caps / SMALL250).
- **Sell the Greed:** When momentum exceeds the 90th percentile, rotate out of Small/Mid caps into defensive Large Caps or Cash.

## 3. Data Sources & Tooling
- **Holdings Data:** `market_data.mf_holdings` (to reference exact historical weightings of DSP funds for comparison).
- **Pricing Data:** `market_data.daily_prices` (for historical close prices of Gold, Silver, DXY, US10Y, and proxy ETFs like NIFTYBEES, GOLDBEES).
- **Scripting:** Python, `pandas` (for time-series alignment and vectorized backtesting), `numpy` (for drawdown and Sharpe calculations).
- **Target Script:** Create `scripts/backtest_dsp_strategies.py`.

## 4. Implementation Steps

1. **Data Preparation:**
   - Query daily closing prices for GC=F (Gold), SI=F (Silver), NIFTYBEES (Equity proxy), and LIQUIDBEES (Cash/Bond proxy) from ClickHouse.
   - Forward-fill missing data to create a continuous daily time-series from June 2022 to March 2026.
   - Calculate the daily GSR (Gold / Silver price).

2. **Signal Generation:**
   - Create a daily signal vector for Strategy A (GSR based) and Strategy B (Momentum based).
   - Apply a 1-day lag to signals to prevent look-ahead bias (i.e., trade on day T+1 based on day T's closing signal).

3. **Portfolio Simulation:**
   - Calculate daily returns for the underlying assets.
   - Compute the daily strategy return as the dot product of the lagged target weights and the asset returns.
   - Rebalance portfolio weights daily or monthly based on the strategy rules.

4. **Performance Metrics Calculation:**
   - Calculate Cumulative Return (Equity Curve).
   - Calculate Annualized Return (CAGR).
   - Calculate Annualized Volatility.
   - Compute Sharpe Ratio (assuming a ~6% risk-free rate).
   - Compute Maximum Drawdown (peak-to-trough decline).

## 5. Output & Verification
- The script will output a rich terminal table comparing:
  - Strategy A vs. 60/40 Benchmark vs. DSP Aggressive Hybrid actual returns.
  - Strategy B vs. Nifty 500 Benchmark vs. DSP Small Cap actual returns.
- **Validation:** Ensure the simulated equity curves align logically with the major macro turning points (e.g., the Q1 2026 GSR collapse).
- **Actionable Insight:** The final output will determine if the DSP ruleset should be permanently coded into our live `src/agents/signal_aggregator.py` as a primary trading algorithm.