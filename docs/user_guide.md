# Mosaic User Guide

Welcome to the **Mosaic User Guide**. This document helps you navigate the features and tools available in Mosaic, whether you are a non-technical investor, an active market researcher, or a quantitative engineer.

---

## 🧭 Navigation by Persona

To help you get started quickly, identify your profile below:

| Profile | Primary Tooling | Best Starting Point |
|---|---|---|
| **🟢 Non-Technical Investor** | Streamlit Web UI, LLM Chat Co-pilot | [1. Visual Web UI](#1-using-the-visual-web-ui) |
| **👑 Active Market Researcher** | Conversational CLI, Pre-built Reports | [2. Conversational Q&A](#2-conversational-qa) & [4. Equity Research](#4-multi-market-equity-research) |
| **🔬 Quantitative Engineer** | ClickHouse DB, ML Models, GARCH Vol | [5. ML & Volatility Pipelines](#5-ml--volatility-pipelines) & [6. Database Engineering](#6-database-engineering) |

---

## 1. Using the Visual Web UI

The easiest way to interact with Mosaic is through the local web interface powered by Streamlit.

### How to Launch
1. **Via Docker (Recommended):** If you are running Docker Desktop, double-click `run.sh` (Mac/Linux) or `run.bat` (Windows). The container will launch and automatically open `http://localhost:8501`.
2. **Via CLI:** Run the following command in your terminal:
   ```bash
   python src/main.py ui
   ```

### Core Web Dashboard Tabs
*   **🪁 Kite Dashboard:** Sync your live holdings, margin limits, and trade history from your Zerodha Kite broker account.
*   **📊 Signals Dashboard:** View the real-time **6-Pillar Composite Scores** (0-100) for all 18+ core Indian ETFs.
*   **💾 Database Ingest:** Trigger historical data imports visually for categories like Commodities, Mutual Funds, and institutional flows.
*   **🔍 SQL Explorer:** Write and execute custom SQL queries directly against ClickHouse. Result sets are rendered as interactive tables and charts.

---

## 2. Conversational Q&A (The Chat Co-pilot)

Mosaic features a multi-agent routing loop. You don't need to know SQL or database command arguments; you can ask questions in plain English.

### How to Query
Depending on your environment setup, you can launch the conversational agents as follows:

*   **🐳 Via the Docker Wrapper (Recommended / Zero-Dependency):**
    If you are running Mosaic through Docker, you can execute the agent without setting up Python locally:
    ```bash
    # On macOS or Linux:
    ./mosaic.sh ask "YOUR_QUESTION"

    # On Windows:
    mosaic.bat ask "YOUR_QUESTION"
    ```
*   **🐍 Via Local Python CLI:**
    ```bash
    python src/main.py ask "YOUR_QUESTION"
    ```

> [!TIP]
> This wrapper pattern applies to all CLI commands and scripts. For example, to run the GOLDBEES report in Docker, use `./mosaic.sh src/scripts/goldbees_report.py` instead of `python src/scripts/goldbees_report.py`.

### High-Value Questions to Try:
*   **Portfolio Analysis:**
    *   *"Am I overexposed to the IT or PSU sectors?"*
    *   *"Which of my current holdings has the worst news sentiment today?"*
*   **ETF Pricing & Premiums:**
    *   *"Which gold or international ETFs are trading at a high premium over their iNAV?"*
    *   *"Are there any scarcity premium signals active on international ETFs?"*
*   **Stock Deep Dives:**
    *   *"Show me a detailed financial analysis of ADSK."*
    *   *"Analyze Roku's research & development expense trend over the last 3 years."*

---

## 3. Volatility, ML, & ETF Signal Pipelines

For tactical asset allocators, Mosaic provides pre-built terminal telemetry commands.

### A. The GOLDBEES ML & Sizing Pipeline
Runs the LightGBM classifier, computes the Kelly Sizing based on past model hit rates, and scales the result based on GARCH(1,1) conditional volatility.
```bash
# Run the full GOLDBEES quant report with LLM recommendation:
python src/scripts/goldbees_report.py
```
*   **What it returns:** Expected return percentages, model AUC/accuracy, conditional volatility bands, GARCH regimes, and the final **Blended Weight recommendation** (50% Risk Governor + 50% Kelly).

### B. 6-Pillar Composite ETF Signals
Computes quantitative scorecards (0-100 scale) for all 18+ registered ETFs based on Macro, Flow, Valuation (iNAV), Sentiment, ML predictions, and Volatility.
```bash
# View the detailed score breakdown for all ETFs:
python src/main.py signals --verbose
```

### C. COMEX Pre-Market Telemetry
For commodity traders, get international market levels before the Indian NSE opens:
```bash
python src/main.py comex
```
*   **Assets covered:** Gold (XAU), Silver (XAG), Copper (HG), Platinum (XPT), and Palladium (XPD) vs. previous sessions.

### D. GOLDBEES ML Model Drift Monitor
Evaluates the accuracy of the predictive models by comparing historical predictions with actual matured realized returns, updating the database table, and triggering alerts/retraining if stats drop:
```bash
# Run the drift monitor:
python src/main.py drift-monitor --lookback 90

# Or using the Docker wrapper:
./mosaic.sh drift-monitor
```
*   **Metrics evaluated:** Rolling Hit Ratio (direction accuracy), Area Under ROC Curve (AUC), and Mean Absolute Error (MAE).
*   **Automatic Retraining:** If hit ratio falls below 50% or AUC drops below 0.50, the monitor clears cache files and triggers model retraining automatically with 7 walk-forward CV splits.

---

## 4. Multi-Market Equity Research

Mosaic connects to SEC filings (US) and domestic exchanges (India) to generate deep research reports.

### 🇮🇳 Indian Stock Deep Dives (NSE/BSE)
Queries quarterly results, cash flow statements, promoter/DII shareholding changes, and DSP active mutual fund conviction:
```bash
# Search by name or ticker:
python src/main.py ask "Research HDFC Bank"
```
*   **Output:** Includes interactive ASCII price charts, QoQ promoter trend arrows, cash flow margins, and cross-fund ownership data.

### 🇺🇸 US Stock Deep Dives (SEC EDGAR)
Extracts and parses SEC 10-K/10-Q text, XBRL financials, executive pay databases, and hiring trends:
```bash
# Research a US-listed company:
python src/main.py ask "Run deepdive analysis on ADSK"
```
*   **Under the Hood:** Gathers filings, searches executive compensation, parses hiring/talent metrics (Workday job scrapers), and generates a structured Markdown report under `output/deepdive/TICKER/`.

---

## 5. Volatility & Anomaly Scaling (Under the Hood)

For quants looking to understand the mathematical mechanics:

1.  **GARCH(1,1) Volatility:**
    The platform models log returns $r_t$ with conditional variance $\sigma_t^2$:
    $$\sigma_t^2 = \omega + \alpha \epsilon_{t-1}^2 + \beta \sigma_{t-1}^2$$
    Standardized residuals are analyzed to detect volatility clustering.
2.  **Isolation Forest Regimes:**
    Standardized residuals, currency spreads (USDINR/DXY), and COT positions are passed into an Isolation Forest classifier to tag abnormal market regimes.
3.  **Position Scaling:**
    The Risk Governor scales position weights inversely to conditional volatility:
    $$w_t = \frac{\sigma_{\text{target}}}{\sigma_t}$$
    This is then blended with the Kelly Sizing:
    $$\text{Kelly Sizing} = \frac{p \cdot R - (1 - p)}{R}$$
    *(where $p$ is the LightGBM probability and $R$ is the risk-to-reward ratio)*.

---

## 6. Database Engineering

All quantitative datasets are centralized in **ClickHouse** inside the `market_data` database.

### Core Tables
*   `daily_prices` — Historical asset pricing (final deduplication via `ReplacingMergeTree`).
*   `mf_holdings` — Mutual Fund portfolio holdings (historically covering DSP, Nippon, ICICI portfolios).
*   `fii_dii_flows` — Daily FII/DII net cash flow records.
*   `inav_snapshots` — Intra-day iNAV data used for premium/discount Z-scores.

### Data Freshness Gate
Because macro signals rely on cross-asset correlations, you must sync database states before running models:
```bash
# Backfill/Delta-sync all categories:
python src/main.py import --category etfs,stocks,mf,fii_dii,cot,fx_rates
```

---

## 🔌 Running Locally with Ollama

To run Mosaic completely offline without sending data to third-party APIs (like OpenAI or Anthropic):

1.  **Install Ollama:** Follow download instructions at [ollama.com](https://ollama.com).
2.  **Pull Gemma:**
    ```bash
    ollama pull gemma4:latest
    ```
3.  **Create customized model:**
    ```bash
    ollama create mosaic-gemma4 -f ollama/Modelfile
    ```
4.  **Configure your `.env`:**
    ```env
    LLM_PROVIDER=openai
    LLM_MODEL=mosaic-gemma4
    LLM_BASE_URL=http://localhost:11434/v1
    ```
    *The agent system will automatically compress prompts and inject raw structured data tables to prevent local model hallucinations.*
