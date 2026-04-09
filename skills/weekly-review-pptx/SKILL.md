# Skill: Weekly-Review-PPTX

Generates a professional weekly portfolio review slide deck combining macro themes, ETF performance, institutional flows, ML signals, and next-week outlook.

## Trigger

Use this skill when the user asks:
- "Create a weekly review slide deck"
- "Generate a PowerPoint for this week's analysis"
- "Make a presentation with this week's ETF signals"
- "Weekly report as slides"

## Slide Structure (8 slides)

### Slide 1: Cover
- Title: "Mosaic Fund Agent — Weekly Review"
- Subtitle: Week ending [date]
- Key stat callouts: GOLDBEES +X%, Nifty +X%, FII net flow

### Slide 2: Macro Environment
- Active macro themes this week (icons + one-liner each)
- Overall market regime: Risk-On / Risk-Off / Mixed
- DXY, US10Y, USDINR summary

### Slide 3: Institutional Flows (FII / DII)
- Bar chart: 5-day FII vs DII net flows
- Cumulative 5-day totals
- Signal: "FII still selling — wait for reversal"

### Slide 4: ETF Performance Scorecard
- Table: ETF | 1d | 5d | 20d | Signal
- Color coded: green (positive), red (negative)
- Highlight outperformers and laggards

### Slide 5: Gold Intelligence
- GOLDBEES price + NAV spread
- COMEX XAU signal
- Central bank buying streak
- ML prediction: expected 5d return + regime signal

### Slide 6: Opportunities
- BUY candidates with conviction rating
- Entry rationale in bullet points
- Risk factors

### Slide 7: Avoid / Reduce
- SELL / AVOID candidates
- Why: macro headwinds + flow data
- Wait trigger (e.g., "Buy BANKBEES when FII net positive for 2 days")

### Slide 8: Next Week Watchlist
- Key events to watch: Fed minutes, RBI data, earnings (TCS, Infosys)
- Triggers that would change the thesis
- "Not financial advice" disclaimer

## Design Guidelines

- Color palette: Midnight Executive (Navy #1E2761, Ice Blue #CADCFC, White)
- Dark background for cover + conclusion, light for content slides
- Use `anthropic-skills:pptx` with pptxgenjs for creation
- Font: Calibri headers, Calibri Light body

## File Naming

```
output/weekly_review_YYYYMMDD.pptx
```

## Data Sources

- `market_data.daily_prices` — weekly ETF returns
- `market_data.fii_dii_flows` — weekly institutional flows
- `market_data.ml_predictions` — GOLDBEES ML forecast
- `src/tools/macro_event_scanner.py` — active macro themes
- `src/tools/etf_news_scanner.py` — ETF news sentiment

## Dependencies

- `anthropic-skills:pptx` skill loaded
- `pptxgenjs` installed (`npm install -g pptxgenjs`)
- ClickHouse running
