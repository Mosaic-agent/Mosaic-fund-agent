---
name: live-monitor
description: Start the standalone live multi-symbol anomaly + news-correlation monitor for NSE market hours. Watches indices, current holdings, and configured symbols via Shoonya websocket, detects volume-spike/price-break anomalies on 5-minute bars, and pushes a Slack alert enriched with correlated breaking news. Trigger when the user says "start live monitor", "watch the market live", "alert me on anomalies", "check shoonya session", or invokes /live-monitor.
---

# Skill: Live Multi-Symbol Anomaly + News-Correlation Monitor

Watches NSE indices, current portfolio holdings, and any ad-hoc configured symbols via a single Shoonya websocket connection through the full trading session, scores every closed 5-minute bar for a price-break or volume-spike anomaly (MAD-based robust z-score, same formula as the EOD anomaly pipeline), and — when one fires — races concurrent news lookups and sends ONE combined Slack alert (anomaly + correlated headline, or "no news found" if nothing surfaces within the timeout).

Built after reconstructing, after the fact, that a Nifty selloff broke sharply within the same ~5-minute window as a breaking news headline — this surfaces that correlation live instead of requiring a manual after-the-close investigation.

## Trigger

Use this skill when the user asks:
- "Start the live monitor"
- "Watch the market live for anomalies"
- "Alert me if Nifty/my holdings break out during market hours"
- "Check my Shoonya session before market open"
- "/live-monitor"

## What it does

1. **Watchlist resolution (once, at startup)**: static indices (NIFTY, NIFTY BANK, INDIA VIX) + current holdings from ClickHouse `market_data.user_holdings` + any symbols in the ad-hoc config file. Adding a symbol requires a restart — dynamic runtime registration is a known v1 limitation, not yet built.
2. **Live bar aggregation**: one Shoonya websocket connection subscribed to every watched token at once, rolled into 5-minute OHLCV bars per symbol.
3. **Anomaly scoring**: each closed bar is scored with a rolling robust z-score on both bar-over-bar return (`price_break`) and bar volume (`volume_spike`, one-sided). The 09:15–09:20 opening-auction bar is always excluded (opening jumps are structurally different from a live anomaly).
4. **VIX confirmation gate**: a `price_break` alert on any non-VIX symbol is only forwarded if INDIA VIX also moved meaningfully (`|z_return| >= LIVE_MONITOR_VIX_CONFIRMATION_ZSCORE`, default 2.0) in the *same* bar — cuts single-stock/single-ETF noise that isn't part of a broader market move. `volume_spike` alerts and VIX's own alerts are never gated. Fails open (never suppresses) if VIX has no scored baseline yet or its data is stale by more than one bar.
5. **News correlation + Slack**: on a flagged bar, races Google News + Yahoo Finance news (bounded by a short timeout) and sends one Slack message with the anomaly and whatever headline was found. Every alert is also logged to ClickHouse `market_data.live_alerts` regardless of Slack delivery outcome.

## Usage

### Morning checklist (run before market open)
```bash
python src/agents/live_monitor.py --check-session-only
```
Validates the cached Shoonya session WITHOUT starting the websocket, and exits non-zero if an interactive OAuth re-login is required — intended to be cron'd around 08:30 IST so a stale session is caught with time to fix it before 09:15.

### Run from CLI
```bash
ALLOW_LOCAL_RUN=1 python src/agents/live_monitor.py \
    --watchlist-config config/live_watchlist.yaml
```
Runs headless (structured logging + Slack, no fullscreen TUI) until NSE close (15:30 IST) or Ctrl+C. A second Ctrl+C force-exits if shutdown is stuck.

### Dry-run (no live Shoonya, exercises the full alert pipeline)
```bash
python src/agents/live_monitor.py --dry-run
```
Replays a synthetic tick sequence (stable baseline + one injected spike) through the real bar-builder → EventBus → Slack/ClickHouse pipeline. Useful for verifying Slack webhook configuration and ClickHouse logging without waiting for a real market move.

## Configuration

Settings in `.env` / `config/settings.py`:
- `SLACK_WEBHOOK_URL` — leave blank to disable Slack delivery (alerts are still logged to ClickHouse).
- `LIVE_MONITOR_ZSCORE_THRESHOLD` (default 3.0) — not yet validated on intraday bars; paper-test with a lower threshold (1.5–2.0) first to see real alert frequency before trusting the default.
- `LIVE_MONITOR_VIX_CONFIRMATION_ZSCORE` (default 2.0) — deliberately lower than the alert threshold; asks "did VIX move meaningfully", not "did VIX itself trip its full anomaly threshold".
- `LIVE_MONITOR_BAR_SECONDS` (default 300), `LIVE_MONITOR_BUFFER_SIZE` (default 30), `LIVE_MONITOR_NEWS_TIMEOUT_SECONDS` (default 5.0), `LIVE_MONITOR_WATCHLIST_CONFIG` (default `config/live_watchlist.yaml`).

## Source Files

- `src/agents/live_monitor.py` — process orchestration, websocket manager, CLI
- `src/agents/live_bar_builder.py` — pure tick→bar→z-score scorer (unit-tested in `tests/test_live_bar_builder.py`)
- `src/agents/live_watchlist.py` — watchlist resolution
- `src/events/live_events.py` — `LiveAlertEvent`
- `src/events/observers.py` — `LiveAlertObserver` (news race + Slack + ClickHouse logging)
