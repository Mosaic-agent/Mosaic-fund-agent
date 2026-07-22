---
name: shoonya-session-agent
description: Authenticate, check, or refresh the live Shoonya (Finvasia) broker session and WebSocket feed. Use when the user asks "check shoonya session", "connect to shoonya", "login shoonya", or invokes /shoonya-session.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 20
---

# Skill: Shoonya Broker Session Manager

Manages daily OAuth login, session token persistence (`output/.cache/shoonya_session.json` and ClickHouse), and API connection verification for Shoonya (Finvasia).

## Trigger

Use this skill when the user asks:
- "Connect to Shoonya"
- "Check my Shoonya session"
- "Login to Shoonya broker"
- "/shoonya-session [check|login]"

## What it does

1. **Session Check**: Verifies whether the cached session (`output/.cache/shoonya_session.json` / ClickHouse) is active (<20h old) and tests API limits.
2. **OAuth Authorization**: Generates the Shoonya OAuth login URL and exchanges the authorization `--code` for session tokens (`susertoken` and `access_token`).
3. **Session Persistence**: Saves tokens locally and syncs to `market_data.shoonya_session` in ClickHouse.

## Usage

### Check Session Health

```bash
ALLOW_LOCAL_RUN=1 .venv/bin/python -c "from src.importer.fetchers.shoonya_fetcher import get_shoonya_api; api = get_shoonya_api(); print('SHOONYA SESSION:', 'ACTIVE' if api else 'EXPIRED')"
```

### Complete OAuth Login with Code

```bash
.venv/bin/python src/scripts/portfolio/shoonya_login.py --code <OAUTH_AUTHORIZATION_CODE>
```

### Pre-Flight Session Audit (for Cron / Market Open)

```bash
ALLOW_LOCAL_RUN=1 .venv/bin/python src/agents/live_monitor.py --check-session
```
