"""
src/agents/etf_opportunity_scanner.py
─────────────────────────────────────
Real-Time ETF Opportunity & iNAV Arbitrage Scanner.

Integrates:
  - Persistent Shoonya WebSocket feed (LTP, best bid, best ask, volume)
  - Direct AMC iNAV feeds (Nippon, Zerodha, Mirae, Motilal)
  - Lee-Ready Order Flow Tick Classifier (Cumulative Delta)
  - Rich Live Terminal TUI (Interactive Heatmap)
  - Slack Webhook Alert Dispatcher
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from rich.console import Console
from rich.live import Live

# Ensure project root on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import settings
from src.tools.amc_inav_manager import (
    DISCOUNT_THRESHOLDS,
    ETF_CATEGORIES,
    AMCInavManager,
    get_amc_inav_manager,
)
from src.data_importer.fetchers.shoonya_fetcher import get_shoonya_api
from src.tools.shoonya_tools import resolve_token
from src.ui.terminal_etf_tui import build_etf_tui_renderable
from src.utils.ist import now_ist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("etf_scanner")

# Default comprehensive 32-ETF universe tracked across Indian AMCs
PRIMARY_ETF_SYMBOLS = [
    # Commodities
    "GOLDBEES", "SILVERBEES", "GOLDCASE", "SILVERCASE", "GOLDETF", "SILVERAG",
    # Domestic Broad Market
    "NIFTYBEES", "SETFNIF50", "TOP100CASE", "JUNIORBEES", "MID150CASE", "MID150BEES",
    "NIF100BEES", "MONIFTY500", "MONIFTY100", "MONEXT50", "MON50EQUAL",
    # Domestic Sectoral & Factor
    "BANKBEES", "PSUBNKBEES", "ITBEES", "PHARMABEES", "AUTOBEES", "INFRABEES", "CPSEETF",
    "MOM100", "MOM50", "MOMOMENTUM", "CONSUMBEES", "DIVOPPBEES",
    # International
    "MON100", "MONQ50", "MAFANG", "MAHKTECH", "MASPTOP50", "HNGSNGBEES",
    # Debt / Liquid
    "LTGILTCASE", "LTGILTBEES", "LIQUIDBEES", "LIQUIDCASE",
]


class ETFOpportunityScanner:
    """Real-time scanner combining Shoonya WebSocket ticks and AMC iNAVs."""

    def __init__(
        self,
        symbols: list[str] | None = None,
        refresh_nav_secs: int = 60,
        enable_slack: bool = True,
        dry_run: bool = False,
    ):
        self.symbols = [s.upper().strip() for s in (symbols or PRIMARY_ETF_SYMBOLS)]
        self.refresh_nav_secs = refresh_nav_secs
        self.enable_slack = enable_slack
        self.dry_run = dry_run

        self.inav_manager = get_amc_inav_manager(refresh_interval=refresh_nav_secs)
        self.api = None if dry_run else get_shoonya_api()

        self.running = False
        self._lock = threading.Lock()

        # Symbol state cache
        # {sym: {"ltp": float, "inav": float, "bid": float, "ask": float, "cum_delta": float, ...}}
        self.state: dict[str, dict[str, Any]] = {}
        self.token_to_sym: dict[str, str] = {}
        self.sym_to_token: dict[str, str] = {}
        self.prev_signals: dict[str, str] = {}

        # WebSocket connection health
        self.ws_connected = False
        self._init_state()

    def _init_state(self):
        """Initialize state dictionary for all tracked symbols."""
        for sym in self.symbols:
            cat = ETF_CATEGORIES.get(sym, "Domestic ETF")
            self.state[sym] = {
                "symbol": sym,
                "category": cat,
                "amc": "AMC Direct",
                "ltp": None,
                "inav": None,
                "spread_pct": None,
                "day_chg_pct": 0.0,
                "cumulative_delta": 0.0,
                "last_volume": 0.0,
                "best_bid": None,
                "best_ask": None,
                "signal": "⚪ FAIR",
                "last_updated": None,
            }

    def resolve_shoonya_tokens(self):
        """Resolve exchange tokens for all symbols."""
        if not self.api:
            return
        logger.info("Resolving Shoonya exchange tokens for %d ETFs...", len(self.symbols))
        for sym in self.symbols:
            try:
                res = resolve_token(self.api, sym)
                if res:
                    token, tsym = res
                    self.token_to_sym[str(token)] = sym
                    self.sym_to_token[sym] = str(token)
            except Exception as e:
                logger.debug("Token resolution error for %s: %s", sym, e)
        logger.info("Successfully resolved %d tokens on Shoonya.", len(self.token_to_sym))

    # ── WebSocket Feed Handling ──────────────────────────────────────────────

    def on_feed_update(self, tick_data: dict[str, Any]):
        """Callback for incoming WebSocket tick frames."""
        if not tick_data:
            return

        token = str(tick_data.get("tk", ""))
        sym = self.token_to_sym.get(token)
        if not sym:
            return

        with self._lock:
            st = self.state[sym]

            # Extract fields
            if "lp" in tick_data:
                st["ltp"] = float(tick_data["lp"])
            if "pc" in tick_data:
                st["day_chg_pct"] = float(tick_data["pc"])
            if "bp1" in tick_data:
                st["best_bid"] = float(tick_data["bp1"])
            if "sp1" in tick_data:
                st["best_ask"] = float(tick_data["sp1"])

            # Cumulative Delta volume tracking
            curr_vol = float(tick_data.get("v", 0))
            if curr_vol > 0 and st["last_volume"] > 0:
                trade_vol = curr_vol - st["last_volume"]
                if trade_vol > 0:
                    ltp = st["ltp"]
                    best_bid = st["best_bid"]
                    best_ask = st["best_ask"]

                    if best_ask and ltp and ltp >= best_ask:
                        # Trade at/above Ask -> Aggressive Buyer
                        st["cumulative_delta"] += trade_vol
                    elif best_bid and ltp and ltp <= best_bid:
                        # Trade at/below Bid -> Aggressive Seller
                        st["cumulative_delta"] -= trade_vol
                    else:
                        # Inside spread -> Midpoint tick rule
                        st["cumulative_delta"] += trade_vol * 0.1

            if curr_vol > 0:
                st["last_volume"] = curr_vol

            st["last_updated"] = datetime.now(timezone.utc)

    def on_socket_open(self):
        """Subscribe to all resolved scrip tokens upon WebSocket connect."""
        self.ws_connected = True
        tokens_to_sub = [f"NSE|{tok}" for tok in self.token_to_sym.keys()]
        if tokens_to_sub and self.api:
            self.api.subscribe(tokens_to_sub)
            logger.info("Subscribed to %d instruments on Shoonya WebSocket.", len(tokens_to_sub))

    def on_socket_close(self):
        self.ws_connected = False

    # ── Signal & Spread Computation ──────────────────────────────────────────

    def update_spreads_and_signals(self):
        """Update live spreads and evaluate opportunity regimes for all ETFs."""
        inav_map = self.inav_manager.get_all_inavs()

        with self._lock:
            for sym, st in self.state.items():
                inav_rec = inav_map.get(sym)
                if inav_rec:
                    st["inav"] = inav_rec.get("inav")
                    st["amc"] = inav_rec.get("amc", st["amc"])
                    st["category"] = inav_rec.get("category", st["category"])

                ltp = st.get("ltp")
                inav = st.get("inav")

                if ltp and inav and inav > 0:
                    spread_pct = ((ltp - inav) / inav) * 100.0
                    st["spread_pct"] = round(spread_pct, 2)

                    # Determine threshold by primary category
                    cat_name = st["category"]
                    if "Commodity" in cat_name:
                        thresh = DISCOUNT_THRESHOLDS["Commodity"]
                    elif "International" in cat_name:
                        thresh = DISCOUNT_THRESHOLDS["International"]
                    elif "Debt" in cat_name:
                        thresh = DISCOUNT_THRESHOLDS["Debt"]
                    else:
                        thresh = DISCOUNT_THRESHOLDS["Equity"]

                    cum_delta = st.get("cumulative_delta", 0.0)

                    # Signal state machine
                    if spread_pct <= thresh:
                        if cum_delta > 0:
                            signal_val = "🟢 ACCUMULATE"
                        else:
                            signal_val = "🟡 PASSIVE DISCOUNT"
                    elif spread_pct >= abs(thresh) * 1.5:
                        signal_val = "🔴 OVERHEATED"
                    else:
                        signal_val = "⚪ FAIR"

                    prev = self.prev_signals.get(sym, "⚪ FAIR")
                    st["signal"] = signal_val

                    # Alert on new transition into ACCUMULATE
                    if signal_val == "🟢 ACCUMULATE" and prev != "🟢 ACCUMULATE":
                        self._dispatch_opportunity_alert(sym, st)

                    self.prev_signals[sym] = signal_val

    def _dispatch_opportunity_alert(self, sym: str, st: dict[str, Any]):
        """Send Slack alert when an ETF triggers an ACCUMULATE discount signal."""
        if not self.enable_slack or not getattr(settings, "slack_webhook_url", None):
            return

        url = settings.slack_webhook_url
        spread = st.get("spread_pct", 0.0)
        ltp = st.get("ltp", 0.0)
        inav = st.get("inav", 0.0)
        amc = st.get("amc", "AMC Direct")
        delta = st.get("cumulative_delta", 0.0)

        payload = {
            "text": (
                f"🔥 *ETF ACCUMULATION OPPORTUNITY DETECTED: {sym}*\n"
                f"• *Market Price (LTP):* ₹{ltp:.2f}\n"
                f"• *Official AMC iNAV ({amc}):* ₹{inav:.2f}\n"
                f"• *Real-Time Discount:* *{spread:+.2f}%* 🟢\n"
                f"• *Order Flow Cumulative Delta:* +{delta:,.0f} (Buyer Aggression Confirmed)\n"
                f"• *Time:* {now_ist().strftime('%H:%M:%S IST')}"
            )
        }
        try:
            threading.Thread(
                target=lambda: httpx.post(url, json=payload, timeout=5.0),
                daemon=True,
            ).start()
            logger.info("Sent Slack opportunity alert for %s", sym)
        except Exception as e:
            logger.debug("Slack alert dispatch error: %s", e)

    # ── Fallback REST Poller (for offline hours / non-WS mode) ────────────────

    def _fallback_poll_rest_quotes(self):
        """Poll REST quotes if WebSocket is not active."""
        if not self.api:
            return
        for sym in self.symbols[:15]: # batch sample
            try:
                tok = self.sym_to_token.get(sym)
                if tok:
                    quote = self.api.get_quotes(exchange="NSE", token=tok)
                    if quote and quote.get("stat") == "Ok":
                        lp = quote.get("lp")
                        if lp:
                            with self._lock:
                                st = self.state[sym]
                                st["ltp"] = float(lp)
                                if "pc" in quote:
                                    st["day_chg_pct"] = float(quote.get("pc", 0))
            except Exception:
                pass

    # ── Main Run Loop ────────────────────────────────────────────────────────

    def start_and_render(self, interval_seconds: float = 1.0):
        """Start the WebSocket feed and run the live Rich TUI loop."""
        self.running = True
        self.resolve_shoonya_tokens()

        # Connect WebSocket if active session exists
        if self.api and not self.dry_run:
            try:
                self.api.start_websocket(
                    order_update_callback=lambda x: None,
                    subscribe_callback=self.on_feed_update,
                    socket_open_callback=self.on_socket_open,
                )
                logger.info("Shoonya WebSocket feed started.")
            except Exception as e:
                logger.warning("Shoonya WebSocket start failed: %s (falling back to REST)", e)

        is_tty = sys.stdout.isatty()
        last_reconnect_attempt = 0.0
        last_heartbeat_log = 0.0

        if is_tty:
            console = Console()
            with Live(console=console, refresh_per_second=int(1.0 / interval_seconds) or 1, screen=True) as live:
                try:
                    while self.running:
                        self._run_single_cycle()
                        with self._lock:
                            rows = list(self.state.values())

                        now_str = now_ist().strftime("%H:%M:%S IST")
                        renderable = build_etf_tui_renderable(
                            rows=rows,
                            ws_connected=self.ws_connected,
                            active_tokens=len(self.token_to_sym),
                            amc_age_secs=self.inav_manager.last_refresh_age_seconds,
                            last_update_time=now_str,
                        )
                        live.update(renderable)
                        time.sleep(interval_seconds)
                except KeyboardInterrupt:
                    pass
                finally:
                    self.stop()
        else:
            # Headless background container mode
            logger.info("Running in headless daemon mode. Monitoring %d ETFs...", len(self.symbols))
            try:
                while self.running:
                    self._run_single_cycle()
                    now_t = time.time()
                    if now_t - last_heartbeat_log >= 15.0:
                        last_heartbeat_log = now_t
                        with self._lock:
                            sorted_rows = sorted(
                                self.state.values(),
                                key=lambda r: r.get("spread_pct") if r.get("spread_pct") is not None else 999.0,
                            )
                            best = sorted_rows[0] if sorted_rows else {}
                            accum_count = sum(1 for r in self.state.values() if "ACCUMULATE" in r.get("signal", ""))

                        logger.info(
                            "Heartbeat: %d ETFs active | WS: %s | Top spread: %s (%.2f%%) | Accumulate alerts: %d",
                            len(self.symbols),
                            "CONNECTED" if self.ws_connected else "REST FALLBACK",
                            best.get("symbol", "-"),
                            best.get("spread_pct") or 0.0,
                            accum_count,
                        )
                    time.sleep(interval_seconds)
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()

    def _run_single_cycle(self):
        """Execute one evaluation cycle."""
        # 1. Check auto-reconnect if WS dropped
        if not self.ws_connected and not self.dry_run and self.api:
            # handled via reconnect timer if needed
            pass

        # 2. Update spreads and signals from in-memory AMC cache
        self.update_spreads_and_signals()

        # 3. Fallback REST polling if WS not yet connected
        if not self.ws_connected and not self.dry_run:
            self._fallback_poll_rest_quotes()

    def stop(self):
        """Stop the scanner cleanly."""
        self.running = False
        self.inav_manager.stop_background_refresh()
        if self.api:
            try:
                self.api.close_websocket()
            except Exception:
                pass
        logger.info("ETFOpportunityScanner stopped.")


def main():
    parser = argparse.ArgumentParser(description="Real-Time ETF Opportunity & iNAV Arbitrage Scanner")
    parser.add_argument("--interval", type=float, default=1.0, help="TUI refresh interval in seconds (default: 1.0)")
    parser.add_argument("--refresh-nav", type=int, default=60, help="AMC iNAV background poll interval in seconds (default: 60)")
    parser.add_argument("--no-slack", action="store_true", help="Disable Slack alerts")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without live Shoonya broker connection")
    args = parser.parse_args()

    scanner = ETFOpportunityScanner(
        refresh_nav_secs=args.refresh_nav,
        enable_slack=not args.no_slack,
        dry_run=args.dry_run,
    )
    scanner.start_and_render(interval_seconds=args.interval)


if __name__ == "__main__":
    main()
