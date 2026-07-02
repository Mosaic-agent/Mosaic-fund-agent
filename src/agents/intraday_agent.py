"""
Intraday signal agent for ETFs and stocks.

Connects to a Shoonya WebSocket for live tick data, combines it with
ClickHouse historical baselines (50-day EMA, 15-day avg volume), and
prints a read-only BUY/HOLD/WATCH signal at a configurable interval.

Momentum layer (computed in BaseIntradayAgent, shared by all subclasses):
  - Cumulative Delta:  order-flow imbalance from bid/ask classification
  - VWAP ±2σ Bands:    mean-reversion zones around intraday VWAP
  - Tick RSI (9):      momentum oscillator on 1-tick returns
  - Micro-Momentum:    EMA-9 of log returns (acceleration/deceleration)

Two subclasses handle asset-specific logic:
  - StockIntradayAgent: momentum + price/volume technicals
  - ETFIntradayAgent:   momentum + technicals + iNAV premium/discount overlay
"""

from __future__ import annotations

import math
import sys
import os
import time
import logging
import argparse
import threading
from datetime import datetime
import pandas as pd

# Ensure project root is on sys.path for standalone and programmatic use
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
from src.tools.shoonya_tools import resolve_token
from src.importer.fetchers.nse_inav_fetcher import get_latest_inav
from src.db.pool import get_pool

logger = logging.getLogger(__name__)


class BaseIntradayAgent:
    """
    Base Intraday Agent.

    Handles baseline historical price loading, WebSocket subscription,
    tick capture, VWAP calculations, momentum indicators, and the
    output loop.  Subclasses must override evaluate_signal_logic().
    """

    _MAX_HISTORY = 5_000  # cap tick history (~80 min at 1 tick/sec)
    _RSI_PERIOD = 9       # tick RSI look-back
    _MOM_SPAN = 9         # micro-momentum EMA span

    def __init__(self, symbol: str, category: str, interval_seconds: int = 5):
        self.symbol = symbol.strip().upper()
        self.category = category.lower()
        self.interval_seconds = interval_seconds
        self.db_pool = get_pool()
        self.api = None
        self.running = False

        # Live state variables
        self.live_price: float | None = None
        self.live_volume: float = 0
        self.last_signal: str | None = None
        self.ticks_count: int = 0

        # Live bid/ask for cumulative delta classification
        self.best_bid: float | None = None
        self.best_ask: float | None = None

        # Technical baselines
        self.ema50: float | None = None
        self.avg_vol_15d: float | None = None
        self.prev_close: float | None = None

        # Intraday tracking lists (guarded by _lock)
        self._lock = threading.Lock()
        self.price_history: list[float] = []
        self.volume_history: list[float] = []

        # Cumulative delta tracking
        self.cumulative_delta: float = 0.0
        self._prev_cum_volume: float | None = None  # None = first tick not yet seen
        self._tick_bid_ask: list[tuple[float, float | None, float | None]] = []
        # Each entry: (ltp_at_tick, best_bid_at_tick, best_ask_at_tick)

        # UI state variables for in-place refreshes
        self.remaining_seconds: int | None = None
        self._prev_line_count: int = 0
        self.adx_value: float = 15.0
        self.regime: str = "Mean-Reverting"

    # ── Historical baselines ──────────────────────────────────────────

    def fetch_historical_baseline(self):
        """Fetch daily price baseline metrics from ClickHouse."""
        logger.info(f"[{self.symbol}] Querying historical baseline from ClickHouse...")
        query = """
        SELECT trade_date, open, high, low, close, volume
        FROM market_data.daily_prices FINAL
        WHERE symbol = {symbol:String}
        ORDER BY trade_date DESC
        LIMIT 100
        """
        df = self.db_pool.query_df(query, parameters={"symbol": self.symbol})
        if df.empty:
            raise ValueError(f"No historical price data found for symbol: {self.symbol}")

        df = df.iloc[::-1].reset_index(drop=True)
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()

        self.prev_close = df['close'].iloc[-1]
        self.ema50 = df['ema50'].iloc[-1]
        self.avg_vol_15d = df['volume'].rolling(15).mean().iloc[-1]

        # Compute ADX and Regime
        self.adx_value, self.regime = self._calculate_adx(df)

        logger.info(
            f"[{self.symbol}] Category: {self.category} | "
            f"Prev Close: ₹{self.prev_close:.2f} | "
            f"50-day EMA: ₹{self.ema50:.2f} | "
            f"15d Avg Vol: {self.avg_vol_15d:,.0f} shares | "
            f"ADX: {self.adx_value:.1f} ({self.regime})"
        )

    def fetch_nav_reference(self):
        """Hook for fetching NAV reference (ETFs only). No-op in base."""
        pass

    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> tuple[float, str]:
        """Calculate Average Directional Index (ADX) and return (adx_value, regime)."""
        if len(df) < period * 2:
            return 15.0, "Mean-Reverting"

        # True Range
        df = df.copy()
        df['h_l'] = df['high'] - df['low']
        df['h_pc'] = (df['high'] - df['close'].shift(1)).abs()
        df['l_pc'] = (df['low'] - df['close'].shift(1)).abs()
        df['tr'] = df[['h_l', 'h_pc', 'l_pc']].max(axis=1)

        # Directional Movement
        df['up_move'] = df['high'] - df['high'].shift(1)
        df['down_move'] = df['low'].shift(1) - df['low']

        df['plus_dm'] = 0.0
        df['minus_dm'] = 0.0

        plus_mask = (df['up_move'] > df['down_move']) & (df['up_move'] > 0)
        df.loc[plus_mask, 'plus_dm'] = df.loc[plus_mask, 'up_move']

        minus_mask = (df['down_move'] > df['up_move']) & (df['down_move'] > 0)
        df.loc[minus_mask, 'minus_dm'] = df.loc[minus_mask, 'down_move']

        # Wilder's smoothing
        span = 2 * period - 1
        df['str'] = df['tr'].ewm(span=span, adjust=False).mean()
        df['splus_dm'] = df['plus_dm'].ewm(span=span, adjust=False).mean()
        df['sminus_dm'] = df['minus_dm'].ewm(span=span, adjust=False).mean()

        # DI+ and DI-
        df['plus_di'] = 100 * (df['splus_dm'] / df['str'])
        df['minus_di'] = 100 * (df['sminus_dm'] / df['str'])

        # DX and ADX
        df['dx'] = 100 * ((df['plus_di'] - df['minus_di']).abs() / (df['plus_di'] + df['minus_di']))
        df['adx'] = df['dx'].ewm(span=span, adjust=False).mean()

        adx_val = float(df['adx'].iloc[-1])
        if pd.isna(adx_val):
            adx_val = 15.0

        regime = "Trending" if adx_val >= 25 else "Mean-Reverting"
        return adx_val, regime

    def _calculate_confidence_score(
        self, price: float, volume: float, vwap: float,
        pct_from_ema: float, relative_vol: float, momentum: dict,
        premium_pct: float | None, premium_threshold: float | None, signal: str
    ) -> int:
        """Calculate weighted confidence score from technical factors (0% to 100%)."""
        # 1. EMA Trend (30% weight)
        if pct_from_ema >= 0:
            ema_score = 100 if relative_vol > 1.0 else 80
        else:
            ema_score = 60 if relative_vol > 1.2 else 30

        # 2. VWAP (20% weight)
        vwap_z = momentum.get("vwap_z", 0.0)
        if abs(vwap_z) <= 0.5:
            vwap_score = 50
        elif price > vwap:
            vwap_score = 100 if relative_vol > 1.0 else 70
        else:
            vwap_score = 60 if relative_vol > 1.2 else 30

        # 3. Delta (20% weight)
        cum_delta = momentum.get("cum_delta", 0.0)
        if pct_from_ema >= 0:
            if cum_delta > 0:
                delta_score = 100
            elif cum_delta < 0:
                delta_score = 30
            else:
                delta_score = 50
        else:
            if cum_delta > 0:
                delta_score = 80
            elif cum_delta < 0:
                delta_score = 100
            else:
                delta_score = 50

        # 4. Volume (15% weight)
        vol_score = min(100, int(relative_vol * 80))

        # 5. RSI (10% weight)
        rsi = momentum.get("rsi")
        if rsi is None:
            rsi_score = 50
        elif rsi > 75:
            rsi_score = 30 if pct_from_ema >= 0 else 40
        elif rsi < 25:
            rsi_score = 100 if pct_from_ema < 0 else 40
        else:
            rsi_score = 60

        # 6. Premium (5% weight)
        if premium_pct is not None and premium_threshold is not None:
            if premium_pct <= premium_threshold:
                premium_score = 100
            else:
                premium_score = max(20, int(100 - (premium_pct - premium_threshold) * 20))
        else:
            premium_score = 100

        weighted_score = (
            0.30 * ema_score +
            0.20 * vwap_score +
            0.20 * delta_score +
            0.15 * vol_score +
            0.10 * rsi_score +
            0.05 * premium_score
        )
        return int(round(weighted_score))

    def _generate_rationale(
        self, signal: str, pct_from_ema: float, relative_vol: float,
        vwap_z: float, cum_delta: float, premium_pct: float | None,
        premium_threshold: float | None
    ) -> str:
        """Construct a context-rich natural language rationale statement."""
        # 1. EMA Trend
        if pct_from_ema >= 0:
            ema_part = f"Price remains {pct_from_ema:.1f}% above the 50-day EMA"
        else:
            ema_part = f"Price remains {abs(pct_from_ema):.1f}% below the 50-day EMA"

        # 2. VWAP
        if abs(vwap_z) <= 0.5:
            vwap_part = "while trading near VWAP"
        elif vwap_z > 0:
            vwap_part = "while breaking above VWAP"
        else:
            vwap_part = "while trading below VWAP"

        # 3. Volume
        if relative_vol >= 1.2:
            vol_part = "on above-average volume"
        elif relative_vol <= 0.8:
            vol_part = "on below-average volume"
        else:
            vol_part = "on average volume"

        # 4. Order Flow (Delta)
        if cum_delta > 0 and relative_vol > 0.5:
            delta_part = "Order flow is positive (buyers dominating)"
        elif cum_delta < 0 and relative_vol > 0.5:
            delta_part = "Order flow is negative (sellers dominating)"
        else:
            delta_part = "Order flow is neutral"

        # 5. Premium
        premium_part = ""
        if premium_pct is not None and premium_threshold is not None:
            if premium_pct > premium_threshold:
                premium_part = f"and the ETF premium is elevated at {premium_pct:+.2f}%"
            else:
                premium_part = "and the ETF premium remains within acceptable limits"

        # 6. Conclusion
        if "BUY" in signal or "ACCUMULATE" in signal:
            conclusion = "A high-conviction intraday entry setup is present."
        elif "AVOID" in signal:
            conclusion = "Avoid entry due to premium drag."
        else:
            conclusion = "No high-conviction intraday entry is present."

        parts = [ema_part, vwap_part, vol_part + "."]
        if premium_part:
            parts.append(f"{delta_part} {premium_part}.")
        else:
            parts.append(f"{delta_part}.")
        parts.append(conclusion)

        return " ".join(parts)

    # ── WebSocket callback ────────────────────────────────────────────

    def on_feed_update(self, tick_data):
        """Websocket feed callback to capture live updates."""
        if not tick_data or tick_data.get("t") not in ("tk", "tf"):
            return

        lp = tick_data.get("lp")
        v = tick_data.get("v")
        bp1 = tick_data.get("bp1")
        sp1 = tick_data.get("sp1")

        # Update best bid/ask (these may arrive independently of lp/v)
        if bp1 is not None:
            self.best_bid = float(bp1)
        if sp1 is not None:
            self.best_ask = float(sp1)

        updated = False
        if lp is not None:
            self.live_price = float(lp)
            updated = True
        if v is not None:
            self.live_volume = float(v)
            updated = True

        if updated:
            with self._lock:
                self.ticks_count += 1
                if lp is not None and v is not None:
                    lp_f = float(lp)
                    v_f = float(v)

                    self.price_history.append(lp_f)
                    self.volume_history.append(v_f)

                    # ── Cumulative delta update ──
                    if self._prev_cum_volume is None:
                        # First tick: set baseline, don't classify
                        self._prev_cum_volume = v_f
                    vol_delta = max(0.0, v_f - self._prev_cum_volume)
                    if vol_delta > 0 and self.best_bid is not None and self.best_ask is not None:
                        mid = (self.best_bid + self.best_ask) / 2.0
                        if lp_f >= self.best_ask:
                            self.cumulative_delta += vol_delta   # buy
                        elif lp_f <= self.best_bid:
                            self.cumulative_delta -= vol_delta   # sell
                        elif lp_f > mid:
                            self.cumulative_delta += vol_delta * 0.5  # lean buy
                        else:
                            self.cumulative_delta -= vol_delta * 0.5  # lean sell
                    self._prev_cum_volume = v_f

                    # Trim to bounded window
                    if len(self.price_history) > self._MAX_HISTORY:
                        self.price_history = self.price_history[-self._MAX_HISTORY:]
                        self.volume_history = self.volume_history[-self._MAX_HISTORY:]

    # ── VWAP ──────────────────────────────────────────────────────────

    def _compute_vwap(self, prices: list[float], volumes: list[float]) -> float:
        """Compute VWAP from snapshot copies of price/volume history."""
        if not prices:
            return self.live_price or self.prev_close

        total_p_v = 0.0
        total_v = 0.0
        prev_v = 0.0

        for p, v in zip(prices, volumes):
            delta_v = max(0.0, v - prev_v)
            total_p_v += p * delta_v
            total_v += delta_v
            prev_v = v

        return total_p_v / total_v if total_v > 0 else (self.live_price or self.prev_close)

    # ── VWAP Deviation Bands (±1σ, ±2σ) ──────────────────────────────

    def _compute_vwap_bands(
        self, prices: list[float], vwap: float
    ) -> tuple[float, float, float, float]:
        """Return (sigma, vwap_z, lower_2s, upper_2s)."""
        if len(prices) < 2:
            return 0.0, 0.0, vwap, vwap

        # Standard deviation of price deviations from VWAP
        deviations = [p - vwap for p in prices]
        mean_dev = sum(deviations) / len(deviations)
        variance = sum((d - mean_dev) ** 2 for d in deviations) / len(deviations)
        sigma = math.sqrt(variance) if variance > 0 else 0.001

        current_price = prices[-1]
        vwap_z = (current_price - vwap) / sigma

        return sigma, vwap_z, vwap - 2 * sigma, vwap + 2 * sigma

    # ── Tick RSI (9-period) ───────────────────────────────────────────

    def _compute_tick_rsi(self, prices: list[float]) -> float | None:
        """Compute RSI over the last _RSI_PERIOD price changes."""
        n = self._RSI_PERIOD
        if len(prices) < n + 1:
            return None

        recent = prices[-(n + 1):]
        gains = 0.0
        losses = 0.0

        for i in range(1, len(recent)):
            change = recent[i] - recent[i - 1]
            if change > 0:
                gains += change
            else:
                losses -= change  # make positive

        if gains + losses == 0:
            return 50.0  # no movement → neutral

        avg_gain = gains / n
        avg_loss = losses / n

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ── Micro-Momentum (EMA-9 of log returns) ────────────────────────

    def _compute_micro_momentum(self, prices: list[float]) -> float | None:
        """EMA of recent log returns — positive = accelerating up."""
        n = self._MOM_SPAN
        if len(prices) < n + 1:
            return None

        recent = prices[-(n + 1):]
        log_returns = []
        for i in range(1, len(recent)):
            if recent[i - 1] > 0:
                log_returns.append(math.log(recent[i] / recent[i - 1]))

        if not log_returns:
            return None

        # Simple EMA over the log returns
        alpha = 2.0 / (len(log_returns) + 1)
        ema = log_returns[0]
        for r in log_returns[1:]:
            ema = alpha * r + (1 - alpha) * ema

        return ema * 100.0  # express as percentage

    # ── Shared EMA / volume decision tree ─────────────────────────────

    def _ema_volume_signal(self, pct_from_ema: float, relative_vol: float) -> tuple[str, str]:
        """Base technical signal from EMA position + relative volume."""
        if self.live_price < self.ema50:
            if relative_vol > 1.2:
                return (
                    "ACCUMULATE (BUYING SUPPORT)",
                    f"Price is below 50-day EMA ({pct_from_ema:+.2f}%) but heavy "
                    f"intraday volume ({relative_vol:.2f}x) suggests high accumulation/support.",
                )
            return (
                "HOLD",
                f"Price is below 50-day EMA ({pct_from_ema:+.2f}%) on average volume. "
                "No strong entry signal.",
            )
        if relative_vol > 1.0:
            return (
                "BUY",
                f"Price is above 50-day EMA ({pct_from_ema:+.2f}%) with strong "
                f"volume expansion ({relative_vol:.2f}x).",
            )
        return (
            "WATCH_LONG",
            f"Price is above 50-day EMA ({pct_from_ema:+.2f}%) but volume "
            "momentum remains weak.",
        )

    # ── Momentum summary (shared by all subclasses) ───────────────────

    def _compute_momentum_snapshot(
        self, price_hist: list[float], vwap: float, cum_delta: float
    ) -> dict:
        """Compute all four momentum indicators from a snapshot."""
        sigma, vwap_z, lower_2s, upper_2s = self._compute_vwap_bands(price_hist, vwap)
        rsi = self._compute_tick_rsi(price_hist)
        micro_mom = self._compute_micro_momentum(price_hist)

        # Delta direction label
        if cum_delta > 0:
            delta_label = "BUYERS IN CONTROL ↑"
        elif cum_delta < 0:
            delta_label = "SELLERS IN CONTROL ↓"
        else:
            delta_label = "NEUTRAL"

        # VWAP zone label
        if vwap_z >= 2.0:
            vwap_zone = "OVEREXTENDED"
        elif vwap_z <= -2.0:
            vwap_zone = "OVERSOLD"
        elif abs(vwap_z) <= 0.5:
            vwap_zone = "AT VWAP"
        else:
            vwap_zone = "NEUTRAL"

        # RSI zone label
        rsi_label = "N/A"
        if rsi is not None:
            if rsi > 75:
                rsi_label = "OVERBOUGHT"
            elif rsi < 25:
                rsi_label = "OVERSOLD"
            else:
                rsi_label = "NEUTRAL"

        # Micro-momentum label
        mom_label = "N/A"
        mom_arrow = ""
        if micro_mom is not None:
            if micro_mom > 0.01:
                mom_label = "ACCELERATING"
                mom_arrow = " ↑"
            elif micro_mom < -0.01:
                mom_label = "DECELERATING"
                mom_arrow = " ↓"
            else:
                mom_label = "FLAT"
                mom_arrow = " →"

        return {
            "cum_delta": cum_delta,
            "delta_label": delta_label,
            "sigma": sigma,
            "vwap_z": vwap_z,
            "lower_2s": lower_2s,
            "upper_2s": upper_2s,
            "vwap_zone": vwap_zone,
            "rsi": rsi,
            "rsi_label": rsi_label,
            "micro_mom": micro_mom,
            "mom_label": mom_label,
            "mom_arrow": mom_arrow,
        }

    def _format_momentum_lines(self, m: dict, vwap: float) -> list[str]:
        """Format momentum indicators as dashboard lines."""
        lines = []

        # VWAP Bands
        lines.append(
            f"  VWAP Bands        : "
            f"[₹{m['lower_2s']:.2f} — ₹{m['upper_2s']:.2f}]  "
            f"Z={m['vwap_z']:+.1f}σ ({m['vwap_zone']})"
        )

        # Cumulative Delta
        delta_fmt = f"{m['cum_delta']:+,.0f}"
        lines.append(
            f"  Cumulative Delta  : {delta_fmt} ({m['delta_label']})"
        )

        # Tick RSI
        if m["rsi"] is not None:
            lines.append(
                f"  Tick RSI ({self._RSI_PERIOD})      : "
                f"{m['rsi']:.1f} ({m['rsi_label']})"
            )
        else:
            lines.append(
                f"  Tick RSI ({self._RSI_PERIOD})      : — (warming up)"
            )

        # Micro-Momentum
        if m["micro_mom"] is not None:
            lines.append(
                f"  Micro-Momentum   : "
                f"{m['micro_mom']:+.4f}% ({m['mom_label']}{m['mom_arrow']})"
            )
        else:
            lines.append(
                f"  Micro-Momentum   : — (warming up)"
            )

        return lines

    # ── Signal evaluation (template method) ───────────────────────────

    def evaluate_signal_logic(
        self, vwap: float, pct_from_ema: float, relative_vol: float,
        momentum: dict,
    ) -> tuple[str, str, list[str]]:
        """Subclasses must override to return (signal, rationale, extra_lines)."""
        raise NotImplementedError("Subclasses must implement evaluate_signal_logic()")

    def evaluate_signal(self):
        """Evaluate real-time indicators and print the current trading signal."""
        if self.live_price is None:
            return

        # Take a thread-safe snapshot
        with self._lock:
            price = self.live_price
            volume = self.live_volume
            price_hist = list(self.price_history)
            vol_hist = list(self.volume_history)
            cum_delta = self.cumulative_delta

        vwap = self._compute_vwap(price_hist, vol_hist)
        pct_from_ema = ((price - self.ema50) / self.ema50) * 100
        relative_vol = volume / self.avg_vol_15d if self.avg_vol_15d > 0 else 0.0

        # Compute momentum snapshot
        momentum = self._compute_momentum_snapshot(price_hist, vwap, cum_delta)
        momentum_lines = self._format_momentum_lines(momentum, vwap)

        signal, rationale, extra_lines = self.evaluate_signal_logic(
            vwap, pct_from_ema, relative_vol, momentum
        )
        self.last_signal = signal

        # Override rationale with dynamic, context-rich statement
        premium_pct = getattr(self, "last_premium_pct", None)
        premium_threshold = getattr(self, "last_premium_threshold", None)
        vwap_z = momentum.get("vwap_z", 0.0)
        
        rationale = self._generate_rationale(
            signal, pct_from_ema, relative_vol, vwap_z, cum_delta,
            premium_pct, premium_threshold
        )

        # Calculate confidence score
        confidence = self._calculate_confidence_score(
            price, volume, vwap, pct_from_ema, relative_vol, momentum,
            premium_pct, premium_threshold, signal
        )

        # Build Signal Dashboard content
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_suffix = f" [{self.remaining_seconds}s remaining]" if self.remaining_seconds is not None else ""
        
        lines = []
        lines.append("=" * 70)
        lines.append(f"  [{timestamp}] INTRADAY READ-ONLY SIGNAL ({self.__class__.__name__}): {self.symbol}{time_suffix}")
        lines.append("=" * 70)
        lines.append(f"  Live Ticker Price : ₹{price:.2f}")
        lines.append(f"  50-day EMA        : ₹{self.ema50:.2f} ({pct_from_ema:+.2f}%)")
        lines.append(f"  Intraday VWAP     : ₹{vwap:.2f} (LTP is {'ABOVE' if price >= vwap else 'BELOW'} VWAP)")
        lines.append(f"  Regime            : {self.regime} (ADX: {self.adx_value:.0f})")
        for line in extra_lines:
            lines.append(line)
        for line in momentum_lines:
            lines.append(line)
        lines.append(f"  Today's Vol       : {volume:,.0f} shares ({relative_vol:.2f}x of 15d Avg)")
        lines.append("-" * 70)
        lines.append(f"  SIGNAL            : 💥 {signal}")
        lines.append(f"  CONFIDENCE        : {confidence}%")
        lines.append(f"  RATIONALE         : {rationale}")
        lines.append("=" * 70)
        
        output_str = "\n".join(lines) + "\n"
        
        import sys
        if self._prev_line_count > 0:
            # Move cursor up and clear the block
            sys.stdout.write("\033[F" * self._prev_line_count)
            sys.stdout.flush()
            
        sys.stdout.write(output_str)
        sys.stdout.flush()
        
        self._prev_line_count = len(lines)

    # ── Event loop & lifecycle ────────────────────────────────────────

    def run_loop(self):
        """Core loop to print signal checks at set intervals."""
        while self.running:
            try:
                self.evaluate_signal()
            except Exception as e:
                logger.error(f"Error in signal evaluation loop: {e}")
            time.sleep(self.interval_seconds)

    def start(self):
        """Establish WebSocket connection and start the monitor loop."""
        self.fetch_historical_baseline()
        self.fetch_nav_reference()

        self.api = get_shoonya_api()
        if not self.api:
            raise RuntimeError("Shoonya API authentication failed.")

        res = resolve_token(self.api, self.symbol)
        if not res:
            raise ValueError(f"Could not resolve token for symbol: {self.symbol}")

        token, _ = res
        logger.info(f"[{self.symbol}] Subscribing to NSE|{token}...")
        self.running = True

        self.api.start_websocket(
            order_update_callback=lambda x: None,
            subscribe_callback=self.on_feed_update,
            socket_open_callback=lambda: self.api.subscribe([f"NSE|{token}"]),
        )

        self.loop_thread = threading.Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()
        logger.info(f"[{self.symbol}] Intraday signal monitor started successfully.")

    def stop(self):
        """Stop monitoring and close websocket."""
        logger.info(f"[{self.symbol}] Stopping intraday monitor...")
        self.running = False
        if self.api:
            try:
                self.api.close_websocket()
            except Exception:
                pass


class StockIntradayAgent(BaseIntradayAgent):
    """Child class for standard stocks — momentum + price/volume technicals."""

    def evaluate_signal_logic(
        self, vwap: float, pct_from_ema: float, relative_vol: float,
        momentum: dict,
    ) -> tuple[str, str, list[str]]:
        self.last_premium_pct = None
        self.last_premium_threshold = None
        signal, rationale = self._ema_volume_signal(pct_from_ema, relative_vol)

        # Momentum overrides / refinements
        signal, rationale = self._apply_momentum_overrides(
            signal, rationale, pct_from_ema, relative_vol, momentum
        )
        return signal, rationale, []

    def _apply_momentum_overrides(
        self, signal: str, rationale: str,
        pct_from_ema: float, relative_vol: float, m: dict,
    ) -> tuple[str, str]:
        """Refine EMA/vol signal using momentum indicators."""
        rsi = m.get("rsi")
        vwap_z = m.get("vwap_z", 0.0)
        cum_delta = m.get("cum_delta", 0.0)
        micro_mom = m.get("micro_mom")

        # ── Strong buy confirmation: above EMA + buyer delta + RSI not overbought
        if signal == "BUY" and cum_delta > 0 and rsi is not None and rsi < 75:
            signal = "BUY (MOMENTUM CONFIRMED)"
            rationale += f" Order flow confirms buyers ({cum_delta:+,.0f} delta)."

        # ── Exhaustion warning: price above EMA but RSI overbought + overextended
        if signal in ("BUY", "BUY (MOMENTUM CONFIRMED)"):
            if rsi is not None and rsi > 75 and vwap_z > 1.5:
                signal = "WATCH_LONG (OVERBOUGHT)"
                rationale = (
                    f"Price is above EMA ({pct_from_ema:+.2f}%) but RSI is {rsi:.0f} "
                    f"and VWAP-Z is {vwap_z:+.1f}σ — momentum exhaustion risk."
                )

        # ── Accumulation upgrade: below EMA but strong buyer delta + RSI oversold
        if signal in ("HOLD", "ACCUMULATE (BUYING SUPPORT)"):
            if rsi is not None and rsi < 25 and cum_delta > 0:
                signal = "ACCUMULATE (OVERSOLD + BUYING)"
                rationale = (
                    f"Price is below EMA ({pct_from_ema:+.2f}%) and RSI is {rsi:.0f} "
                    f"(oversold) with positive order flow ({cum_delta:+,.0f} delta)."
                )

        # ── Divergence warning: price up but sellers dominating
        if signal in ("BUY", "BUY (MOMENTUM CONFIRMED)", "WATCH_LONG"):
            if cum_delta < 0 and micro_mom is not None and micro_mom < -0.01:
                signal = "⚠ BEARISH DIVERGENCE"
                rationale = (
                    f"Price is above EMA ({pct_from_ema:+.2f}%) but order flow is negative "
                    f"({cum_delta:+,.0f}) and momentum is decelerating ({micro_mom:+.4f}%). "
                    "Potential reversal."
                )

        return signal, rationale


class ETFIntradayAgent(BaseIntradayAgent):
    """Child class for ETFs — momentum + technicals + iNAV premium/discount overlay."""

    # International ETFs carry structural premiums due to timezone mismatch
    # between foreign market NAV and Indian trading hours.
    INTERNATIONAL_ETFS: set[str] = {
        "MAFANG",     # Mirae Asset NYSE FANG+
        "MON100",     # Motilal Oswal Nasdaq 100
        "MONQ50",     # Motilal Oswal Nasdaq Q50
        "HNGSNGBEES", # Nippon Hang Seng BeES
        "MAHKTECH",   # Mirae Asset Hang Seng TECH
        "N100",       # Nippon Nasdaq 100
    }

    # Commodity ETFs track physical gold/silver — NAV is based on previous
    # day's metal close, but COMEX/LBMA trade 24/7. Intraday premiums of
    # 1-2% are normal when overnight metal prices move.
    COMMODITY_ETFS: set[str] = {
        "GOLDBEES",    # Nippon Gold BeES
        "GOLDCASE",    # ICICI Prudential Gold ETF
        "SILVERBEES",  # Nippon Silver BeES
        "SILVERCASE",  # ICICI Prudential Silver ETF
    }

    PREMIUM_THRESHOLD_DOMESTIC = 1.5       # % — equity ETFs track live market
    PREMIUM_THRESHOLD_COMMODITY = 2.5      # % — overnight COMEX gap is normal
    PREMIUM_THRESHOLD_INTERNATIONAL = 5.0  # % — timezone structural premium

    def __init__(self, symbol: str, category: str, interval_seconds: int = 5):
        super().__init__(symbol, category, interval_seconds)
        self.declared_nav: float | None = None

        sym = symbol.strip().upper()
        if sym in self.INTERNATIONAL_ETFS:
            self.etf_type = "INTERNATIONAL"
            self.premium_threshold = self.PREMIUM_THRESHOLD_INTERNATIONAL
        elif sym in self.COMMODITY_ETFS:
            self.etf_type = "COMMODITY"
            self.premium_threshold = self.PREMIUM_THRESHOLD_COMMODITY
        else:
            self.etf_type = "DOMESTIC"
            self.premium_threshold = self.PREMIUM_THRESHOLD_DOMESTIC

    def fetch_nav_reference(self):
        logger.info(f"[{self.symbol}] Fetching latest declared NAV reference...")
        nav_data = get_latest_inav(self.symbol, store_to_db=False)
        if nav_data and nav_data.get("inav"):
            self.declared_nav = nav_data["inav"]
            logger.info(f"[{self.symbol}] Declared NAV Reference: ₹{self.declared_nav:.4f}")
        else:
            logger.warning(
                f"[{self.symbol}] Could not fetch NAV reference. "
                "iNAV premium checks will be bypassed."
            )

    def evaluate_signal_logic(
        self, vwap: float, pct_from_ema: float, relative_vol: float,
        momentum: dict,
    ) -> tuple[str, str, list[str]]:
        extra_lines: list[str] = []
        premium_pct = 0.0
        
        self.last_premium_pct = None
        self.last_premium_threshold = None

        if self.declared_nav:
            premium_pct = ((self.live_price - self.declared_nav) / self.declared_nav) * 100
            self.last_premium_pct = premium_pct
            self.last_premium_threshold = self.premium_threshold
            extra_lines.append(f"  ETF Type          : {self.etf_type} (premium threshold: {self.premium_threshold:.1f}%)")
            extra_lines.append(f"  Declared NAV      : ₹{self.declared_nav:.4f}")
            extra_lines.append(
                f"  iNAV Spread       : {premium_pct:+.2f}% "
                f"({'PREMIUM' if premium_pct > 0 else 'DISCOUNT'})"
            )

        # Premium drag override — use asset-appropriate threshold
        if self.declared_nav and premium_pct > self.premium_threshold:
            rationale = (
                f"Live LTP (₹{self.live_price:.2f}) trades at an elevated premium "
                f"of {premium_pct:.2f}% above NAV (₹{self.declared_nav:.2f}). "
                f"Exceeds {self.premium_threshold:.1f}% threshold. Avoid buying."
            )
            # Even within premium drag, note if sellers are pushing
            if momentum.get("cum_delta", 0) < 0:
                rationale += " Sellers dominating — premium may narrow."
            return "WATCH / AVOID (PREMIUM DRAG)", rationale, extra_lines

        # Discount + buyer accumulation = strong ETF-specific buy
        if self.declared_nav and premium_pct < -0.5:
            rsi = momentum.get("rsi")
            cum_delta = momentum.get("cum_delta", 0)
            if cum_delta > 0 and rsi is not None and rsi < 60:
                return (
                    "BUY (DISCOUNT + ACCUMULATION)",
                    f"ETF trades at {premium_pct:+.2f}% discount to NAV with positive "
                    f"order flow ({cum_delta:+,.0f} delta) and RSI {rsi:.0f}.",
                    extra_lines,
                )

        # Fall through to shared EMA/volume + momentum logic
        signal, rationale = self._ema_volume_signal(pct_from_ema, relative_vol)
        signal, rationale = StockIntradayAgent._apply_momentum_overrides(
            self, signal, rationale, pct_from_ema, relative_vol, momentum
        )
        return signal, rationale, extra_lines


def create_intraday_agent(symbol: str, interval_seconds: int = 5) -> BaseIntradayAgent:
    """Factory: resolve symbol category from ClickHouse, return correct subclass."""
    db_pool = get_pool()
    symbol_upper = symbol.strip().upper()

    query = """
    SELECT DISTINCT category
    FROM market_data.daily_prices FINAL
    WHERE symbol = {symbol:String}
    """
    df = db_pool.query_df(query, parameters={"symbol": symbol_upper})
    category = "stocks"
    if not df.empty:
        category = str(df['category'].iloc[0]).lower()

    if category == "etfs":
        return ETFIntradayAgent(symbol_upper, category, interval_seconds)
    return StockIntradayAgent(symbol_upper, category, interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Read-Only Intraday Agent for ETF/Stock signals.")
    parser.add_argument("--symbol", type=str, default="GOLDBEES", help="NSE ETF/Stock symbol to track.")
    parser.add_argument("--interval", type=int, default=10, help="Interval in seconds between signal prints.")
    parser.add_argument("--duration", type=int, default=30, help="Total execution duration in seconds.")
    args, unknown_args = parser.parse_known_args()
    
    duration = args.duration
    # Parse positional duration from unknown args (e.g. 1 min, 60s, 1m, 60)
    if unknown_args:
        arg_str = " ".join(unknown_args).lower()
        import re
        
        match_min = re.search(r"(\d+(?:\.\d+)?)\s*(?:m|min|minute|minutes)\b", arg_str)
        match_sec = re.search(r"(\d+(?:\.\d+)?)\s*(?:s|sec|second|seconds)\b", arg_str)
        
        if match_min:
            try:
                duration = int(float(match_min.group(1)) * 60)
            except ValueError:
                pass
        elif match_sec:
            try:
                duration = int(float(match_sec.group(1)))
            except ValueError:
                pass
        else:
            match_num = re.search(r"^(\d+)$", unknown_args[0])
            if match_num:
                try:
                    duration = int(match_num.group(1))
                except ValueError:
                    pass

    # Instantiate correct subclass via factory function
    agent = create_intraday_agent(args.symbol, interval_seconds=args.interval)
    try:
        agent.remaining_seconds = duration
        agent.start()
        for remaining in range(duration, 0, -1):
            agent.remaining_seconds = remaining
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping agent...")
    finally:
        agent.stop()
