"""
src/agents/signal_sources.py
─────────────────────────────
Strategy pattern for signal collection.

Each SignalSource encapsulates one pillar of the composite signal.
The aggregator treats them as interchangeable — register a new source,
it flows through automatically without touching the orchestrator.

Architecture
────────────
  SignalSource (ABC)           — scored signal (0–100 per ETF)
  AnomalySource (ABC)          — regime flag (string per ETF)

  MacroSignalSource            → macro_event_scanner (HTTP)
  SentimentSignalSource        → news_articles (DB)
  ValuationSignalSource        → inav_snapshots (DB, via repo)
  FlowSignalSource             → fii_dii_flows (DB)
  MLSignalSource               → ml_predictions (DB)

  GARCHAnomalySource           → daily_prices → GARCH + IF pipeline

Adding a new source
───────────────────
  1. Subclass SignalSource (or AnomalySource)
  2. Set name and weight
  3. Implement collect()
  4. Append to SIGNAL_SOURCES (or ANOMALY_SOURCE) in signal_aggregator.py

The aggregator never needs to change.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)

# Single source of truth for the ETF universe covered by all signal sources.
# Imported by signal_aggregator — do not import SIGNAL_ETFS from there.
SIGNAL_ETFS: list[str] = [
    "GOLDBEES", "NIFTYBEES", "BANKBEES", "ITBEES", "JUNIORBEES",
    "SILVERBEES", "CPSEETF", "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES",
    "MON100", "MAFANG", "HNGSNGBEES", "AUTOBEES", "PHARMABEES",
    "PSUBNKBEES", "MID150BEES", "SMALL250",
]

_NEUTRAL = {etf: 50.0 for etf in SIGNAL_ETFS}
_NORMAL  = {etf: "Normal" for etf in SIGNAL_ETFS}


# ── Base interfaces ───────────────────────────────────────────────────────────

class SignalSource(ABC):
    """
    Produces a 0–100 score per ETF for one signal pillar.

    Scores:
      100 = strongly bullish
       50 = neutral / no signal
        0 = strongly bearish
    """
    name:   str
    weight: float

    @abstractmethod
    def collect(self, repo) -> dict[str, float]:
        """
        Returns {etf_symbol: score} for all SIGNAL_ETFS.
        Must never raise — return neutral dict on any error.

        Parameters
        ----------
        repo : MarketDataRepository
        """

    def neutral(self) -> dict[str, float]:
        return dict(_NEUTRAL)


class AnomalySource(ABC):
    """Produces a regime-flag string per ETF."""

    @abstractmethod
    def collect(self, repo) -> dict[str, str]:
        """Returns {etf_symbol: regime_label}. Never raises."""

    def normal(self) -> dict[str, str]:
        return dict(_NORMAL)


# ── Concrete signal sources ───────────────────────────────────────────────────

class MacroSignalSource(SignalSource):
    """
    Runs the macro event scanner (GNews + Yahoo RSS) and normalises
    the per-ETF net score to 0–100.

    Net signal range ≈ −8 to +8 (article-count × theme weight).
    Linear map: −8 → 0, 0 → 50, +8 → 100.

    Fundamentals overlay: World Bank / IMF WEO data (from market_data.macro_indicators)
    adds a bounded ±5 adjustment to India-relevant ETFs based on GDP growth, CPI,
    and current account balance.  Degrades gracefully if the table is empty.
    """
    name   = "macro"
    weight = 0.25

    # ETF buckets for fundamentals adjustment
    _INDIA_EQUITY = frozenset({
        "NIFTYBEES", "BANKBEES", "JUNIORBEES", "CPSEETF",
        "AUTOBEES", "PHARMABEES", "PSUBNKBEES", "MID150BEES", "SMALL250",
    })
    _GOLD_ETFS = frozenset({"GOLDBEES", "SILVERBEES"})
    _BOND_ETFS = frozenset({"GILT5YBEES", "LIQUIDBEES", "LIQUIDCASE"})

    @staticmethod
    def _fundamentals_delta(mf, etf: str) -> float:
        """
        Return a score delta (clamped to ±5) based on India annual macro fundamentals.
        Applied additively to the news-derived 0-100 score before final clamping.
        Rules use India structural benchmarks: GDP trend ~6.5%, CPI target 4%, CA -2% GDP.
        """
        from src.tools.macro_event_scanner import MacroFundamentals
        delta = 0.0
        is_equity = etf in MacroSignalSource._INDIA_EQUITY
        is_gold   = etf in MacroSignalSource._GOLD_ETFS

        if mf.gdp_growth_pct is not None and is_equity:
            if mf.gdp_growth_pct >= 7.0:
                delta += 5.0
            elif mf.gdp_growth_pct >= 6.0:
                delta += 2.5
            elif mf.gdp_growth_pct < 5.0:
                delta -= 5.0
            elif mf.gdp_growth_pct < 6.0:
                delta -= 2.5

        if mf.cpi_pct is not None:
            if is_gold:
                # Elevated CPI → real-return erosion → gold inflation hedge bid
                if mf.cpi_pct > 6.0:
                    delta += 5.0
                elif mf.cpi_pct > 5.0:
                    delta += 2.5
            elif is_equity:
                # High inflation → RBI rate-hike risk → equity premium compression
                if mf.cpi_pct > 6.0:
                    delta -= 3.0
                elif mf.cpi_pct > 5.0:
                    delta -= 1.5

        if mf.ca_balance_pct is not None and is_equity:
            # Wide CA deficit → INR pressure → imported inflation → equity headwind
            if mf.ca_balance_pct < -3.0:
                delta -= 3.0
            elif mf.ca_balance_pct < -2.0:
                delta -= 1.5

        return max(-5.0, min(5.0, delta))

    def collect(self, repo) -> dict[str, float]:
        try:
            from src.tools.macro_event_scanner import scan_macro_events
            report = scan_macro_events(max_per_theme=3)
            mf     = report.quant.macro_fundamentals
            scores = {}
            for etf in SIGNAL_ETFS:
                net     = report.etf_net_signal.get(etf, 0)
                clamped = max(-8, min(8, net))
                base    = 50 + (clamped / 8) * 50
                delta   = self._fundamentals_delta(mf, etf)
                scores[etf] = round(max(0.0, min(100.0, base + delta)), 1)
            log.info(
                "Macro: %d themes, %d ETF signals, fundamentals=%s",
                len(report.themes_detected), len(scores),
                "ok" if mf.gdp_growth_pct is not None else "unavailable",
            )
            return scores
        except Exception as exc:
            log.warning("MacroSignalSource failed: %s", exc)
            return self.neutral()


class SentimentSignalSource(SignalSource):
    """
    Positive / negative article ratio from news_articles (last 7 days).
    No articles → neutral 50.
    """
    name   = "sentiment"
    weight = 0.15

    def collect(self, repo) -> dict[str, float]:
        try:
            rows = repo.news_sentiment_rows(days=7)
            pos: dict[str, int] = {}
            neg: dict[str, int] = {}
            for etfs_str, sentiment in rows:
                for etf in etfs_str.split(","):
                    etf = etf.strip()
                    if etf in SIGNAL_ETFS:
                        if sentiment == "POSITIVE":
                            pos[etf] = pos.get(etf, 0) + 1
                        elif sentiment == "NEGATIVE":
                            neg[etf] = neg.get(etf, 0) + 1
            scores = {}
            for etf in SIGNAL_ETFS:
                p, n  = pos.get(etf, 0), neg.get(etf, 0)
                total = p + n
                scores[etf] = round((p / total) * 100, 1) if total else 50.0
            log.info("Sentiment: %d news rows processed", len(rows))
            return scores
        except Exception as exc:
            log.warning("SentimentSignalSource failed: %s", exc)
            return self.neutral()


class ValuationSignalSource(SignalSource):
    """
    iNAV premium/discount Z-score from inav_snapshots.
    Discount (low Z) → high score (buying opportunity).
    Z ≤ −2 → 100, Z ≥ +2 → 0.
    """
    name   = "valuation"
    weight = 0.15

    def collect(self, repo) -> dict[str, float]:
        try:
            from src.tools.domestic_etf_scanner import scan_domestic_etfs
            with repo._pool.get_client() as cl:
                etf_data = scan_domestic_etfs(cl, symbols=SIGNAL_ETFS, lookback_days=30)
            scores = {}
            for item in etf_data:
                sym = item.get("symbol", "")
                z   = item.get("z_score")
                if sym in SIGNAL_ETFS and z is not None:
                    clamped    = max(-2, min(2, z))
                    scores[sym] = round(50 - (clamped / 2) * 50, 1)
            for etf in SIGNAL_ETFS:
                scores.setdefault(etf, 50.0)
            n_scored = sum(1 for v in scores.values() if v != 50.0)
            log.info("Valuation: %d ETFs with Z-scores", n_scored)
            return scores
        except Exception as exc:
            log.warning("ValuationSignalSource failed: %s", exc)
            return self.neutral()


class FlowSignalSource(SignalSource):
    """
    FII + DII combined 5-day net flow → equity score.
    Equity ETFs: direct correlation.
    Safe-haven ETFs (gold, liquid): inverse.
    International ETFs: neutral.
    """
    name   = "flow"
    weight = 0.25  # 0.15 base + 0.10 extra allocated in aggregator

    _EQUITY_ETFS = {"NIFTYBEES", "BANKBEES", "ITBEES", "JUNIORBEES", "CPSEETF",
                    "AUTOBEES", "PHARMABEES", "PSUBNKBEES", "MID150BEES", "SMALL250"}
    _HAVEN_ETFS  = {"GOLDBEES", "SILVERBEES", "LIQUIDBEES", "LIQUIDCASE", "GILT5YBEES"}
    _INTL_ETFS   = {"MON100", "MAFANG", "HNGSNGBEES"}

    def collect(self, repo) -> dict[str, float]:
        try:
            fii_5d, dii_5d = repo.fii_dii_5d()
            net         = fii_5d + dii_5d
            clamped     = max(-15000, min(15000, net))
            eq_score    = round(50 + (clamped / 15000) * 50, 1)
            log.info("Flow: FII 5d=%.0f Cr, DII 5d=%.0f Cr, eq_score=%.1f",
                     fii_5d, dii_5d, eq_score)
            scores = {}
            for etf in SIGNAL_ETFS:
                if etf in self._EQUITY_ETFS:
                    scores[etf] = eq_score
                elif etf in self._HAVEN_ETFS:
                    scores[etf] = round(100 - eq_score, 1)
                else:
                    scores[etf] = 50.0
            return scores
        except Exception as exc:
            log.warning("FlowSignalSource failed: %s", exc)
            return self.neutral()


class MLSignalSource(SignalSource):
    """
    Latest GOLDBEES LightGBM prediction from ml_predictions.
    Expected return mapped to 0–100: −3% → 0, 0 → 50, +3% → 100.
    Other ETFs neutral until multi-ETF ML is implemented.
    """
    name   = "ml"
    weight = 0.15

    def collect(self, repo) -> dict[str, float]:
        try:
            pred = repo.latest_ml_prediction()
            scores = self.neutral()
            if pred:
                ret     = pred["expected_return_pct"]
                clamped = max(-3, min(3, ret))
                scores["GOLDBEES"] = round(50 + (clamped / 3) * 50, 1)
                log.info("ML: GOLDBEES pred=%.2f%% → score=%.1f", ret, scores["GOLDBEES"])
            return scores
        except Exception as exc:
            log.warning("MLSignalSource failed: %s", exc)
            return self.neutral()


# ── Anomaly source ────────────────────────────────────────────────────────────

class GARCHAnomalySource(AnomalySource):
    """
    GARCH(1,1) + Isolation Forest regime detection on GOLDBEES OHLCV.
    Returns regime label string per ETF (other ETFs always 'Normal').
    """

    def collect(self, repo) -> dict[str, str]:
        flags = self.normal()
        try:
            import pandas as pd
            df = repo.ohlcv("GOLDBEES", "etfs")
            if len(df) < 60:
                return flags
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            from src.ml.anomaly import run_composite_anomaly
            _, df_flagged, _ = run_composite_anomaly(df, z_threshold=3.0)
            if not df_flagged.empty:
                flags["GOLDBEES"] = str(df_flagged.iloc[-1].get("regime", "Normal"))
            
            # Calculate Anomaly Density Report
            from datetime import datetime
            now = pd.Timestamp(datetime.now().date())
            densities = {}
            for label, days in [("30D", 30), ("90D", 90), ("1Y", 365), ("Lifetime", None)]:
                if days is not None:
                    cutoff = now - pd.Timedelta(days=days)
                    sub_df = df[df["trade_date"] >= cutoff]
                    sub_flagged = df_flagged[df_flagged["trade_date"] >= cutoff] if not df_flagged.empty else pd.DataFrame()
                else:
                    sub_df = df
                    sub_flagged = df_flagged
                
                n_total = len(sub_df)
                n_flag = len(sub_flagged)
                pct = (n_flag / n_total * 100) if n_total > 0 else 0.0
                densities[label] = (pct, n_flag, n_total)

            log.info("Anomaly: GOLDBEES regime=%s", flags["GOLDBEES"])
            log.info("GOLDBEES Anomaly Density Report:")
            log.info("  Lifetime: %.2f%% (%d/%d)", densities["Lifetime"][0], densities["Lifetime"][1], densities["Lifetime"][2])
            log.info("  1Y:       %.2f%% (%d/%d)", densities["1Y"][0], densities["1Y"][1], densities["1Y"][2])
            log.info("  90D:      %.2f%% (%d/%d)", densities["90D"][0], densities["90D"][1], densities["90D"][2])
            log.info("  30D:      %.2f%% (%d/%d)", densities["30D"][0], densities["30D"][1], densities["30D"][2])
        except Exception as exc:
            log.warning("GARCHAnomalySource failed: %s", exc)
        return flags
