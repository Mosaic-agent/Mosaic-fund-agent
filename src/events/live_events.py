"""
src/events/live_events.py
──────────────────────────
Event fired by the standalone live monitor (src/agents/live_monitor.py) when a
5-minute bar for a watched symbol trips a price/volume anomaly threshold.

Kept separate from src/events/bus.py's DataImportedEvent / AnomalyDetectedEvent:
those are EOD/import-batch shaped (fire a few times a day across all symbols);
a LiveAlertEvent can in principle fire many times a minute across N watched
symbols during market hours, and carries different fields (bar timestamp,
numeric z-score, price/volume) that don't map onto the daily "regime" schema.

Fired via the same EventBus singleton:
    from src.events.bus import get_event_bus
    get_event_bus().publish(LiveAlertEvent(symbol="NIFTY", ...))
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class LiveAlertEvent:
    """
    Fired by LiveBarBuilder when a closed 5-min bar's robust z-score crosses
    settings.live_monitor_zscore_threshold on price or volume.

      symbol              : NSE trading symbol (or index name, e.g. "NIFTY")
      timestamp           : bar close time, IST
      alert_type          : "price_break" | "volume_spike" (both may fire for the same bar
                             as two separate events)
      zscore              : robust z-score value that tripped the threshold
      price               : bar close price
      volume              : bar traded volume (delta of Shoonya's cumulative tick volume)
      baseline_avg_volume : rolling-window median volume at the time of the bar
    """
    event_type:          str      = "live.alert"
    symbol:              str      = ""
    timestamp:           datetime = field(default_factory=datetime.now)
    alert_type:          str      = ""
    zscore:              float    = 0.0
    price:               float    = 0.0
    volume:              float    = 0.0
    baseline_avg_volume: float    = 0.0
