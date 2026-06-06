import pytest
from datetime import date, datetime
import pandas as pd
import numpy as np

from src.ml.correlation import (
    CandidateEvent,
    CorrelationFinding,
    CorrelationService,
    EventType,
    PreEventLeakStrategy,
    PostMacroShockStrategy,
    CrossAssetCoMovementStrategy,
)

def test_candidate_event_and_finding_structures():
    event = CandidateEvent(
        trade_date=date(2026, 6, 1),
        event_type=EventType.COMPANY_FILING,
        label="SPLIT (1:2)",
        description="Stock Split Announcement",
        metadata={"ratio": "1:2"}
    )
    assert event.trade_date == date(2026, 6, 1)
    assert event.event_type == EventType.COMPANY_FILING
    assert event.label == "SPLIT (1:2)"
    assert event.metadata["ratio"] == "1:2"

    finding = CorrelationFinding(
        anomaly_date=date(2026, 5, 28),
        event=event,
        strategy_name="Pre-Event Leak Detector",
        correlation_score=85.0,
        lead_lag_days=-4,
        confidence="HIGH",
        explanation="Detected front-running before split announcement"
    )
    assert finding.anomaly_date == date(2026, 5, 28)
    assert finding.lead_lag_days == -4
    assert finding.confidence == "HIGH"


def test_pre_event_leak_strategy_with_mock_data():
    # Construct mock price data spanning 30 days
    dates = pd.date_range(start="2026-05-01", periods=30, freq="D")
    df_ohlcv = pd.DataFrame({
        "trade_date": dates,
        "open": np.linspace(100, 110, 30),
        "high": np.linspace(101, 111, 30),
        "low": np.linspace(99, 109, 30),
        "close": np.linspace(100, 110, 30),
        "volume": [1000.0] * 30,
    })

    # Simulate insider leak on days 15-19 (indices 15-19) before a split announcement on day 20 (index 20)
    # The ex-date of the event is 2026-05-21 (day 20)
    # Insider leak: run-up in price + spike in volume
    # Let's override indices 15-19
    for idx in range(15, 20):
        df_ohlcv.loc[idx, "close"] = df_ohlcv.loc[idx-1, "close"] * 1.025  # 2.5% return daily
        df_ohlcv.loc[idx, "volume"] = 3500.0  # 3.5x volume spike

    df_anomaly = pd.DataFrame({
        "trade_date": dates,
        "is_anomaly": [False] * 30,
        "garch_vol": [1.0] * 30,
    })
    # Flag index 18 as an anomaly day
    df_anomaly.loc[18, "is_anomaly"] = True
    df_anomaly.loc[15:19, "garch_vol"] = 1.6  # GARCH Vol expansion

    events = [
        CandidateEvent(
            trade_date=dates[20].date(),
            event_type=EventType.COMPANY_FILING,
            label="BONUS (1:1)",
            description="1:1 Bonus Announcement"
        )
    ]

    strategy = PreEventLeakStrategy(window_days=5, min_score=20.0)
    findings = strategy.analyze(df_ohlcv, df_anomaly, None, events)

    assert len(findings) == 1
    f = findings[0]
    assert f.event.label == "BONUS (1:1)"
    assert f.correlation_score >= 40.0
    assert f.lead_lag_days < 0  # Anomaly happened BEFORE event
    assert f.confidence in ("MODERATE", "HIGH")


def test_post_macro_shock_strategy():
    dates = pd.date_range(start="2026-05-01", periods=10, freq="D")
    df_ohlcv = pd.DataFrame({
        "trade_date": dates,
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.0] * 10,
        "volume": [1000.0] * 10,
    })
    # Price shock at index 5 (day 5)
    df_ohlcv.loc[5, "close"] = 104.5  # 4.5% daily return shock

    df_anomaly = pd.DataFrame({
        "trade_date": dates,
        "is_anomaly": [False] * 10,
        "garch_vol": [1.0] * 10,
    })
    df_anomaly.loc[5, "is_anomaly"] = True

    events = [
        CandidateEvent(
            trade_date=dates[4].date(),  # Event on day 4
            event_type=EventType.MACRO_RATE_DECISION,
            label="RBI Easing",
            description="RBI Cut Repo Rate"
        )
    ]

    strategy = PostMacroShockStrategy(window_days=3, min_return_pct=1.5)
    findings = strategy.analyze(df_ohlcv, df_anomaly, None, events)

    assert len(findings) == 1
    f = findings[0]
    assert f.event.label == "RBI Easing"
    assert f.lead_lag_days >= 0  # Anomaly happened AFTER or ON event day
    assert f.confidence == "MODERATE"


def test_correlation_service_fallback_empty_data():
    service = CorrelationService()
    findings = service.find_correlations("TICKER", pd.DataFrame())
    assert findings == []


def test_decimal_glitch_and_yield_repair():
    from src.ml.anomaly import build_features
    # 1. Test decimal glitch repair on the fly (10x and 100x shifts)
    dates = pd.date_range(start="2026-05-01", periods=10, freq="D")
    df_glitch = pd.DataFrame({
        "trade_date": dates,
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.0] * 10,
        "volume": [1000.0] * 10,
    })
    # Introduce 1-day 10x drop on day 3
    df_glitch.loc[3, ["open", "high", "low", "close"]] /= 10.0
    # Introduce 2-day 100x drop on days 6 & 7
    df_glitch.loc[6:7, ["open", "high", "low", "close"]] /= 100.0

    res = build_features(df_glitch)
    # The prices at index 3, 6, and 7 should be successfully scaled back to normal (~100)
    assert abs(res.loc[3, "close"] - 100.0) < 1.0
    assert abs(res.loc[6, "close"] - 100.0) < 1.0
    assert abs(res.loc[7, "close"] - 100.0) < 1.0

    # 2. Test yield protection for negative/zero close values
    df_yield = pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * 10,
        "high": [11.0] * 10,
        "low": [9.0] * 10,
        "close": [1.0, 1.2, -0.05, -0.04, 0.0, 1.1, 1.2, 1.3, 1.1, 1.0],  # Negative & zero values
        "volume": [1000.0] * 10,
    })
    res_yield = build_features(df_yield)
    # Assert no NaNs or Infs in daily_return (except first row) or log_return (except first row)
    assert not res_yield["daily_return"].iloc[1:].isna().any()
    assert not res_yield["log_return"].iloc[1:].isna().any()
    assert not np.isinf(res_yield["log_return"]).any()


def test_post_macro_shock_decay_filtering():
    dates = pd.date_range(start="2026-05-01", periods=10, freq="D")
    df_ohlcv = pd.DataFrame({
        "trade_date": dates,
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.0] * 10,
        "volume": [1000.0] * 10,
    })
    # Moderate price shock at index 6 (day 6): 1.6% return
    df_ohlcv.loc[6, "close"] = 101.6

    df_anomaly = pd.DataFrame({
        "trade_date": dates,
        "is_anomaly": [False] * 10,
        "garch_vol": [1.0] * 10,
    })

    # Event happens at index 2 (day 2) -> lag is 4 days to day 6
    events = [
        CandidateEvent(
            trade_date=dates[2].date(),
            event_type=EventType.MACRO_RATE_DECISION,
            label="Old Policy Event",
            description="Policy update days ago"
        )
    ]

    # With a 4-day lag: weight = exp(-4/2) = 0.1353.
    # Raw score: 1.6 * 25.0 = 40.0. Decayed score: 40.0 * 0.1353 = 5.41.
    # This falls below the 15.0 post-decay threshold and must be filtered out.
    strategy = PostMacroShockStrategy(window_days=5, min_return_pct=1.5)
    findings = strategy.analyze(df_ohlcv, df_anomaly, None, events)
    assert len(findings) == 0


def test_correlation_clustering_deduplication():
    from unittest.mock import MagicMock
    import src.ml.correlation as correlation_mod

    service = CorrelationService()

    # Create mock strategies
    strat1 = MagicMock()
    strat1.name = "Strategy A"
    strat1.analyze.return_value = [
        CorrelationFinding(
            anomaly_date=date(2026, 6, 1),
            event=CandidateEvent(date(2026, 6, 1), EventType.COMPANY_FILING, "Event A", "Description A"),
            strategy_name="Strategy A",
            correlation_score=80.0,
            lead_lag_days=0,
            confidence="HIGH",
            explanation="Trigger A"
        )
    ]

    strat2 = MagicMock()
    strat2.name = "Strategy B"
    strat2.analyze.return_value = [
        CorrelationFinding(
            anomaly_date=date(2026, 6, 1),
            event=CandidateEvent(date(2026, 6, 2), EventType.MACRO_RATE_DECISION, "Event B", "Description B"),
            strategy_name="Strategy B",
            correlation_score=50.0,
            lead_lag_days=-1,
            confidence="MODERATE",
            explanation="Trigger B"
        )
    ]

    service._strategies = [strat1, strat2]

    # Mock helpers to avoid real database/API queries
    service._fetch_symbol_news = MagicMock(return_value=[])
    service._load_corp_actions = MagicMock(return_value=None)

    import src.ml.anomaly as anomaly_mod

    original_anomaly = anomaly_mod.run_composite_anomaly
    anomaly_mod.run_composite_anomaly = MagicMock(return_value=(pd.DataFrame(), None, None))

    try:
        df_ohlcv = pd.DataFrame({
            "trade_date": pd.date_range("2026-06-01", periods=5, freq="D"),
            "open": [100.0] * 5,
            "high": [100.0] * 5,
            "low": [100.0] * 5,
            "close": [100.0] * 5,
            "volume": [1000.0] * 5
        })
        findings = service.find_correlations("MOCK", df_ohlcv)

        # Verify deduplication happened
        assert len(findings) == 1
        f = findings[0]
        assert f.anomaly_date == date(2026, 6, 1)
        assert f.strategy_name == "Strategy A"  # Selected as primary due to higher score (80 vs 50)
        assert "Trigger A" in f.explanation
        assert "Event B" in f.explanation
        assert "Strategy B" in f.explanation
    finally:
        anomaly_mod.run_composite_anomaly = original_anomaly


def test_car_benchmark_calculation():
    dates = pd.date_range(start="2026-05-01", periods=10, freq="D")
    df_ohlcv = pd.DataFrame({
        "trade_date": dates,
        "open": [100.0] * 10,
        "high": [101.0] * 10,
        "low": [99.0] * 10,
        "close": [100.0, 100.0, 100.0, 100.0, 100.0, 105.0, 105.0, 105.0, 105.0, 105.0],
        "volume": [1000.0] * 10,
    })

    # Define a benchmark with the exact same return (so CAR = 0.0)
    df_benchmark = pd.DataFrame({
        "trade_date": dates,
        "close": [10.0, 10.0, 10.0, 10.0, 10.0, 10.5, 10.5, 10.5, 10.5, 10.5]
    })

    df_anomaly = pd.DataFrame({
        "trade_date": dates,
        "is_anomaly": [False] * 10,
        "garch_vol": [1.0] * 10,
    })
    df_anomaly.loc[5, "is_anomaly"] = True

    events = [
        CandidateEvent(
            trade_date=dates[6].date(),
            event_type=EventType.COMPANY_FILING,
            label="DIVIDEND",
            description="Dividend announcement"
        )
    ]

    strategy = PreEventLeakStrategy(window_days=3, min_score=0.0)
    findings = strategy.analyze(df_ohlcv, df_anomaly, df_benchmark, events)
    assert len(findings) == 1
    # Check that CAR was computed as 0.0.
    assert "cumulative abnormal return of +0.00%" in findings[0].explanation


def test_news_quality_and_hierarchy_weights():
    service = CorrelationService()

    # 1. Company earnings news (expected quality=1.0, hierarchy=1.0 -> multiplier = 1.0)
    f_earnings = CorrelationFinding(
        anomaly_date=date(2026, 6, 1),
        event=CandidateEvent(date(2026, 6, 1), EventType.NEWS_ANNOUNCEMENT, "Q4 Earnings Results Rally", "Company Q4 profits beat estimates"),
        strategy_name="Post-Macro Shock Trigger",
        correlation_score=80.0,
        lead_lag_days=0,
        confidence="HIGH",
        explanation="Test earnings"
    )

    # 2. Opinion/speculative news (expected quality=0.10, hierarchy=1.0 -> multiplier = 0.10)
    f_opinion = CorrelationFinding(
        anomaly_date=date(2026, 6, 1),
        event=CandidateEvent(date(2026, 6, 1), EventType.NEWS_ANNOUNCEMENT, "CEO compensation could get bumped up", "Speculation about opinion target price"),
        strategy_name="Post-Macro Shock Trigger",
        correlation_score=80.0,
        lead_lag_days=0,
        confidence="HIGH",
        explanation="Test opinion"
    )

    # 3. Macro policy event (expected quality=1.0, hierarchy=0.5 -> multiplier = 0.50)
    f_macro = CorrelationFinding(
        anomaly_date=date(2026, 6, 2),
        event=CandidateEvent(date(2026, 6, 2), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut", "Fed cut rates"),
        strategy_name="Post-Macro Shock Trigger",
        correlation_score=80.0,
        lead_lag_days=0,
        confidence="HIGH",
        explanation="Test macro"
    )

    # 4. Blocked publisher (simplywall.st should get nq_weight=0.0 -> filtered out)
    f_simplywall = CorrelationFinding(
        anomaly_date=date(2026, 6, 1),
        event=CandidateEvent(date(2026, 6, 1), EventType.NEWS_ANNOUNCEMENT, "Why simplywall.st thinks MSUMI is underperforming", "Opinion article on simplywall"),
        strategy_name="Post-Macro Shock Trigger",
        correlation_score=80.0,
        lead_lag_days=0,
        confidence="HIGH",
        explanation="Test simplywall"
    )

    from unittest.mock import MagicMock
    import src.ml.anomaly as anomaly_mod

    strat = MagicMock()
    strat.name = "Mock Strat"
    strat.analyze.return_value = [f_earnings, f_opinion, f_macro, f_simplywall]

    service._strategies = [strat]
    service._fetch_symbol_news = MagicMock(return_value=[])
    service._load_corp_actions = MagicMock(return_value=None)

    original_anomaly = anomaly_mod.run_composite_anomaly
    anomaly_mod.run_composite_anomaly = MagicMock(return_value=(pd.DataFrame(), None, None))

    try:
        df_ohlcv = pd.DataFrame({
            "trade_date": pd.date_range("2026-06-01", periods=5, freq="D"),
            "open": [100.0] * 5,
            "high": [100.0] * 5,
            "low": [100.0] * 5,
            "close": [100.0] * 5,
            "volume": [1000.0] * 5
        })
        findings = service.find_correlations("MOCK", df_ohlcv)

        # Earnings finding should survive with 80.0 * 1.0 * 1.0 = 80.0 score (HIGH confidence)
        # Opinion finding should be filtered out because 80.0 * 0.10 * 1.0 = 8.0 < 15.0
        # Macro finding should survive with 80.0 * 0.5 = 40.0 score (MODERATE confidence)
        assert len(findings) == 2

        # Verify earnings finding details
        earnings_res = [x for x in findings if "Earnings" in x.event.label][0]
        assert earnings_res.correlation_score == 80.0
        assert earnings_res.confidence == "HIGH"

        # Verify macro finding details
        macro_res = [x for x in findings if "Fed" in x.event.label][0]
        assert macro_res.correlation_score == 40.0
        assert macro_res.confidence == "MODERATE"

    finally:
        anomaly_mod.run_composite_anomaly = original_anomaly


