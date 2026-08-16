"""
tests/test_whale_accumulation_scanner.py
──────────────────────────────────────────
Unit tests for the Cross-AMC Whale Accumulation Scanner, including the
opt-in technical-confirmation layer (RSI / drawdown / volume surge).
"""

import unittest
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

from src.scripts.portfolio.whale_accumulation_scanner import (
    run_whale_scan,
    _get_amc_group,
    _is_debt_instrument,
    _is_passive,
    _normalize_security_name,
)

MODULE = "src.scripts.portfolio.whale_accumulation_scanner"

RELIANCE_ISIN = "INE002A01018"


def _holdings_fixture() -> pd.DataFrame:
    """
    Two calendar months (2026-05-01 prev, 2026-06-01 latest) across three
    distinct AMC groups (HDFC, KOTAK, SBI):
      - Reliance Industries: held by all 3 AMCs, weight rising each month
        → should surface as a consensus accumulator (num_amcs=3).
      - Tata Elxsi: zero weight in May, entered by HDFC + KOTAK in June
        → should surface as a fresh ("zero-to-hero") entry (num_amcs=2).
      - NTPC: held by HDFC only → should be filtered out at min_amcs=2.
    """
    rows = [
        # fund_name,          as_of_month,   security_name,             pct_of_nav, market_value_cr, isin
        ("HDFC_FLEXI_CAP",   "2026-05-01", "Reliance Industries Ltd.", 2.0, 100.0, RELIANCE_ISIN),
        ("HDFC_FLEXI_CAP",   "2026-06-01", "Reliance Industries Ltd.", 2.5, 130.0, RELIANCE_ISIN),
        ("KOTAK_FLEXI_CAP",  "2026-05-01", "Reliance Industries Ltd.", 1.5, 60.0,  RELIANCE_ISIN),
        ("KOTAK_FLEXI_CAP",  "2026-06-01", "Reliance Industries Ltd.", 2.0, 90.0,  RELIANCE_ISIN),
        ("SBI_BLUECHIP",     "2026-05-01", "Reliance Industries Ltd.", 1.0, 40.0,  RELIANCE_ISIN),
        ("SBI_BLUECHIP",     "2026-06-01", "Reliance Industries Ltd.", 1.5, 70.0,  RELIANCE_ISIN),
        # Placeholder/synthetic ISIN (real convention used elsewhere in this codebase
        # for unlisted/non-tradeable lines) — keeps this security out of the
        # technical-confirmation isin_map so the with_technicals test stays a
        # single-ticker yfinance mock instead of needing a MultiIndex fixture.
        ("HDFC_FLEXI_CAP",   "2026-06-01", "Tata Elxsi Ltd.",          1.0, 45.0,  "PH_670A01012"),
        ("KOTAK_FLEXI_CAP",  "2026-06-01", "Tata Elxsi Ltd.",          0.8, 30.0,  "PH_670A01012"),
        ("HDFC_FLEXI_CAP",   "2026-05-01", "NTPC Ltd.",                3.0, 120.0, "INE733E01010"),
        ("HDFC_FLEXI_CAP",   "2026-06-01", "NTPC Ltd.",                3.2, 125.0, "INE733E01010"),
        # Same real stock as "Reliance Industries Ltd." above, but spelled with
        # "Limited" — the newer-AMC convention. Should MERGE into the same
        # consensus row (num_amcs=4), not appear as a separate security.
        # Trailing period matches the "Reliance Industries Ltd." spelling used by
        # the other AMCs above exactly once "Limited" -> "Ltd" is substituted.
        ("Axis Flexi Cap Fund", "2026-05-01", "Reliance Industries Limited.", 1.2, 50.0, RELIANCE_ISIN),
        ("Axis Flexi Cap Fund", "2026-06-01", "Reliance Industries Limited.", 1.8, 80.0, RELIANCE_ISIN),
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "fund_name", "as_of_month", "security_name",
            "pct_of_nav", "market_value_cr", "isin",
        ],
    )
    df["asset_type"] = "equity"
    return df


def _query_df_side_effect(sql, *args, **kwargs):
    if "amfi_category_flows" in sql:
        return pd.DataFrame()
    return _holdings_fixture()


def _make_price_series(n=60, start=100.0, trend=-0.35, vol_spike_at=-1):
    """n daily closes trending by `trend` fraction total, with a volume spike."""
    closes = np.linspace(start, start * (1 + trend), n)
    volumes = np.full(n, 100_000.0)
    if vol_spike_at is not None:
        volumes[vol_spike_at] = 500_000.0
    idx = pd.date_range("2026-01-01", periods=n, freq="D")
    return pd.DataFrame({"Close": closes, "Volume": volumes}, index=idx)


class TestWhaleAccumulationScan(unittest.TestCase):

    @patch(f"{MODULE}.query_df", side_effect=_query_df_side_effect)
    @patch("yfinance.download")
    def test_run_whale_scan_basic_no_technicals(self, mock_yf_download, mock_query_df):
        results = run_whale_scan(amc="all", lookback_months=1, min_amcs=2)

        self.assertNotIn("error", results)
        self.assertFalse(results["with_technicals"])
        mock_yf_download.assert_not_called()

        acc_by_name = {r["security_name"]: r for r in results["top_accumulators"]}
        self.assertIn("Reliance Industries Ltd.", acc_by_name)
        reliance = acc_by_name["Reliance Industries Ltd."]
        # 4, not 3: AXIS holds the same stock spelled "Reliance Industries Limited"
        # in the fixture — proves the Ltd/Limited normalizer merges it in rather
        # than letting it appear as a separate, uncounted security.
        self.assertEqual(reliance["num_amcs"], 4)
        self.assertIn("AXIS", reliance["amcs"])
        self.assertGreater(reliance["consensus_score"], 0)
        self.assertNotIn("opportunity_score", reliance)

        # NTPC held by only 1 AMC — filtered out at min_amcs=2
        self.assertNotIn("NTPC Ltd.", acc_by_name)

        fresh_by_name = {r["security_name"]: r for r in results["fresh_entries"]}
        self.assertIn("Tata Elxsi Ltd.", fresh_by_name)
        self.assertEqual(fresh_by_name["Tata Elxsi Ltd."]["num_amcs"], 2)

    @patch(f"{MODULE}.query_df", side_effect=_query_df_side_effect)
    @patch("yfinance.download")
    def test_run_whale_scan_with_technicals(self, mock_yf_download, mock_query_df):
        mock_yf_download.return_value = _make_price_series()

        results = run_whale_scan(amc="all", lookback_months=1, min_amcs=2, with_technicals=True)

        self.assertTrue(results["with_technicals"])
        mock_yf_download.assert_called_once()

        acc_by_name = {r["security_name"]: r for r in results["top_accumulators"]}
        reliance = acc_by_name["Reliance Industries Ltd."]
        self.assertIsNotNone(reliance["rsi"])
        self.assertIsNotNone(reliance["drawdown_pct"])
        self.assertIsNotNone(reliance["volume_surge"])
        self.assertLess(reliance["drawdown_pct"], 0)  # downtrend fixture → negative drawdown
        self.assertIsInstance(reliance["opportunity_score"], float)
        self.assertGreaterEqual(reliance["opportunity_score"], 0.0)
        self.assertLessEqual(reliance["opportunity_score"], 100.0)

    @patch(f"{MODULE}.query_df", side_effect=_query_df_side_effect)
    @patch("yfinance.download", side_effect=Exception("network down"))
    def test_technical_confirmation_download_failure_degrades_gracefully(
        self, mock_yf_download, mock_query_df
    ):
        results = run_whale_scan(amc="all", lookback_months=1, min_amcs=2, with_technicals=True)

        self.assertNotIn("error", results)
        acc_by_name = {r["security_name"]: r for r in results["top_accumulators"]}
        reliance = acc_by_name["Reliance Industries Ltd."]
        self.assertIsNone(reliance["rsi"])
        self.assertIsNone(reliance["drawdown_pct"])
        # neutral tech_score (50.0) still yields a valid blended opportunity_score
        self.assertIsInstance(reliance["opportunity_score"], float)


class TestAmcGroupingAndFilters(unittest.TestCase):
    """
    Direct unit coverage for the three pure helper functions that a stale,
    unmerged branch (fix/whale-scanner-amc-grouping-and-debt-filter) targeted.
    Their fixes are already present on main — these tests lock that in.
    """

    def test_get_amc_group_generic_brand_token(self):
        # Previously a hardcoded whitelist collapsed non-listed AMCs into "OTHER".
        self.assertEqual(_get_amc_group("HDFC_FLEXI_CAP"), "HDFC")
        self.assertEqual(_get_amc_group("KOTAK_FLEXI_CAP"), "KOTAK")
        self.assertEqual(_get_amc_group("SBI_BLUECHIP"), "SBI")

    def test_get_amc_group_reliance_folds_into_nippon(self):
        self.assertEqual(_get_amc_group("RELIANCE_MULTI_ASSET_ALLOCATION_FUND"), "NIPPON")

    def test_get_amc_group_human_readable_fund_names(self):
        # Abakkus/Axis/Canara Robeco/Helios/Invesco/Mirae Asset/Motilal Oswal ship
        # space-separated fund_names with no underscore. A prior version returned
        # the ENTIRE uppercased name here, bucketing every such fund alone and
        # excluding all 7 of these AMCs from every cross-AMC consensus scan.
        self.assertEqual(_get_amc_group("Axis Flexi Cap Fund"), "AXIS")
        self.assertEqual(_get_amc_group("Abakkus Small Cap Fund"), "ABAKKUS")
        self.assertEqual(_get_amc_group("Canara Robeco Mid Cap Fund"), "CANARA")
        self.assertEqual(_get_amc_group("Helios Flexi Cap Fund"), "HELIOS")
        self.assertEqual(_get_amc_group("Invesco India Flexi Cap Fund"), "INVESCO")
        self.assertEqual(_get_amc_group("Mirae Asset Large Cap Fund"), "MIRAE")
        self.assertEqual(_get_amc_group("Motilal Oswal Multicap Fund"), "MOTILAL")

    def test_is_debt_instrument(self):
        self.assertTrue(_is_debt_instrument("7.18% Govt Stock 2033"))
        self.assertTrue(_is_debt_instrument("364 Days T-Bill (12/03/2027)"))
        self.assertFalse(_is_debt_instrument("Reliance Industries Ltd."))

    def test_is_passive(self):
        self.assertTrue(_is_passive("HDFC_NIFTY_50_INDEX_FUND"))
        self.assertTrue(_is_passive("NIPPON_CPSE_ETF"))
        self.assertFalse(_is_passive("HDFC_FLEXI_CAP"))

    def test_normalize_security_name_ltd_vs_limited(self):
        # Older BRAND_SCHEME importers spell "Ltd"; newer AMFI-sourced importers
        # (Abakkus/Axis/Canara Robeco/Helios/Invesco/Mirae Asset/Motilal Oswal)
        # spell "Limited" for the exact same companies.
        self.assertEqual(_normalize_security_name("Reliance Industries Limited"), "Reliance Industries Ltd")
        self.assertEqual(_normalize_security_name("Reliance Industries Ltd"), "Reliance Industries Ltd")

    def test_normalize_security_name_leaves_distinct_instruments_distinct(self):
        # Trailing derivative/rights/partly-paid/expiry-dated suffixes denote a
        # genuinely different instrument — must NOT collapse onto the plain equity.
        self.assertNotEqual(
            _normalize_security_name("Reliance Industries Limited Apr25"),
            _normalize_security_name("Reliance Industries Limited"),
        )
        self.assertNotEqual(
            _normalize_security_name("Reliance Industries Limited - Partly Paid Up"),
            _normalize_security_name("Reliance Industries Limited"),
        )


if __name__ == "__main__":
    unittest.main()
