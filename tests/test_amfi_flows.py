"""
tests/test_amfi_flows.py
────────────────────────
Unit tests for AMFI Category Flows fetcher and subcategory normalisation logic.
"""

import unittest
from datetime import date
from src.importer.fetchers.amfi_flows_fetcher import (
    _normalize_subcategory,
    _parse_amount,
    _month_dates,
    _deduplicate,
)


class TestAmfiFlows(unittest.TestCase):

    def test_normalize_subcategory_exact(self):
        self.assertEqual(_normalize_subcategory("Small Cap Fund"), "Equity")
        self.assertEqual(_normalize_subcategory("Large & Mid Cap Fund"), "Equity")
        self.assertEqual(_normalize_subcategory("Liquid Fund"), "Debt")
        self.assertEqual(_normalize_subcategory("Arbitrage Fund"), "Hybrid")
        self.assertEqual(_normalize_subcategory("Index Funds"), "Passive")

    def test_normalize_subcategory_fuzzy(self):
        self.assertEqual(_normalize_subcategory("Special Small Cap Scheme"), "Equity")
        self.assertEqual(_normalize_subcategory("Corporate Bond Institutional"), "Debt")
        self.assertEqual(_normalize_subcategory("Balanced Advantage Plan"), "Hybrid")
        self.assertEqual(_normalize_subcategory("Unknown Category X"), "Other")

    def test_parse_amount(self):
        self.assertEqual(_parse_amount("12,345.67"), 12345.67)
        self.assertEqual(_parse_amount("(3,210.44)"), -3210.44)
        self.assertEqual(_parse_amount("-"), 0.0)
        self.assertEqual(_parse_amount(""), 0.0)
        self.assertEqual(_parse_amount("N.A."), 0.0)

    def test_month_dates(self):
        dates = _month_dates(3)
        self.assertEqual(len(dates), 3)
        self.assertTrue(all(d.day == 1 for d in dates))
        self.assertGreater(dates[0], dates[1])
        self.assertGreater(dates[1], dates[2])

    def test_deduplicate(self):
        rows = [
            {"report_month": date(2026, 6, 1), "category_name": "Small Cap", "net_flow_cr": 100},
            {"report_month": date(2026, 6, 1), "category_name": "Small Cap", "net_flow_cr": 200},  # duplicate, overrides
            {"report_month": date(2026, 6, 1), "category_name": "Large Cap", "net_flow_cr": 300},
        ]
        deduped = _deduplicate(rows)
        self.assertEqual(len(deduped), 2)
        small_cap = next(r for r in deduped if r["category_name"] == "Small Cap")
    def test_parse_csv_content(self):
        from src.importer.fetchers.amfi_flows_fetcher import _parse_csv_content
        csv_data = b"Category,Gross Purchase,Gross Redemption,Net,AUM\nSmall Cap Fund,12500,4500,8000,350000\n"
        rows = _parse_csv_content(csv_data, [date(2026, 6, 1)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["category_name"], "Small Cap Fund")
        self.assertEqual(rows[0]["net_flow_cr"], 8000.0)


if __name__ == "__main__":
    unittest.main()
