"""
tests/test_declarative_templates.py
────────────────────────────────────
Unit tests for template_output Markdown rendering and deterministic ContextManager.
"""
import unittest
from src.agents.declarative.declarative_runner import DeclarativeAgentRunner
from src.workflows.context_manager import ContextManager


class TestDeclarativeTemplates(unittest.TestCase):
    def test_template_output_exact_numeric_rendering(self):
        spec_dict = {
            "agent_name": "Template Test Agent",
            "agent_type": "template_test",
            "steps": [
                {
                    "id": "format_report",
                    "step_type": "template_output",
                    "format": "markdown",
                    "template": (
                        "# Quantitative Report for {{ symbol }}\n"
                        "- Raw Kelly: {{ raw_kelly }}\n"
                        "- Expected Return: {{ expected_return_pct }}%\n"
                        "- GARCH Volatility: {{ garch_vol }}%"
                    ),
                }
            ],
            "sample_input": {
                "symbol": "GOLDBEES",
                "raw_kelly": 0.8524,
                "expected_return_pct": 1.45,
                "garch_vol": 12.35,
            },
        }
        runner = DeclarativeAgentRunner(spec_dict)
        res = runner.run()
        output = res["output"]

        self.assertIn("# Quantitative Report for GOLDBEES", output)
        self.assertIn("- Raw Kelly: 0.8524", output)
        self.assertIn("- Expected Return: 1.45%", output)
        self.assertIn("- GARCH Volatility: 12.35%", output)

    def test_context_manager_dedup_and_truncate(self):
        cm = ContextManager(max_chars=100)
        table_text = (
            "| Symbol | Price |\n"
            "|---|---|\n"
            "| GOLDBEES | 54.25 |\n"
            "| GOLDBEES | 54.25 |\n"
            "| SILVERBEES | 72.10 |\n"
        )
        deduped = cm.dedup_rows(table_text)
        self.assertEqual(deduped.count("GOLDBEES"), 1)
        self.assertEqual(deduped.count("SILVERBEES"), 1)

        truncated = cm.compress("key", table_text * 10)
        self.assertIn("chars trimmed", truncated)

    def test_xml_tag_filter(self):
        spec_dict = {
            "agent_name": "XML Tag Test Agent",
            "agent_type": "xml_tag_test",
            "steps": [
                {
                    "id": "format_report",
                    "step_type": "template_output",
                    "format": "markdown",
                    "template": "{{ symbol | xml_tag('target_stock') }}",
                }
            ],
            "sample_input": {
                "symbol": "TATAMOTORS",
            },
        }
        runner = DeclarativeAgentRunner(spec_dict)
        res = runner.run()
        output = res["output"]
        self.assertIn("<target_stock>\nTATAMOTORS\n</target_stock>", output)


if __name__ == "__main__":
    unittest.main()
