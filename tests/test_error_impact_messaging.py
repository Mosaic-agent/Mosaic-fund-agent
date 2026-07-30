"""
tests/test_error_impact_messaging.py
──────────────────────────────────────
Unit tests verifying that tool calling and agent exception messages include
explicit impact statements that are readable by LLMs.
"""

import unittest
from langchain_core.tools import tool
from src.utils.error_utils import format_tool_error, format_agent_error
from src.agents.sub_agents.infra import _wrap_tool_for_dedup


class TestErrorImpactMessaging(unittest.TestCase):

    def test_format_tool_error(self):
        err = format_tool_error("test_query", ValueError("Connection refused"))
        self.assertIn("Tool Error [test_query]: Connection refused.", err)
        self.assertIn("Impact:", err)
        self.assertIn("Downstream analysis using this tool's output will be incomplete", err)

    def test_format_tool_error_custom_impact(self):
        custom_impact = "GARCH volatility falling back to default 15% target."
        err = format_tool_error("garch_calculator", RuntimeError("No convergence"), custom_impact)
        self.assertIn("Tool Error [garch_calculator]: No convergence.", err)
        self.assertIn(f"Impact: {custom_impact}", err)

    def test_format_agent_error(self):
        err = format_agent_error("signal", RuntimeError("ML model file missing"))
        self.assertIn("Agent Error [signal]: ML model file missing.", err)
        self.assertIn("Impact:", err)
        self.assertIn("Findings for this domain will be omitted", err)

    def test_dedup_tool_wrapper_catches_exception_with_impact(self):
        @tool
        def failing_tool(x: int) -> str:
            """Dummy failing tool."""
            raise RuntimeError("Database query timed out")

        wrapped = _wrap_tool_for_dedup(failing_tool)
        res = wrapped.func(x=10)
        self.assertIn("Tool Error [failing_tool]: Database query timed out.", res)
        self.assertIn("Impact:", res)

    def test_delegation_tool_error_impact(self):
        from src.tools.agent_tools import delegate_to_signal_agent
        # Force delegation to fail by passing a bad mock or triggering exception if subagent fails
        try:
            from unittest.mock import patch
            with patch("src.agents.sub_agents.run_subagent_for", side_effect=RuntimeError("Sub-agent process crashed")):
                res = delegate_to_signal_agent.func("Test question")
                self.assertIn("Agent Error [signal]: Sub-agent process crashed.", res)
                self.assertIn("Impact: Signal sub-agent failed.", res)
        except Exception as exc:
            self.fail(f"Delegation error handler failed: {exc}")


if __name__ == "__main__":
    unittest.main()
