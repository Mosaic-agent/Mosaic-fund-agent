"""
tests/test_declarative_runner.py
─────────────────────────────────
Unit tests for DeclarativeAgentRunner and auto_tools parallel execution.
"""
import time
import unittest
from src.agents.declarative.declarative_runner import DeclarativeAgentRunner


def mock_fetch_price(symbol: str) -> dict:
    time.sleep(0.1)
    return {"symbol": symbol, "price": 54.25}


def mock_fetch_garch(symbol: str) -> dict:
    time.sleep(0.1)
    return {"symbol": symbol, "garch_vol": 12.4}


def mock_failing_tool(symbol: str) -> dict:
    raise ValueError("Database query failed")


class TestDeclarativeRunner(unittest.TestCase):
    def setUp(self):
        self.tool_registry = {
            "mock_fetch_price": mock_fetch_price,
            "mock_fetch_garch": mock_fetch_garch,
            "mock_failing_tool": mock_failing_tool,
        }

    def test_auto_tools_parallel_execution(self):
        spec_dict = {
            "agent_name": "Parallel Test Agent",
            "agent_type": "parallel_test",
            "steps": [
                {
                    "id": "batch_fetch",
                    "step_type": "auto_tools",
                    "parallel": True,
                    "tool_calls": [
                        {"tool_name": "mock_fetch_price", "params": {"symbol": "GOLDBEES"}},
                        {"tool_name": "mock_fetch_garch", "params": {"symbol": "GOLDBEES"}},
                    ],
                },
                {
                    "id": "format",
                    "step_type": "template_output",
                    "format": "markdown",
                    "template": "Price: {{ batch_fetch.mock_fetch_price.price }}, Vol: {{ batch_fetch.mock_fetch_garch.garch_vol }}",
                },
            ],
            "sample_input": {"symbol": "GOLDBEES"},
        }
        runner = DeclarativeAgentRunner(spec_dict, tool_registry=self.tool_registry)

        start_t = time.time()
        res = runner.run()
        elapsed = time.time() - start_t

        self.assertIn("Price: 54.25", res["output"])
        self.assertIn("Vol: 12.4", res["output"])
        # Both tools take 0.1s each -> in parallel, total time < 0.18s
        self.assertLess(elapsed, 0.18)

    def test_auto_tools_fail_on_error_false(self):
        spec_dict = {
            "agent_name": "Fail Test Agent",
            "agent_type": "fail_test",
            "steps": [
                {
                    "id": "fetch_with_failure",
                    "step_type": "auto_tools",
                    "parallel": False,
                    "tool_calls": [
                        {
                            "tool_name": "mock_failing_tool",
                            "params": {"symbol": "GOLDBEES"},
                            "fail_on_error": False,
                        }
                    ],
                },
                {
                    "id": "format",
                    "step_type": "template_output",
                    "template": "Status: {{ fetch_with_failure.mock_failing_tool }}",
                },
            ],
        }
        runner = DeclarativeAgentRunner(spec_dict, tool_registry=self.tool_registry)
        res = runner.run()
        self.assertIn("Tool Error [mock_failing_tool]", res["output"])

    def test_auto_tools_fail_on_error_true(self):
        spec_dict = {
            "agent_name": "Strict Fail Test Agent",
            "agent_type": "strict_fail",
            "steps": [
                {
                    "id": "fetch_with_strict_failure",
                    "step_type": "auto_tools",
                    "parallel": False,
                    "tool_calls": [
                        {
                            "tool_name": "mock_failing_tool",
                            "params": {"symbol": "GOLDBEES"},
                            "fail_on_error": True,
                        }
                    ],
                }
            ],
        }
        runner = DeclarativeAgentRunner(spec_dict, tool_registry=self.tool_registry)
        with self.assertRaises(RuntimeError) as ctx:
            runner.run()
        self.assertIn("Tool Error [mock_failing_tool]", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
