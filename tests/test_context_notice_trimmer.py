"""
tests/test_context_notice_trimmer.py
──────────────────────────────────────
Unit tests for format_notice_marker and Notice Replacement context trimmer.
"""
import unittest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from src.agents.sub_agents.infra import _make_context_trimmer, format_notice_marker


class TestContextNoticeTrimmer(unittest.TestCase):
    def test_format_notice_marker(self):
        tool_msg = ToolMessage(
            content="A" * 500 + "\nSecond line data",
            name="query_clickhouse_db",
            tool_call_id="call_abc123",
        )
        notice = format_notice_marker(tool_msg)
        self.assertIn("Historical Tool Result Pruned", notice)
        self.assertIn("query_clickhouse_db", notice)
        self.assertIn("517 chars", notice)

    def test_notice_replacement_preserves_ai_thoughts(self):
        # 1000 tokens context window -> max input chars = 2000 chars, max tool = 400 chars
        trimmer_hook = _make_context_trimmer(context_window=1000)

        # 5 tool round-trips: after step 1 tool cap (400 chars each), total is ~2200 chars > 2000
        msgs = [
            HumanMessage(content="Research TATAMOTORS"),
            AIMessage(content="Checking snapshot.", tool_calls=[{"name": "get_company_snapshot", "args": {}, "id": "c1"}]),
            ToolMessage(content="X" * 1000, name="get_company_snapshot", tool_call_id="c1"),
            AIMessage(content="Checking earnings.", tool_calls=[{"name": "get_quarterly_results", "args": {}, "id": "c2"}]),
            ToolMessage(content="Y" * 1000, name="get_quarterly_results", tool_call_id="c2"),
            AIMessage(content="Checking holdings.", tool_calls=[{"name": "get_mf_holdings", "args": {}, "id": "c3"}]),
            ToolMessage(content="Z" * 1000, name="get_mf_holdings", tool_call_id="c3"),
            AIMessage(content="Checking price anomaly.", tool_calls=[{"name": "search_anomaly_events", "args": {}, "id": "c4"}]),
            ToolMessage(content="W" * 1000, name="search_anomaly_events", tool_call_id="c4"),
            AIMessage(content="Checking news.", tool_calls=[{"name": "get_etf_news", "args": {}, "id": "c5"}]),
            ToolMessage(content="N" * 1000, name="get_etf_news", tool_call_id="c5"),
        ]

        state = {"messages": msgs}
        res = trimmer_hook(state)
        trimmed = res["llm_input_messages"]

        # Assert all 11 messages remain in list (no AI message deleted!)
        self.assertEqual(len(trimmed), 11)
        # Assert historical ToolMessage (index 2) was replaced with Notice Marker
        self.assertIn("Historical Tool Result Pruned", str(trimmed[2].content))
        self.assertIn("get_company_snapshot", str(trimmed[2].content))
        # Assert AI messages are 100% preserved
        self.assertEqual(trimmed[1].content, "Checking snapshot.")
        self.assertEqual(trimmed[3].content, "Checking earnings.")


if __name__ == "__main__":
    unittest.main()
