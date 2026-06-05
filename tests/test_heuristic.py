import unittest
from unittest.mock import patch, MagicMock
import os

from src.agents.mosaic_fund_agent import MosaicFundAgent

class TestDeepDiveHeuristic(unittest.TestCase):
    @patch("src.tools.company_resolver.resolve_company_info")
    @patch("src.agents.sub_agents.run_subagent_for")
    @patch("src.agents.sub_agents.get_subagent")
    @patch("src.agents.intent_router.route_intent_llm")
    def test_indian_equity_pdf_ask_and_chat(self, mock_route_intent, mock_get_subagent, mock_run_subagent, mock_resolve_info):
        # Configure resolve_company_info mock for Indian stock "msumi"
        mock_resolve_info.return_value = {
            "symbol": "MSUMI",
            "nse_symbol": "MSUMI",
            "exchange": "NSE",
            "market": "India",
            "company_name": "MOTHERSON SUMI WRNG IND L"
        }
        
        # Mock the sub-agent responses
        mock_subagent_instance = MagicMock()
        mock_subagent_instance.run.return_value = "Sub-agent ran successfully for msumi"
        mock_get_subagent.return_value = mock_subagent_instance
        mock_run_subagent.return_value = "Sub-agent ran successfully for msumi"

        agent = MosaicFundAgent()

        # 1. Test ask() method
        res_ask = agent.ask("deep dive msumi save as pdf")
        mock_resolve_info.assert_called_with("msumi")
        mock_run_subagent.assert_called_once()
        # Verify that the PDF instruction was appended to the sub-agent prompt
        prompt_passed_ask = mock_run_subagent.call_args[0][1]
        self.assertIn("MSUMI", prompt_passed_ask)
        self.assertIn("publish_consolidated_pdf", prompt_passed_ask)
        self.assertEqual(res_ask, "Sub-agent ran successfully for msumi")

        # Reset mocks
        mock_resolve_info.reset_mock()
        mock_run_subagent.reset_mock()

        # 2. Test chat() method
        res_chat = agent.chat("deep dive msumi save as pdf")
        mock_resolve_info.assert_called_with("msumi")
        mock_get_subagent.assert_called_with("india_equity")
        mock_subagent_instance.run.assert_called_once()
        # Verify that the PDF instruction was appended to the sub-agent prompt
        prompt_passed_chat = mock_subagent_instance.run.call_args[0][0]
        self.assertIn("MSUMI", prompt_passed_chat)
        self.assertIn("publish_consolidated_pdf", prompt_passed_chat)
        self.assertEqual(res_chat, "Sub-agent ran successfully for msumi")

    @patch("src.tools.company_resolver.resolve_company_info")
    @patch("src.tools.skills_tools.run_deepdive_analysis")
    def test_us_equity_pdf_ask_and_chat(self, mock_deepdive_analysis, mock_resolve_info):
        # Configure resolve_company_info mock for US stock "aapl"
        mock_resolve_info.return_value = {
            "symbol": "AAPL",
            "market": "US",
            "company_name": "Apple Inc."
        }
        
        mock_deepdive_analysis.invoke.return_value = "US Deepdive report preview"

        agent = MosaicFundAgent()

        # Mock os.path.exists and open to simulate report.md generation
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", unittest.mock.mock_open(read_data="Full Apple Report")), \
             patch("src.tools.report_publisher.publish_consolidated_pdf", return_value="✅ PDF saved to /aapl.pdf") as mock_publish_pdf:
             
            # Test ask() method
            res_ask = agent.ask("deep dive aapl save as pdf")
            mock_resolve_info.assert_called_with("aapl")
            mock_deepdive_analysis.invoke.assert_called_with({"ticker": "AAPL"})
            mock_publish_pdf.assert_called_once_with(
                report_markdown="Full Apple Report",
                symbols="AAPL",
                title="US Deep Dive Analysis: AAPL"
            )
            self.assertIn("✅ PDF saved to /aapl.pdf", res_ask)

            # Reset mocks
            mock_resolve_info.reset_mock()
            mock_deepdive_analysis.reset_mock()
            mock_publish_pdf.reset_mock()

            # Test chat() method
            res_chat = agent.chat("deep dive aapl save as pdf")
            mock_resolve_info.assert_called_with("aapl")
            mock_deepdive_analysis.invoke.assert_called_with({"ticker": "AAPL"})
            mock_publish_pdf.assert_called_once_with(
                report_markdown="Full Apple Report",
                symbols="AAPL",
                title="US Deep Dive Analysis: AAPL"
            )
            self.assertIn("✅ PDF saved to /aapl.pdf", res_chat)

    @patch("src.tools.company_resolver.resolve_company_info")
    @patch("src.tools.skills_tools.run_deepdive_analysis")
    @patch("src.agents.intent_router.route_intent_llm")
    def test_unresolved_heuristic_falls_through(self, mock_route_intent, mock_deepdive_analysis, mock_resolve_info):
        # Resolution fails
        mock_resolve_info.return_value = {
            "symbol": None,
            "market": None
        }
        mock_route_intent.return_value = "main"

        agent = MosaicFundAgent()
        
        # Stub the LangGraph agent run/invoke to avoid hitting the real LLM/checkpointer
        agent._agent = MagicMock()
        agent._agent.invoke.return_value = {"messages": [MagicMock(content="Fallback answer from main agent")]}

        # Should fall through and NOT throw validation errors or invoke deepdive analysis
        res = agent.chat("deep dive msumi save as pdf")
        mock_deepdive_analysis.invoke.assert_not_called()
        self.assertIsNotNone(res)

if __name__ == "__main__":
    unittest.main()
