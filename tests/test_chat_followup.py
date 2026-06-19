"""
tests/test_chat_followup.py
────────────────────────────
Unit tests for the follow-up routing logic in chat_cmd.py.
"""
import unittest
import re

from src.commands.chat_cmd import _FOLLOWUP_RE, _CONFIRMATION_RE, _is_numeric_choice_prompt, _REPORT_FOLLOWUP_RE

class TestChatFollowupRouting(unittest.TestCase):
    def test_followup_regex(self):
        """Test that _FOLLOWUP_RE correctly matches standard follow-up prefixes."""
        self.assertTrue(_FOLLOWUP_RE.match("compare with HDFC"))
        self.assertTrue(_FOLLOWUP_RE.match("vs RELIANCE"))
        self.assertTrue(_FOLLOWUP_RE.match("what about TCS"))
        self.assertTrue(_FOLLOWUP_RE.match("how about INFY"))
        self.assertTrue(_FOLLOWUP_RE.match("now show goldbees"))
        self.assertTrue(_FOLLOWUP_RE.match("also check KOTAKGOLD"))
        
        self.assertFalse(_FOLLOWUP_RE.match("yes"))
        self.assertFalse(_FOLLOWUP_RE.match("no"))
        self.assertFalse(_FOLLOWUP_RE.match("show composite signals"))

    def test_report_followup_regex(self):
        """Test that _REPORT_FOLLOWUP_RE correctly matches report-specific queries."""
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("summarise the report"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("summarize it"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("explain the risks"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("any red flags?"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("what is the valuation?"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("tell me about the competitors"))
        self.assertTrue(_REPORT_FOLLOWUP_RE.search("details on the company"))
        
        self.assertFalse(_REPORT_FOLLOWUP_RE.search("compare goldbees and nifty"))
        self.assertFalse(_REPORT_FOLLOWUP_RE.search("get stock news for tata"))

    def test_confirmation_regex(self):
        """Test that _CONFIRMATION_RE correctly matches confirmation/negation words."""
        self.assertTrue(_CONFIRMATION_RE.match("yes"))
        self.assertTrue(_CONFIRMATION_RE.match("no"))
        self.assertTrue(_CONFIRMATION_RE.match("y"))
        self.assertTrue(_CONFIRMATION_RE.match("n"))
        self.assertTrue(_CONFIRMATION_RE.match("sure"))
        self.assertTrue(_CONFIRMATION_RE.match("please"))
        self.assertTrue(_CONFIRMATION_RE.match("ok"))
        self.assertTrue(_CONFIRMATION_RE.match("okay"))
        self.assertTrue(_CONFIRMATION_RE.match("yeah"))
        self.assertTrue(_CONFIRMATION_RE.match("yep"))
        self.assertTrue(_CONFIRMATION_RE.match("nah"))
        self.assertTrue(_CONFIRMATION_RE.match("go ahead"))
        self.assertTrue(_CONFIRMATION_RE.match("do it"))
        self.assertTrue(_CONFIRMATION_RE.match("sure thing"))
        self.assertTrue(_CONFIRMATION_RE.match("yes please"))

        self.assertFalse(_CONFIRMATION_RE.match("yesterday's volume"))
        self.assertFalse(_CONFIRMATION_RE.match("nifty index close"))
        self.assertFalse(_CONFIRMATION_RE.match("not sure"))

    def test_numeric_choice_prompt(self):
        """Test that _is_numeric_choice_prompt correctly identifies prompts for numeric selections."""
        prompt_with_choices = (
            "I need to import GOLDBEES data for the last 3 months. Before I proceed, please let me know which data source you'd like to use:\n\n"
            " 1 Shoonya — Real-time NSE feed via Shoonya broker API\n"
            " 2 NSE — Direct NSE website data\n"
            " 3 yfinance — Yahoo Finance (free, global)\n"
            "Which source would you prefer? (Enter 1, 2, or 3)"
        )
        self.assertTrue(_is_numeric_choice_prompt(prompt_with_choices))
        
        # Test direct tool response strings
        tool_req = (
            "DATA_SOURCE_REQUIRED: Ask the user which data source to use before importing:\n"
            "1. Shoonya\n2. NSE\n3. yfinance"
        )
        self.assertTrue(_is_numeric_choice_prompt(tool_req))

        # Test other variations
        self.assertTrue(_is_numeric_choice_prompt("Please choose an option:\n1) Gold\n2) Silver\n3) Copper"))
        self.assertTrue(_is_numeric_choice_prompt("Which of the following is correct? Enter 1, 2, or 3."))

        # Test non-choice messages
        self.assertFalse(_is_numeric_choice_prompt("GOLDBEES has gone up by 1.2% today, compared to NIFTY which is up 2.3% and gold at 3.1%."))
        self.assertFalse(_is_numeric_choice_prompt("Here is the GOLDBEES ML prediction report."))
        self.assertFalse(_is_numeric_choice_prompt(""))

if __name__ == "__main__":
    unittest.main()
