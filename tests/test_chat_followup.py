"""
tests/test_chat_followup.py
────────────────────────────
Unit tests for the follow-up routing logic in chat_cmd.py.
"""
import unittest
import re

from src.commands.chat_cmd import _FOLLOWUP_RE, _CONFIRMATION_RE

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

if __name__ == "__main__":
    unittest.main()
