"""
tests/test_declarative_e2e.py
──────────────────────────────
End-to-end integration tests for declarative YAML playbooks and sub-agent registry loading.
"""
import unittest
from src.agents.sub_agents.registry import get_subagent


class TestDeclarativeE2E(unittest.TestCase):
    def test_registry_loads_goldbees_pipeline_playbook(self):
        agent = get_subagent("goldbees_pipeline")
        self.assertIsNotNone(agent)
        self.assertTrue(hasattr(agent, "runner"))

    def test_registry_loads_india_equity_playbook(self):
        agent = get_subagent("india_equity")
        self.assertIsNotNone(agent)
        self.assertTrue(hasattr(agent, "runner"))


if __name__ == "__main__":
    unittest.main()
