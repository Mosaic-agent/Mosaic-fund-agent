"""
tests/test_declarative_spec.py
───────────────────────────────
Unit tests for DeclarativeAgentSpec schema validation.
"""
import unittest
from pydantic import ValidationError
from src.agents.declarative.declarative_spec import (
    DeclarativeAgentSpec,
    StepType,
    load_agent_spec_from_dict,
)


class TestDeclarativeSpec(unittest.TestCase):
    def test_valid_agent_spec(self):
        spec_dict = {
            "agent_name": "Test Agent",
            "agent_type": "test_agent",
            "default_model": "google/gemini-2.5-flash",
            "task_brief": "Do research.",
            "steps": [
                {
                    "id": "fetch_data",
                    "step_type": "auto_tools",
                    "parallel": True,
                    "tool_calls": [
                        {"tool_name": "mock_fetch", "params": {"symbol": "GOLDBEES"}}
                    ],
                },
                {
                    "id": "synthesize",
                    "step_type": "reason",
                    "prompt": "Analyze findings: {{ fetch_data.output }}",
                },
                {
                    "id": "format_output",
                    "step_type": "template_output",
                    "format": "markdown",
                    "template": "# Report\n{{ synthesize.output }}",
                },
            ],
            "sample_input": {"symbol": "GOLDBEES"},
        }
        spec = load_agent_spec_from_dict(spec_dict)
        self.assertEqual(spec.agent_name, "Test Agent")
        self.assertEqual(len(spec.steps), 3)
        self.assertEqual(spec.steps[0].step_type, StepType.AUTO_TOOLS)

    def test_invalid_step_type(self):
        spec_dict = {
            "agent_name": "Bad Agent",
            "agent_type": "bad_agent",
            "steps": [
                {"id": "step1", "step_type": "invalid_type"}
            ],
        }
        with self.assertRaises(ValidationError):
            load_agent_spec_from_dict(spec_dict)

    def test_duplicate_step_id(self):
        spec_dict = {
            "agent_name": "Dup Agent",
            "agent_type": "dup_agent",
            "steps": [
                {"id": "step1", "step_type": "reason", "prompt": "p1"},
                {"id": "step1", "step_type": "reason", "prompt": "p2"},
            ],
        }
        with self.assertRaises(ValidationError) as ctx:
            load_agent_spec_from_dict(spec_dict)
        self.assertIn("Duplicate step ID", str(ctx.exception))

    def test_missing_prompt_in_reason_step(self):
        spec_dict = {
            "agent_name": "No Prompt Agent",
            "agent_type": "no_prompt",
            "steps": [
                {"id": "step1", "step_type": "reason"}
            ],
        }
        with self.assertRaises(ValidationError) as ctx:
            load_agent_spec_from_dict(spec_dict)
        self.assertIn("requires a non-empty 'prompt'", str(ctx.exception))

    def test_invalid_jinja_syntax(self):
        spec_dict = {
            "agent_name": "Jinja Error Agent",
            "agent_type": "jinja_error",
            "steps": [
                {
                    "id": "step1",
                    "step_type": "template_output",
                    "template": "Hello {% if true %}",
                }
            ],
        }
        with self.assertRaises(ValidationError) as ctx:
            load_agent_spec_from_dict(spec_dict)
        self.assertIn("Invalid Jinja2 syntax", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
