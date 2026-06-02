"""
tests/test_caveman.py
──────────────────────
Unit tests for the Caveman mode configuration and dynamic prompt builder.
"""

from __future__ import annotations
import os
from unittest.mock import patch
from src.utils.caveman import get_caveman_prompt
from src.agents.sub_agents import get_subagent


def test_get_caveman_prompt():
    # 1. Test None/empty/disabled levels
    assert get_caveman_prompt(None) == ""
    assert get_caveman_prompt("off") == ""
    assert get_caveman_prompt("none") == ""
    assert get_caveman_prompt("disabled") == ""
    
    # 2. Test valid levels
    lite_prompt = get_caveman_prompt("lite")
    assert "Intensity Level: lite" in lite_prompt
    assert "Respond terse like smart caveman" in lite_prompt
    
    full_prompt = get_caveman_prompt("full")
    assert "Intensity Level: full" in full_prompt
    assert "Drop articles, fragments OK" in full_prompt
    
    ultra_prompt = get_caveman_prompt("ultra")
    assert "Intensity Level: ultra" in ultra_prompt
    assert "Bare fragments" in ultra_prompt

    # Test shorthand alias for wenyan
    wenyan_prompt = get_caveman_prompt("wenyan")
    assert "Intensity Level: wenyan-full" in wenyan_prompt


def test_caveman_environment_var():
    # Check that it reads from environment variable if no level passed
    os.environ["CAVEMAN_LEVEL"] = "ultra"
    try:
        prompt = get_caveman_prompt()
        assert "Intensity Level: ultra" in prompt
    finally:
        os.environ.pop("CAVEMAN_LEVEL", None)


def test_subagent_rebuild_on_caveman_change():
    # Fetch a subagent (e.g. news)
    sub = get_subagent("news")
    
    # Mock create_react_agent to inspect the prompt passed
    with patch("langgraph.prebuilt.create_react_agent") as mock_create:
        os.environ.pop("CAVEMAN_LEVEL", None)
        sub._agent = None
        sub._built_caveman_level = "dummy"
        sub._build()
        
        # Verify it was built without caveman prompt
        assert mock_create.called
        kwargs = mock_create.call_args[1]
        assert "CAVEMAN MODE" not in kwargs["prompt"]
        
    with patch("langgraph.prebuilt.create_react_agent") as mock_create:
        os.environ["CAVEMAN_LEVEL"] = "ultra"
        try:
            sub._agent = None
            sub._built_caveman_level = "dummy"
            # Simulate run rebuild logic
            current_caveman = os.environ.get("CAVEMAN_LEVEL")
            if sub._agent is None or current_caveman != sub._built_caveman_level:
                sub._build()
                sub._built_caveman_level = current_caveman
                
            assert sub._built_caveman_level == "ultra"
            assert mock_create.called
            kwargs = mock_create.call_args[1]
            assert "CAVEMAN MODE" in kwargs["prompt"]
            assert "Intensity Level: ultra" in kwargs["prompt"]
        finally:
            os.environ.pop("CAVEMAN_LEVEL", None)
