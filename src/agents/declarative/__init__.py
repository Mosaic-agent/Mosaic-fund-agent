"""src/agents/declarative/__init__.py
Declarative configuration-driven agent orchestration module for Mosaic.
"""
from __future__ import annotations

from src.agents.declarative.declarative_spec import (
    AutoToolCallSpec,
    DeclarativeAgentSpec,
    StepSpec,
    StepType,
    TemplateOutputSpec,
    UseToolsSpec,
    load_agent_spec_from_yaml,
    load_agent_spec_from_dict,
)

__all__ = [
    "DeclarativeAgentSpec",
    "StepSpec",
    "StepType",
    "AutoToolCallSpec",
    "UseToolsSpec",
    "TemplateOutputSpec",
    "load_agent_spec_from_yaml",
    "load_agent_spec_from_dict",
]
