"""
src/agents/declarative/declarative_spec.py
─────────────────────────────────────────────
Declarative agent specification schemas and Pydantic validation contracts.

Defines the contract model for configuration-driven agent playbooks:
  - StepTypes: reason, auto_tools, use_tools, tasks, agent, loop, template_output
  - Strict validation for Jinja2 templates, tool call shapes, cycle bounds, and model names.
"""
from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, TemplateSyntaxError
from pydantic import BaseModel, Field, field_validator, model_validator

_jinja_env = Environment()


class StepType(str, Enum):
    REASON = "reason"
    AUTO_TOOLS = "auto_tools"
    USE_TOOLS = "use_tools"
    TASKS = "tasks"
    AGENT = "agent"
    LOOP = "loop"
    TEMPLATE_OUTPUT = "template_output"


class AutoToolCallSpec(BaseModel):
    """Configuration for a single deterministic tool call in an auto_tools step."""
    tool_name: str = Field(..., min_length=1, description="Registered tool function name")
    params: dict[str, Any] = Field(default_factory=dict, description="Parameters passed to the tool")
    fail_on_error: bool = Field(default=True, description="Whether tool exception halts execution")
    condition: str | None = Field(default=None, description="Optional boolean condition for running tool")


class UseToolsSpec(BaseModel):
    """Configuration for model-directed tool selection within guardrails."""
    mandatory_tools: list[str] = Field(default_factory=list)
    optional_tools: list[str] = Field(default_factory=list)
    max_cycles: int = Field(default=3, ge=1, le=10, description="Max think-call-observe rounds")
    prompt: str = Field(default="", description="Prompt instructing model on tool usage")


class TemplateOutputSpec(BaseModel):
    """Configuration for final deterministic report rendering."""
    format: str = Field(default="markdown", description="Output format (markdown, json, html)")
    template: str = Field(..., min_length=1, description="Jinja2 template string")

    @field_validator("template")
    @classmethod
    def validate_jinja_template(cls, v: str) -> str:
        try:
            _jinja_env.parse(v)
        except TemplateSyntaxError as err:
            raise ValueError(f"Invalid Jinja2 syntax in template: {err}") from err
        return v


class StepSpec(BaseModel):
    """Specification for an individual workflow step."""
    id: str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_-]+$", description="Unique step identifier")
    step_type: StepType = Field(..., description="Action verb for step execution")
    condition: str | None = Field(default=None, description="Optional condition expression")

    # Fields for step_type == 'reason' / 'use_tools'
    prompt: str | None = Field(default=None, description="Jinja2 rendered prompt string")

    # Fields for step_type == 'auto_tools'
    parallel: bool = Field(default=True, description="Run tool calls in parallel thread pool")
    tool_calls: list[AutoToolCallSpec] = Field(default_factory=list)

    # Fields for step_type == 'use_tools'
    mandatory_tools: list[str] = Field(default_factory=list)
    optional_tools: list[str] = Field(default_factory=list)
    max_cycles: int = Field(default=3, ge=1, le=10)

    # Fields for step_type == 'tasks' (nested parallel steps)
    max_concurrency: int = Field(default=4, ge=1)
    tasks: list[StepSpec] = Field(default_factory=list)

    # Fields for step_type == 'agent' (sub-agent delegation)
    agent_type: str | None = Field(default=None)
    params: dict[str, Any] = Field(default_factory=dict)

    # Fields for step_type == 'loop' (bounded iteration)
    max_iterations: int = Field(default=3, ge=1, le=20)
    stop_when: str | None = Field(default=None)
    steps: list[StepSpec] = Field(default_factory=list)

    # Fields for step_type == 'template_output'
    format: str = Field(default="markdown")
    template: str | None = Field(default=None)

    @field_validator("prompt", "template")
    @classmethod
    def validate_jinja_string(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                _jinja_env.parse(v)
            except TemplateSyntaxError as err:
                raise ValueError(f"Invalid Jinja2 syntax: {err}") from err
        return v

    @model_validator(mode="after")
    def validate_step_type_fields(self) -> StepSpec:
        st = self.step_type
        if st == StepType.REASON and not self.prompt:
            raise ValueError(f"Step '{self.id}' of type 'reason' requires a non-empty 'prompt'.")
        if st == StepType.AUTO_TOOLS and not self.tool_calls:
            raise ValueError(f"Step '{self.id}' of type 'auto_tools' requires at least one item in 'tool_calls'.")
        if st == StepType.TEMPLATE_OUTPUT and not self.template:
            raise ValueError(f"Step '{self.id}' of type 'template_output' requires a non-empty 'template'.")
        if st == StepType.LOOP and not self.steps:
            raise ValueError(f"Step '{self.id}' of type 'loop' requires nested 'steps'.")
        if st == StepType.TASKS and not self.tasks:
            raise ValueError(f"Step '{self.id}' of type 'tasks' requires nested 'tasks'.")
        return self


class DeclarativeAgentSpec(BaseModel):
    """Root contract specification for a configuration-driven agent playbook."""
    agent_name: str = Field(..., min_length=1, description="Human-readable agent name")
    agent_type: str = Field(..., min_length=1, pattern=r"^[a-zA-Z0-9_-]+$", description="Logical agent intent key")
    default_model: str = Field(default="google/gemini-2.5-flash", description="Default LLM model tag")
    seed: int | None = Field(default=42, description="Random seed for reproducibility")
    task_brief: str = Field(default="", description="System-level directive prompt")
    steps: list[StepSpec] = Field(..., min_items=1, description="Ordered workflow steps")
    sample_input: dict[str, Any] = Field(default_factory=dict, description="Executable sample inputs")

    @field_validator("steps")
    @classmethod
    def validate_unique_step_ids(cls, v: list[StepSpec]) -> list[StepSpec]:
        seen: set[str] = set()
        for step in v:
            if step.id in seen:
                raise ValueError(f"Duplicate step ID '{step.id}' found in agent specification.")
            seen.add(step.id)
        return v


def load_agent_spec_from_dict(data: dict[str, Any]) -> DeclarativeAgentSpec:
    """Validate and load a DeclarativeAgentSpec from a dictionary."""
    return DeclarativeAgentSpec.model_validate(data)


def load_agent_spec_from_yaml(filepath: str | Path) -> DeclarativeAgentSpec:
    """Load, parse, and validate a DeclarativeAgentSpec from a YAML file."""
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"Agent playbook file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)
    if not isinstance(raw_data, dict):
        raise ValueError(f"Invalid YAML content in {path}: expected a dictionary root.")
    return load_agent_spec_from_dict(raw_data)
