"""Step Definition Registry - Single source of truth for all step metadata.

This module provides a centralized registry of all steps in the IR generation
framework, eliminating inconsistencies in step numbering, call-type classification,
and call-count estimates.
"""

from .types import CallType, StepType, StepDefinition
from .registry import (
    STEP_REGISTRY,
    get_steps_by_phase,
    get_llm_steps,
    get_deterministic_steps,
    get_step_by_id,
    get_step_by_number,
)
from .estimators import (
    estimate_total_calls,
    estimate_phase_calls,
    estimate_cost,
)
from .messages import (
    STEP_MESSAGE_TEMPLATES,
    get_step_message,
)

__all__ = [
    # Types
    "CallType",
    "StepType",
    "StepDefinition",
    # Registry
    "STEP_REGISTRY",
    "get_steps_by_phase",
    "get_llm_steps",
    "get_deterministic_steps",
    "get_step_by_id",
    "get_step_by_number",
    # Estimators
    "estimate_total_calls",
    "estimate_phase_calls",
    "estimate_cost",
    # Messages
    "STEP_MESSAGE_TEMPLATES",
    "get_step_message",
]

