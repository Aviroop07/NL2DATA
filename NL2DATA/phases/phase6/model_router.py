"""Phase 6 model router for step-specific model selection.

This module provides model selection for Phase 6 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 6 step to task type mapping
PHASE6_STEP_MAPPING: dict[str, TaskType] = {
    "6.1": "important",  # Constraint Detection - important, with loop support
    "6.2": "high_fanout",  # Constraint Scope Analysis - per-constraint, high fanout
    "6.3": "high_fanout",  # Constraint Enforcement Strategy - per-constraint, high fanout
    # 6.4, 6.5 are deterministic (no LLM)
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """Get model for a specific Phase 6 step.
    
    Args:
        step_id: Step identifier (e.g., "6.1", "6.2", "6.3")
        model_name: Optional override model name
        temperature: Optional override temperature
        max_tokens: Optional override max tokens
        timeout: Optional override timeout
        
    Returns:
        ChatOpenAI model instance configured for the step
    """
    task_type = PHASE6_STEP_MAPPING.get(step_id, "default")
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

