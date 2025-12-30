"""Phase 7 model router for step-specific model selection.

This module provides model selection for Phase 7 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 7 step to task type mapping
PHASE7_STEP_MAPPING: dict[str, TaskType] = {
    "7.1": "high_fanout",  # Numerical Range Definition - per-numerical attribute, high fanout
    "7.2": "high_fanout",  # Text Generation Strategy - per-text attribute, high fanout
    "7.3": "high_fanout",  # Boolean Dependency Analysis - per-boolean attribute, high fanout
    "7.4": "important",  # Data Volume Specifications - singular, important
    "7.5": "high_fanout",  # Partitioning Strategy - per-entity (conditional), high fanout
    # 7.6 is deterministic (no LLM)
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """Get model for a specific Phase 7 step.
    
    Args:
        step_id: Step identifier (e.g., "7.1", "7.2", "7.3", "7.4", "7.5")
        model_name: Optional override model name
        temperature: Optional override temperature
        max_tokens: Optional override max tokens
        timeout: Optional override timeout
        
    Returns:
        ChatOpenAI model instance configured for the step
    """
    task_type = PHASE7_STEP_MAPPING.get(step_id, "default")
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

