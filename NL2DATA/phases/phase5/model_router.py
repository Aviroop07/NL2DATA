"""Phase 5 model router for step-specific model selection.

This module provides model selection for Phase 5 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 5 step to task type mapping
PHASE5_STEP_MAPPING: dict[str, TaskType] = {
    "5.3": "important",  # DDL Error Correction - important, conditional
    "5.5": "important",  # SQL Query Generation - important, per-information need
    # 5.1, 5.2, 5.4 are deterministic (no LLM)
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """Get model for a specific Phase 5 step.
    
    Args:
        step_id: Step identifier (e.g., "5.3", "5.5")
        model_name: Optional override model name
        temperature: Optional override temperature
        max_tokens: Optional override max tokens
        timeout: Optional override timeout
        
    Returns:
        ChatOpenAI model instance configured for the step
    """
    task_type = PHASE5_STEP_MAPPING.get(step_id, "default")
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

