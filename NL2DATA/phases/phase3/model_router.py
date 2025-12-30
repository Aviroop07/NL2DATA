"""Phase 3 model router for step-specific model selection.

This module provides model selection for Phase 3 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 3 step to task type mapping
PHASE3_STEP_MAPPING: dict[str, TaskType] = {
    "3.1": "advanced_reasoning",  # Information Need Identification - advanced reasoning, use o3
    "3.2": "advanced_reasoning",   # Information Completeness Check - advanced reasoning, use o3
    "3.3": "high_fanout",          # Phase 2 Steps with Enhanced Context - per-entity/per-attribute
    # 3.4 and 3.5 are deterministic (no LLM)
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """
    Get appropriate model for a Phase 3 step based on step ID.
    
    Maps Phase 3 step IDs to task types based on step characteristics.
    
    Args:
        step_id: Phase 3 step identifier (e.g., "3.1", "3.2", "3.3")
        model_name: Override model name (optional)
        temperature: Override temperature (optional)
        max_tokens: Override max tokens (optional)
        timeout: Override timeout (optional)
        
    Returns:
        ChatOpenAI: Configured model for the step
        
    Example:
        >>> # For information need identification (important task)
        >>> model = get_model_for_step("3.1")
        >>> # Uses gpt-4o
    """
    # Validate step ID is Phase 3
    if not step_id.startswith("3."):
        logger.warning(
            f"Step ID '{step_id}' does not appear to be a Phase 3 step. "
            f"Using default 'simple' task type."
        )
        task_type = "simple"
    else:
        # Get task type from mapping
        task_type = PHASE3_STEP_MAPPING.get(step_id, "simple")
        logger.debug(f"Mapped Phase 3 step '{step_id}' to task type '{task_type}'")
    
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )


