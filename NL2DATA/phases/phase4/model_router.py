"""Phase 4 model router for step-specific model selection.

This module provides model selection for Phase 4 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 4 step to task type mapping
PHASE4_STEP_MAPPING: dict[str, TaskType] = {
    "4.1": "critical_reasoning",  # Functional Dependency Analysis - CRITICAL for normalization, use o3-pro
    "4.3": "high_fanout",          # Data Type Assignment - per-entity, high fanout
    "4.4": "high_fanout",          # Categorical Detection - per-entity, high fanout
    "4.5": "high_fanout",          # Check Constraint Detection - per-categorical attribute
    "4.6": "high_fanout",          # Categorical Value Extraction - per-categorical attribute
    "4.7": "high_fanout",          # Categorical Distribution - per-categorical attribute
    # 4.2 is deterministic (no LLM)
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """
    Get appropriate model for a Phase 4 step based on step ID.
    
    Maps Phase 4 step IDs to task types based on step characteristics.
    
    Args:
        step_id: Phase 4 step identifier (e.g., "4.1", "4.3", "4.4")
        model_name: Override model name (optional)
        temperature: Override temperature (optional)
        max_tokens: Override max tokens (optional)
        timeout: Override timeout (optional)
        
    Returns:
        ChatOpenAI: Configured model for the step
        
    Example:
        >>> # For functional dependency analysis (important task)
        >>> model = get_model_for_step("4.1")
        >>> # Uses gpt-4o
    """
    # Validate step ID is Phase 4
    if not step_id.startswith("4."):
        logger.warning(
            f"Step ID '{step_id}' does not appear to be a Phase 4 step. "
            f"Using default 'simple' task type."
        )
        task_type = "simple"
    else:
        # Get task type from mapping
        task_type = PHASE4_STEP_MAPPING.get(step_id, "simple")
        logger.debug(f"Mapped Phase 4 step '{step_id}' to task type '{task_type}'")
    
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )


