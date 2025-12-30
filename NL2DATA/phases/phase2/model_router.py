"""Phase 2 model router for step-specific model selection.

This module provides model selection for Phase 2 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 2 step to task type mapping
PHASE2_STEP_MAPPING: dict[str, TaskType] = {
    "2.1": "high_fanout",   # Attribute Count Detection - per-entity
    "2.2": "high_fanout",   # Intrinsic Attributes - per-entity, high fanout
    "2.3": "high_fanout",   # Attribute Synonym Detection - per-entity
    "2.4": "high_fanout",   # Composite Attribute Handling - per-entity
    "2.5": "high_fanout",   # Temporal Attributes Detection - per-entity
    "2.6": "validation",    # Naming Convention Validation - deterministic, but validation
    "2.7": "important",     # Primary Key Identification - important
    "2.8": "high_fanout",   # Multivalued/Derived Detection - per-entity
    "2.9": "high_fanout",   # Derived Attribute Formulas - per-attribute
    "2.10": "high_fanout",  # Unique Constraints - per-entity
    "2.11": "high_fanout",  # Nullability Constraints - per-entity
    "2.12": "high_fanout",  # Default Values - per-entity
    "2.13": "high_fanout",  # Check Constraints - per-entity
    "2.14": "important",    # Relation Realization - important
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """
    Get appropriate model for a Phase 2 step based on step ID.
    
    Maps Phase 2 step IDs to task types based on step characteristics.
    
    Args:
        step_id: Phase 2 step identifier (e.g., "2.1", "2.4", "2.14")
        model_name: Override model name (optional)
        temperature: Override temperature (optional)
        max_tokens: Override max tokens (optional)
        timeout: Override timeout (optional)
        
    Returns:
        ChatOpenAI: Configured model for the step
        
    Example:
        >>> # For attribute detection (high fanout task)
        >>> model = get_model_for_step("2.1")
        >>> # Uses gpt-4o-mini
        
        >>> # For primary key identification (important task)
        >>> model = get_model_for_step("2.7")
        >>> # Uses gpt-4o
    """
    # Validate step ID is Phase 2
    if not step_id.startswith("2."):
        logger.warning(
            f"Step ID '{step_id}' does not appear to be a Phase 2 step. "
            f"Using default 'simple' task type."
        )
        task_type = "simple"
    else:
        # Get task type from mapping
        task_type = PHASE2_STEP_MAPPING.get(step_id, "simple")
        logger.debug(f"Mapped Phase 2 step '{step_id}' to task type '{task_type}'")
    
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

