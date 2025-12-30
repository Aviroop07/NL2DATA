"""Phase 1 model router for step-specific model selection.

This module provides model selection for Phase 1 steps, mapping each step
to the appropriate task type and model.
"""

from typing import Optional
from langchain_openai import ChatOpenAI

from NL2DATA.utils.llm.base_router import get_model_for_task, TaskType
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Phase 1 step to task type mapping
PHASE1_STEP_MAPPING: dict[str, TaskType] = {
    "1.1": "simple",              # Domain Detection - simple
    "1.2": "simple",              # Entity Mention Detection - simple
    "1.3": "important",           # Domain Inference - important
    "1.4": "critical_reasoning",   # Key Entity Extraction - CRITICAL foundation step, use o3-pro
    "1.5": "simple",              # Relation Mention Detection - simple
    "1.6": "important",           # Auxiliary Entity Suggestion - important
    "1.7": "advanced_reasoning",  # Entity Consolidation - advanced reasoning, use o3
    "1.75": "advanced_reasoning", # Entity vs relation reclassification - advanced reasoning
    "1.8": "high_fanout",         # Entity Cardinality - per-entity, high fanout
    "1.9": "critical_reasoning",  # Key Relations Extraction - CRITICAL, use o3-pro
    "1.10": "advanced_reasoning", # Schema Connectivity Validation - advanced reasoning, use o3
    "1.11": "critical_reasoning", # Relation Cardinality - CRITICAL, use o3-pro
    "1.12": "advanced_reasoning", # Relation Validation - advanced reasoning, use o3
}


def get_model_for_step(
    step_id: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """
    Get appropriate model for a Phase 1 step based on step ID.
    
    Maps Phase 1 step IDs to task types based on step characteristics.
    
    Args:
        step_id: Phase 1 step identifier (e.g., "1.1", "1.4", "1.12")
        model_name: Override model name (optional)
        temperature: Override temperature (optional)
        max_tokens: Override max tokens (optional)
        timeout: Override timeout (optional)
        
    Returns:
        ChatOpenAI: Configured model for the step
        
    Example:
        >>> # For domain detection (simple task)
        >>> model = get_model_for_step("1.1")
        >>> # Uses gpt-4o-mini
        
        >>> # For entity extraction (important task)
        >>> model = get_model_for_step("1.4")
        >>> # Uses gpt-4o
        
        >>> # For entity consolidation (reasoning task)
        >>> model = get_model_for_step("1.7")
        >>> # Uses o3-mini
    """
    # Validate step ID is Phase 1
    if not step_id.startswith("1."):
        logger.warning(
            f"Step ID '{step_id}' does not appear to be a Phase 1 step. "
            f"Using default 'simple' task type."
        )
        task_type = "simple"
    else:
        # Get task type from mapping
        task_type = PHASE1_STEP_MAPPING.get(step_id, "simple")
        logger.debug(f"Mapped Phase 1 step '{step_id}' to task type '{task_type}'")
    
    return get_model_for_task(
        task_type=task_type,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

