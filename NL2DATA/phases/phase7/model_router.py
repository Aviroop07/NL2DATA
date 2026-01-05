"""Model router for Phase 7 (Information Mining)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 7 step mappings
STEP_TO_TASK_TYPE = {
    "7.1": "important",      # Information Need Identification (important for query understanding)
    "7.2": "important",      # SQL Generation and Validation
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 7 step.
    
    Args:
        step_number: Step number (e.g., "7.1", "7.2", etc.)
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
