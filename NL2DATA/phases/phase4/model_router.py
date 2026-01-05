"""Model router for Phase 4 (Relational Schema Compilation)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 4 step mappings
STEP_TO_TASK_TYPE = {
    "4.1": "simple",      # Relational Schema Compilation (deterministic)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 4 step.
    
    Args:
        step_number: Step number (e.g., "4.1")
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
