"""Model router for Phase 6 (DDL Generation & Schema Creation)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 6 step mappings
STEP_TO_TASK_TYPE = {
    "6.1": "simple",      # DDL Compilation (deterministic)
    "6.2": "simple",      # DDL Validation (deterministic)
    "6.3": "important",   # DDL Error Correction (important for schema quality)
    "6.4": "simple",      # Schema Creation (deterministic)
    "6.5": "important",   # SQL Query Generation (important for query quality)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 6 step.
    
    Args:
        step_number: Step number (e.g., "6.1", "6.2", "6.3", "6.4", "6.5")
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
