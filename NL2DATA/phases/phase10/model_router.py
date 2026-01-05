"""Model router for Phase 11 (Final Table Generation).

This phase will handle final table generation from complete metadata.
Currently empty - to be implemented later.
"""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 11 step mappings (to be added when implemented)
STEP_TO_TASK_TYPE = {
    # Add step mappings as needed
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 11 step.
    
    Args:
        step_number: Step number (e.g., "11.1", "11.2", etc.)
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
