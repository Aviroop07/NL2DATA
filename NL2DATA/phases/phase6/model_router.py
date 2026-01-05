"""Model router for Phase 6 (DDL Generation & Schema Creation).

Note: Phase 6 is now fully deterministic - no LLM interaction required.
This router is kept for backward compatibility but is not used.
"""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 6 step mappings (all deterministic, no LLM needed)
STEP_TO_TASK_TYPE = {
    "6.1": "simple",      # DDL Compilation (deterministic)
    "6.2": "simple",      # DDL Validation (deterministic)
    "6.3": "simple",      # Schema Creation (deterministic)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 6 step.
    
    Note: Phase 6 steps are deterministic and do not use LLM.
    This function is kept for backward compatibility.
    
    Args:
        step_number: Step number (e.g., "6.1", "6.2", "6.3")
        
    Returns:
        LLM model instance (not used in Phase 6)
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
