"""Model router for Phase 8 (Functional Dependencies & Constraints)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 8 step mappings
STEP_TO_TASK_TYPE = {
    "8.1": "critical_reasoning",  # Functional Dependency Analysis - CRITICAL for normalization
    "8.2": "important",            # Categorical Column Identification
    "8.3": "important",           # Constraint Detection (important for data quality)
    "8.4": "important",            # Constraint Scope Analysis
    "8.5": "important",            # Constraint Enforcement Strategy
    "8.6": "simple",               # Constraint Conflict Detection
    "8.7": "simple",               # Constraint Compilation (deterministic)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 8 step.
    
    Args:
        step_number: Step number (e.g., "8.1", "8.2", etc.)
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
