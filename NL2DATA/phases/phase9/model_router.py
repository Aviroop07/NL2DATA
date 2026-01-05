"""Model router for Phase 9 (Generation Strategies)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 9 step mappings
STEP_TO_TASK_TYPE = {
    "9.1": "important",      # Numerical Range Definition
    "9.2": "important",      # Text Generation Strategy
    "9.3": "important",      # Boolean Dependency Analysis
    "9.4": "important",     # Data Volume Specifications
    "9.5": "important",    # Partitioning Strategy
    "9.6": "simple",       # Distribution Compilation (deterministic)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 9 step.
    
    Args:
        step_number: Step number (e.g., "9.1", "9.2", etc.)
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
