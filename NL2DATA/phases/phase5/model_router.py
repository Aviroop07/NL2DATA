"""Model router for Phase 5 (Data Type Assignment)."""

from NL2DATA.utils.llm.base_router import get_model_for_task

# Phase 5 step mappings
STEP_TO_TASK_TYPE = {
    "5.1": "simple",      # Dependency graph construction (deterministic, but may need LLM for derived deps)
    "5.2": "high_fanout", # Independent attribute data types (parallel per attribute, excludes FKs)
    "5.3": "simple",      # FK derivation (deterministic)
    "5.4": "high_fanout", # Dependent attribute data types (parallel per attribute, derived handled deterministically)
    "5.5": "simple",      # Nullability detection (excludes PKs and FKs with total participation)
}


def get_model_for_step(step_number: str):
    """Get LLM model for a Phase 5 step.
    
    Args:
        step_number: Step number (e.g., "5.1", "5.2")
        
    Returns:
        LLM model instance
    """
    task_type = STEP_TO_TASK_TYPE.get(step_number, "simple")
    return get_model_for_task(task_type=task_type)
