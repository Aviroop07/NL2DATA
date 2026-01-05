"""Phase 8, Step 8.8: Constraint Compilation.

Compile constraint specifications into final format organized by category.
This is a deterministic step that organizes constraints from previous steps.
"""

from typing import Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema

logger = get_logger(__name__)


class ConstraintCompilationOutput(BaseModel):
    """Output structure for constraint compilation."""
    statistical_constraints: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of statistical constraints with complete metadata"
    )
    distribution_constraints: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of distribution constraints with complete metadata"
    )
    other_constraints: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of other constraints with complete metadata"
    )

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


async def step_8_8_constraint_compilation(
    constraints: List[Dict[str, Any]],
) -> ConstraintCompilationOutput:
    """
    Step 8.8 (deterministic): Compile constraints into final format.
    
    Organizes constraints by category and preserves all metadata from previous steps:
    - Step 8.4: Original description, justification substring, constraint type
    - Step 8.5: Scope information (affected tables/columns, reasoning)
    - Step 8.6: Enforcement strategy, level, column-wise DSL expressions
    - Step 8.7: Conflict resolution status (if applicable)
    
    Args:
        constraints: List of constraint dictionaries with enforcement strategies (from step 8.6)
        
    Returns:
        ConstraintCompilationOutput with constraints organized by category
    """
    if not constraints:
        logger.warning("No constraints provided to step_8_8_constraint_compilation")
        return ConstraintCompilationOutput(
            statistical_constraints=[],
            distribution_constraints=[],
            other_constraints=[],
        )
    
    # Organize constraints by category
    statistical = []
    distribution = []
    other = []
    
    for constraint in constraints:
        # Extract constraint category
        # Try multiple possible field names
        category = (
            constraint.get("constraint_category") or
            constraint.get("constraint_type") or
            "other_constraints"
        )
        
        # Normalize category name
        if category == "statistical":
            category = "statistical_constraints"
        elif category == "distribution":
            category = "distribution_constraints"
        elif category not in ["statistical_constraints", "distribution_constraints", "other_constraints"]:
            category = "other_constraints"
        
        # Convert Pydantic models to dicts if needed
        if hasattr(constraint, 'model_dump'):
            constraint_dict = constraint.model_dump()
        elif hasattr(constraint, 'dict'):
            constraint_dict = constraint.dict()
        else:
            constraint_dict = constraint if isinstance(constraint, dict) else {}
        
        # Add to appropriate category
        if category == "statistical_constraints":
            statistical.append(constraint_dict)
        elif category == "distribution_constraints":
            distribution.append(constraint_dict)
        else:
            other.append(constraint_dict)
    
    logger.info(
        f"Compiled {len(statistical)} statistical, {len(distribution)} distribution, "
        f"and {len(other)} other constraints"
    )
    
    return ConstraintCompilationOutput(
        statistical_constraints=statistical,
        distribution_constraints=distribution,
        other_constraints=other,
    )
