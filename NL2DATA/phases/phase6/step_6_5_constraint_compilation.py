"""Phase 6, Step 6.5: Constraint Compilation.

Convert constraint descriptions into structured constraint objects and add to LogicalIR.
Deterministic transformation - structures constraint information.
"""

import json
from typing import Dict, Any, List, Optional

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def step_6_5_constraint_compilation(
    constraints: List[Dict[str, Any]],  # All constraints from Steps 6.1-6.4
) -> Dict[str, Any]:
    """
    Step 6.5 (deterministic): Compile constraints into LogicalIR format.
    
    This is a deterministic transformation that structures constraint information
    and prepares it for LogicalIR. No LLM call needed.
    
    Args:
        constraints: List of constraints with descriptions, DSL expressions, scope, enforcement
        
    Returns:
        dict: Compiled constraints in LogicalIR format
        
    Example:
        >>> constraints = [
        ...     {
        ...         "description": "Amount must be positive",
        ...         "dsl_expression": "amount > 0",
        ...         "enforcement_type": "check_constraint",
        ...         "affected_attributes": ["Transaction.amount"]
        ...     }
        ... ]
        >>> result = step_6_5_constraint_compilation(constraints)
        >>> len(result["statistical_constraints"]) >= 0
        True
    """
    logger.info("Starting Step 6.5: Constraint Compilation (deterministic)")
    
    statistical_constraints = []
    structural_constraints = []
    distribution_constraints = []
    other_constraints = []
    
    for constraint in constraints:
        category = constraint.get("constraint_category", "other")
        compiled = {
            "description": constraint.get("description", ""),
            "dsl_expression": constraint.get("dsl_expression", ""),
            "enforcement_type": constraint.get("enforcement_type", "application_logic"),
            "affected_entities": constraint.get("affected_entities", []),
            "affected_attributes": constraint.get("affected_attributes", []),
            "scope": constraint.get("scope", {}),
        }
        
        if category == "statistical":
            statistical_constraints.append(compiled)
        elif category == "structural":
            structural_constraints.append(compiled)
        elif category == "distribution":
            distribution_constraints.append(compiled)
        else:
            other_constraints.append(compiled)
    
    logger.info(
        f"Constraint compilation completed: {len(statistical_constraints)} statistical, "
        f"{len(structural_constraints)} structural, {len(distribution_constraints)} distribution, "
        f"{len(other_constraints)} other"
    )
    
    logical_ir_constraints = {
        "statistical_constraints": statistical_constraints,
        "structural_constraints": structural_constraints,
        "distribution_constraints": distribution_constraints,
        "other_constraints": other_constraints,
    }
    
    # Log the complete LogicalIR constraints
    logger.info("=== LOGICALIR CONSTRAINTS (Step 6.5 Output) ===")
    logger.info(json.dumps(logical_ir_constraints, indent=2, default=str))
    logger.info("=== END LOGICALIR CONSTRAINTS ===")
    
    return logical_ir_constraints

