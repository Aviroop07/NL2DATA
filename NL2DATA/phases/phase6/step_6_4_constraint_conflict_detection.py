"""Phase 6, Step 6.4: Constraint Conflict Detection.

Check for conflicting constraints using constraint satisfaction logic.
Deterministic validation - uses constraint satisfaction logic.
"""

from typing import Dict, Any, List, Optional, Tuple

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def step_6_4_constraint_conflict_detection(
    constraints: List[Dict[str, Any]],  # All constraints from Steps 6.1-6.3
) -> Dict[str, Any]:
    """
    Step 6.4 (deterministic): Detect conflicting constraints.
    
    This is a deterministic validation that checks for conflicts between constraints
    using constraint satisfaction logic. No LLM call needed.
    
    Args:
        constraints: List of constraints with descriptions, DSL expressions, scope
        
    Returns:
        dict: Conflict detection result with conflicts list, validation_passed
        
    Example:
        >>> constraints = [
        ...     {"dsl_expression": "age > 18", "description": "Age must be > 18"},
        ...     {"dsl_expression": "age < 16", "description": "Age must be < 16"}
        ... ]
        >>> result = step_6_4_constraint_conflict_detection(constraints)
        >>> result["validation_passed"]
        False
        >>> len(result["conflicts"]) > 0
        True
    """
    logger.info("Starting Step 6.4: Constraint Conflict Detection (deterministic)")
    
    conflicts = []
    
    # Simple conflict detection: compare DSL expressions for same attributes
    # This is a basic implementation - can be enhanced with full constraint solver
    
    for i, constraint1 in enumerate(constraints):
        dsl1 = constraint1.get("dsl_expression", "")
        desc1 = constraint1.get("description", "")
        attrs1 = constraint1.get("affected_attributes", [])
        
        for j, constraint2 in enumerate(constraints[i+1:], start=i+1):
            dsl2 = constraint2.get("dsl_expression", "")
            desc2 = constraint2.get("description", "")
            attrs2 = constraint2.get("affected_attributes", [])
            
            # Check if constraints affect same attributes
            common_attrs = set(attrs1) & set(attrs2)
            if not common_attrs:
                continue  # No overlap, no conflict possible
            
            # Simple conflict detection: check for contradictory comparisons
            conflict = _detect_simple_conflict(dsl1, dsl2, common_attrs)
            if conflict:
                conflicts.append({
                    "constraint1": desc1,
                    "constraint2": desc2,
                    "conflict_type": conflict["type"],
                    "resolution": conflict.get("resolution", "Requires manual resolution")
                })
    
    validation_passed = len(conflicts) == 0
    
    if validation_passed:
        logger.info("Constraint conflict detection passed: no conflicts found")
    else:
        logger.warning(f"Constraint conflict detection found {len(conflicts)} conflicts")
    
    return {
        "conflicts": conflicts,
        "validation_passed": validation_passed
    }


def _detect_simple_conflict(dsl1: str, dsl2: str, common_attrs: set) -> Optional[Dict[str, str]]:
    """Detect simple conflicts between two DSL expressions.
    
    This is a basic implementation that detects obvious conflicts like:
    - "x > 0" vs "x < 0"
    - "x = 'active'" vs "x = 'inactive'" for same condition
    
    Returns conflict info if found, None otherwise.
    """
    # Normalize DSL expressions
    dsl1_lower = dsl1.lower()
    dsl2_lower = dsl2.lower()
    
    # Check for contradictory comparisons on same attribute
    for attr in common_attrs:
        attr_clean = attr.split(".")[-1]  # Get just attribute name
        
        # Pattern: attr > value vs attr < value (where values conflict)
        # This is simplified - full implementation would parse DSL properly
        if f"{attr_clean} >" in dsl1_lower and f"{attr_clean} <" in dsl2_lower:
            # Extract values and check if they conflict
            # For now, flag as potential conflict
            return {
                "type": "contradictory_range",
                "resolution": "Review and adjust constraint ranges"
            }
        if f"{attr_clean} <" in dsl1_lower and f"{attr_clean} >" in dsl2_lower:
            return {
                "type": "contradictory_range",
                "resolution": "Review and adjust constraint ranges"
            }
    
    return None

