"""Constraint validation and satisfiability tools."""

import re
from typing import Dict, Any, List, Optional
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


@tool
def verify_schema_components(
    entities: List[str],
    attributes: List[str],
    schema_state: Dict[str, Any]
) -> Dict[str, Any]:
    """Verify that entities and attributes exist in the schema.
    
    Args:
        entities: List of entity names to verify
        attributes: List of attribute names in format "Entity.attribute" or just "attribute"
        schema_state: Current schema state dictionary containing entities and attributes
        
    Returns:
        Dictionary with:
        - 'all_entities_exist' (bool): True if all entities exist
        - 'all_attributes_exist' (bool): True if all attributes exist
        - 'missing_entities' (List[str]): Entities that don't exist
        - 'missing_attributes' (List[str]): Attributes that don't exist
        - 'reasoning' (str): Explanation of verification results
        
    Purpose: Verifies entities and attributes affected by constraints exist in schema.
    Used in Steps 6.2-6.3 to ensure constraint scope analysis references valid components.
    """
    from NL2DATA.utils.tools.validation.existence import _check_entity_exists_impl
    
    missing_entities = []
    missing_attributes = []
    
    # Get all entity names from schema
    schema_entities = schema_state.get("entities", [])
    entity_names = set()
    for entity in schema_entities:
        if isinstance(entity, dict):
            entity_names.add(entity.get("name", "").lower())
        else:
            entity_names.add(getattr(entity, "name", "").lower())
    
    # Verify entities
    for entity in entities:
        if entity.lower() not in entity_names:
            missing_entities.append(entity)
    
    # Get all attributes from schema (entity -> attributes mapping)
    schema_attributes = schema_state.get("attributes", {})
    
    # Verify attributes
    for attr in attributes:
        # Parse "Entity.attribute" or just "attribute"
        if "." in attr:
            entity_name, attr_name = attr.split(".", 1)
            entity_attrs = schema_attributes.get(entity_name, [])
        else:
            # Check all entities for this attribute
            entity_attrs = []
            for entity_name, attrs in schema_attributes.items():
                entity_attrs.extend(attrs)
        
        # Check if attribute exists
        attr_found = False
        for schema_attr in entity_attrs:
            attr_name_to_check = attr.split(".", 1)[-1] if "." in attr else attr
            schema_attr_name = (
                schema_attr.get("name", "") if isinstance(schema_attr, dict)
                else getattr(schema_attr, "name", "")
            )
            if schema_attr_name.lower() == attr_name_to_check.lower():
                attr_found = True
                break
        
        if not attr_found:
            missing_attributes.append(attr)
    
    all_entities_exist = len(missing_entities) == 0
    all_attributes_exist = len(missing_attributes) == 0
    
    reasoning = (
        f"Verified {len(entities)} entities and {len(attributes)} attributes. "
        f"{len(missing_entities)} entities missing, {len(missing_attributes)} attributes missing."
    )
    
    return {
        "all_entities_exist": all_entities_exist,
        "all_attributes_exist": all_attributes_exist,
        "missing_entities": missing_entities,
        "missing_attributes": missing_attributes,
        "reasoning": reasoning
    }


def _extract_attribute_from_expr(expr: str) -> Optional[str]:
    """Extract attribute name from constraint expression."""
    # Simple pattern matching - extract word before comparison operators
    patterns = [
        r"(\w+)\s*[><=!]",
        r"(\w+)\s*~",
        r"(\w+)\s*IN",
    ]
    for pattern in patterns:
        match = re.search(pattern, expr)
        if match:
            return match.group(1)
    return None


def _extract_value_from_expr(expr: str) -> Optional[str]:
    """Extract value from constraint expression."""
    # Extract value after comparison operator
    patterns = [
        r"[><=!]+\s*([\w.]+)",
        r"~\s*(\w+)",
        r"IN\s*\(([^)]+)\)",
    ]
    for pattern in patterns:
        match = re.search(pattern, expr)
        if match:
            return match.group(1).strip()
    return None


@tool
def check_constraint_satisfiability(
    constraint1: Dict[str, Any],
    constraint2: Dict[str, Any],
    schema_state: Dict[str, Any]
) -> Dict[str, Any]:
    """Check if two constraints can be satisfied simultaneously.
    
    Args:
        constraint1: First constraint dictionary with 'dsl_expression' or 'condition'
        constraint2: Second constraint dictionary with 'dsl_expression' or 'condition'
        schema_state: Current schema state (for context)
        
    Returns:
        Dictionary with:
        - 'satisfiable' (bool): True if constraints can be satisfied together
        - 'conflict_type' (str): Type of conflict if not satisfiable ('range', 'equality', 'logical')
        - 'reasoning' (str): Explanation of satisfiability check
        
    Purpose: Checks if two constraints can be satisfied simultaneously using
    constraint satisfaction logic. Used in Step 6.4 for deterministic conflict detection.
    
    Note: This is a simplified satisfiability check. For complex constraints,
    a full constraint solver would be needed.
    """
    # Extract constraint expressions
    expr1 = constraint1.get("dsl_expression") or constraint1.get("condition", "")
    expr2 = constraint2.get("dsl_expression") or constraint2.get("condition", "")
    
    if not expr1 or not expr2:
        return {
            "satisfiable": True,  # Can't determine without expressions
            "conflict_type": None,
            "reasoning": "Cannot check satisfiability without constraint expressions"
        }
    
    # Simple conflict detection patterns
    conflicts = []
    conflict_type = None
    
    # Check for direct contradictions (e.g., "x > 0" vs "x < 0")
    # This is a simplified check - full implementation would parse DSL
    if ">" in expr1 and "<" in expr2:
        # Check if same attribute with contradictory ranges
        attr1 = _extract_attribute_from_expr(expr1)
        attr2 = _extract_attribute_from_expr(expr2)
        if attr1 and attr2 and attr1.lower() == attr2.lower():
            val1 = _extract_value_from_expr(expr1)
            val2 = _extract_value_from_expr(expr2)
            if val1 and val2:
                try:
                    num1 = float(val1)
                    num2 = float(val2)
                    if num1 >= num2:  # e.g., "x > 5" and "x < 3"
                        conflicts.append("Contradictory range constraints")
                        conflict_type = "range"
                except ValueError:
                    pass
    
    # Check for equality contradictions (e.g., "x = 'A'" vs "x = 'B'")
    if "=" in expr1 and "=" in expr2:
        attr1 = _extract_attribute_from_expr(expr1)
        attr2 = _extract_attribute_from_expr(expr2)
        if attr1 and attr2 and attr1.lower() == attr2.lower():
            val1 = _extract_value_from_expr(expr1)
            val2 = _extract_value_from_expr(expr2)
            if val1 and val2 and val1 != val2:
                conflicts.append("Contradictory equality constraints")
                conflict_type = "equality"
    
    satisfiable = len(conflicts) == 0
    
    reasoning = (
        f"Constraints are {'satisfiable' if satisfiable else 'not satisfiable'}. "
        f"{'; '.join(conflicts) if conflicts else 'No conflicts detected.'}"
    )
    
    return {
        "satisfiable": satisfiable,
        "conflict_type": conflict_type,
        "reasoning": reasoning
    }

