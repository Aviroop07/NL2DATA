"""Deterministic type inference for derived attributes from DSL formulas."""

from typing import Dict, Any, Optional, Tuple, List
import re

logger = None  # Will be set when imported


def _infer_type_from_formula(
    formula: str,
    dependency_types: Dict[str, Dict[str, Any]],  # "entity.attribute" -> type_info
) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
    """
    Infer SQL type from a DSL formula deterministically.
    
    Args:
        formula: DSL formula expression (e.g., "quantity * unit_price")
        dependency_types: Dictionary mapping dependency attribute keys to their type info
        
    Returns:
        Tuple of (sql_type, size, precision, scale)
    """
    if not formula:
        return "VARCHAR", 255, None, None
    
    formula_lower = formula.lower().strip()
    
    # Check for arithmetic operations (+, -, *, /)
    arithmetic_ops = ["+", "-", "*", "/"]
    has_arithmetic = any(op in formula for op in arithmetic_ops)
    
    # Check for string operations (CONCAT, ||, etc.)
    string_ops = ["concat", "||", "substring", "upper", "lower"]
    has_string_op = any(op in formula_lower for op in string_ops)
    
    # Check for date operations
    date_ops = ["datediff", "dateadd", "extract", "current_date", "current_timestamp"]
    has_date_op = any(op in formula_lower for op in date_ops)
    
    # Check for boolean operations
    boolean_ops = ["if", "case", "when", "then", "else", "and", "or", "not", ">", "<", "=", "!="]
    has_boolean_op = any(op in formula_lower for op in boolean_ops)
    
    # Analyze dependency types
    dep_types = []
    for dep_key, type_info in dependency_types.items():
        dep_type = type_info.get("type", "").upper()
        dep_types.append(dep_type)
    
    # Type inference logic
    if has_boolean_op and not has_arithmetic:
        # Boolean expression
        return "BOOLEAN", None, None, None
    
    if has_date_op:
        # Date arithmetic
        if "datediff" in formula_lower or "extract" in formula_lower:
            return "INT", None, None, None  # Date difference returns integer
        return "DATE", None, None, None
    
    if has_string_op:
        # String operations
        max_length = 0
        for dep_key, type_info in dependency_types.items():
            dep_size = type_info.get("size")
            if dep_size:
                max_length = max(max_length, dep_size)
        return "VARCHAR", max_length if max_length > 0 else 255, None, None
    
    if has_arithmetic:
        # Numeric operations
        has_decimal = False
        has_float = False
        max_precision = 0
        max_scale = 0
        
        for dep_key, type_info in dependency_types.items():
            dep_type = type_info.get("type", "").upper()
            if dep_type in ("DECIMAL", "NUMERIC"):
                has_decimal = True
                precision = type_info.get("precision", 0)
                scale = type_info.get("scale", 0)
                max_precision = max(max_precision, precision)
                max_scale = max(max_scale, scale)
            elif dep_type in ("DOUBLE", "FLOAT", "REAL"):
                has_float = True
            elif dep_type in ("INT", "INTEGER", "BIGINT", "SMALLINT"):
                # Integer types
                pass
        
        # Determine result type
        if has_float:
            return "DOUBLE", None, None, None
        elif has_decimal:
            # For multiplication, precision increases; for addition, precision stays similar
            if "*" in formula or "/" in formula:
                # Multiplication/division: increase precision
                result_precision = min(max_precision * 2, 38)  # Cap at 38
                result_scale = max_scale
            else:
                # Addition/subtraction: keep precision
                result_precision = max_precision
                result_scale = max_scale
            return "DECIMAL", None, result_precision if result_precision > 0 else 12, result_scale if result_scale is not None else 2
        else:
            # All integers
            return "BIGINT", None, None, None
    
    # Default: try to infer from dependency types
    if dep_types:
        # Use the first dependency's type as a fallback
        first_dep_type = dep_types[0]
        if first_dep_type in ("DECIMAL", "NUMERIC"):
            return "DECIMAL", None, 12, 2
        elif first_dep_type in ("DOUBLE", "FLOAT"):
            return "DOUBLE", None, None, None
        elif first_dep_type in ("INT", "INTEGER", "BIGINT"):
            return "BIGINT", None, None, None
        elif first_dep_type in ("VARCHAR", "CHAR", "TEXT"):
            return "VARCHAR", 255, None, None
        elif first_dep_type == "BOOLEAN":
            return "BOOLEAN", None, None, None
    
    # Ultimate fallback
    return "VARCHAR", 255, None, None


def infer_derived_attribute_type(
    entity_name: str,
    attribute_name: str,
    formula: str,
    dependencies: List[str],  # List of dependency keys ("entity.attribute")
    all_data_types: Dict[str, Dict[str, Any]],  # "entity.attribute" -> type_info
) -> Dict[str, Any]:
    """
    Deterministically infer SQL type for a derived attribute from its formula.
    
    Args:
        entity_name: Name of the entity
        attribute_name: Name of the derived attribute
        formula: DSL formula expression
        dependencies: List of dependency attribute keys
        all_data_types: Dictionary of all assigned data types
        
    Returns:
        dict: Type info with type, size, precision, scale, constraints, reasoning
    """
    # Get types of dependencies
    dependency_types = {}
    for dep_key in dependencies:
        if dep_key in all_data_types:
            dependency_types[dep_key] = all_data_types[dep_key]
    
    sql_type, size, precision, scale = _infer_type_from_formula(formula, dependency_types)
    
    # Build type string for reasoning
    type_str = sql_type
    if sql_type in ("VARCHAR", "CHAR") and size:
        type_str = f"{sql_type}({size})"
    elif sql_type in ("DECIMAL", "NUMERIC") and precision is not None:
        if scale is not None:
            type_str = f"{sql_type}({precision},{scale})"
        else:
            type_str = f"{sql_type}({precision})"
    
    reasoning = f"Deterministically inferred {type_str} from formula '{formula}'"
    if dependency_types:
        dep_type_strs = [f"{k}: {v.get('type', 'UNKNOWN')}" for k, v in dependency_types.items()]
        reasoning += f" based on dependency types: {', '.join(dep_type_strs)}"
    
    return {
        "type": sql_type,
        "size": size,
        "precision": precision,
        "scale": scale,
        "constraints": {},
        "reasoning": reasoning,
    }
