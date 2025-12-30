"""Semantic validation tools for cardinality, ranges, and distributions."""

from typing import Dict, Any
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


@tool
def validate_cardinality_range(cardinality: str) -> bool:
    """Validate that cardinality is one of the allowed options.
    
    Args:
        cardinality: Cardinality value to validate ("small", "medium", "large", "very_large")
        
    Returns:
        True if cardinality is valid, False otherwise
    """
    valid_cardinalities = ["small", "medium", "large", "very_large"]
    return cardinality.lower() in valid_cardinalities


@tool
def validate_entity_cardinality(cardinality: str) -> bool:
    """Validate entity cardinality in a relation is "1" or "N".
    
    Args:
        cardinality: Cardinality value ("1" or "N")
        
    Returns:
        True if cardinality is valid, False otherwise
    """
    return cardinality.upper() in ["1", "N"]


@tool
def validate_range(min_val: float, max_val: float) -> Dict[str, Any]:
    """Validate that a numerical range is valid (min < max).
    
    Args:
        min_val: Minimum value
        max_val: Maximum value
        
    Returns:
        Dictionary with 'valid' (bool) and 'error' (str, if invalid) keys
    """
    if min_val >= max_val:
        return {"valid": False, "error": f"Minimum value ({min_val}) must be less than maximum value ({max_val})"}
    
    return {"valid": True, "error": None}


@tool
def validate_distribution_sum(distribution: Dict[str, float]) -> Dict[str, Any]:
    """Validate that probability distribution values sum to 1.0.
    
    Args:
        distribution: Dictionary mapping values to probabilities
        
    Returns:
        Dictionary with 'valid' (bool), 'sum' (float), and 'error' (str, if invalid) keys
    """
    total = sum(distribution.values())
    tolerance = 0.001  # Allow small floating point errors
    
    if abs(total - 1.0) > tolerance:
        return {
            "valid": False,
            "sum": total,
            "error": f"Distribution probabilities sum to {total:.4f}, expected 1.0"
        }
    
    return {"valid": True, "sum": total, "error": None}


@tool
def validate_distribution_type(distribution_type: str) -> bool:
    """Validate that distribution type is supported.
    
    Args:
        distribution_type: Distribution type name
        
    Returns:
        True if distribution type is valid, False otherwise
    """
    valid_types = [
        "uniform", "normal", "lognormal", "pareto", "zipf",
        "bernoulli", "categorical", "seasonal", "trend",
    ]
    return distribution_type.lower() in valid_types

