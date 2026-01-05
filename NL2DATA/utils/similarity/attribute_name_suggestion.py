"""Helper function for suggesting attribute names when LLM uses non-existent names.

This module provides a simple interface for attribute name suggestion based on similarity.
Used in functional dependency analysis, information completeness, and other steps.
"""

from typing import Dict, Any, List, Optional
from .attribute_similarity import (
    _lexical_jaccard,
    _char_similarity,
    _normalize_name_for_tokens,
)


def suggest_attribute_candidates(
    target: str,
    candidates: List[str],
    threshold: float = 0.7,
    max_results: int = 3,
) -> List[Dict[str, Any]]:
    """
    Propose candidate attribute names based on similarity to a target name.
    
    This is a simplified interface for single-attribute lookup (as opposed to
    the full synonym pair generation in attribute_similarity.py).
    
    Args:
        target: The attribute name that doesn't exist (e.g., "unit_price")
        candidates: List of valid attribute names to match against
        threshold: Minimum similarity score (0.0-1.0) to include
        max_results: Maximum number of suggestions to return
        
    Returns:
        List of dicts with keys: "candidate", "similarity", "reason"
        Sorted by similarity (highest first)
    """
    if not target or not candidates:
        return []
    
    target_norm = _normalize_name_for_tokens(target)
    if not target_norm:
        return []
    
    suggestions = []
    
    for candidate in candidates:
        if not candidate:
            continue
        
        candidate_norm = _normalize_name_for_tokens(candidate)
        if not candidate_norm:
            continue
        
        # Skip exact matches (case-insensitive)
        if target_norm == candidate_norm:
            continue
        
        # Calculate similarity scores
        lexical_sim = _lexical_jaccard(target, candidate)
        char_sim = _char_similarity(target, candidate)
        
        # Combined similarity (weighted average)
        # Lexical similarity is more important for attribute names
        combined_sim = (lexical_sim * 0.6) + (char_sim * 0.4)
        
        if combined_sim >= threshold:
            suggestions.append({
                "candidate": candidate,
                "similarity": round(combined_sim, 4),
                "reason": f"lexical={round(lexical_sim, 3)}, char={round(char_sim, 3)}"
            })
    
    # Sort by similarity (descending)
    suggestions.sort(key=lambda x: x["similarity"], reverse=True)
    
    return suggestions[:max_results]


def suggest_attribute_name(
    invalid_name: str,
    valid_names: List[str],
    threshold: float = 0.7,
) -> Optional[str]:
    """
    Get the best matching attribute name suggestion.
    
    Args:
        invalid_name: The non-existent attribute name
        valid_names: List of valid attribute names
        threshold: Minimum similarity threshold
        
    Returns:
        Best matching attribute name if similarity >= threshold, else None
    """
    candidates = suggest_attribute_candidates(
        target=invalid_name,
        candidates=valid_names,
        threshold=threshold,
        max_results=1
    )
    
    if candidates:
        return candidates[0]["candidate"]
    return None
