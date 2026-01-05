"""Deterministic similarity utilities.

These utilities are intentionally dependency-light at import time.
Any heavy ML dependencies must be imported lazily inside functions.
"""

from .attribute_similarity import propose_attribute_synonym_candidates
from .attribute_name_suggestion import suggest_attribute_candidates, suggest_attribute_name

__all__ = [
    "propose_attribute_synonym_candidates",  # For pair generation (full synonym detection)
    "suggest_attribute_candidates",  # For single-attribute lookup (suggestion)
    "suggest_attribute_name",  # For best-match lookup
]

