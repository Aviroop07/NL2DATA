"""Deterministic similarity utilities.

These utilities are intentionally dependency-light at import time.
Any heavy ML dependencies must be imported lazily inside functions.
"""

from .attribute_similarity import propose_attribute_synonym_candidates

__all__ = ["propose_attribute_synonym_candidates"]

