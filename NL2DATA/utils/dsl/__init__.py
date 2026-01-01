"""Domain-specific language (DSL) for derivations, decompositions, and generators.

This package provides:
- A formal grammar
- A parser (deterministic)
- A validator (deterministic)
"""

from .validator import validate_dsl_expression_strict

__all__ = ["validate_dsl_expression_strict"]

