"""Dataset IR models - core intermediate representation structures.

DEPRECATED: This file is kept for backward compatibility.
New code should use GenerationState from state.py instead.
"""

from NL2DATA.ir.models.state import (
    GenerationState,
    EntityInfo,
    RelationInfo,
    AttributeInfo,
    ForeignKeyInfo,
    ConstraintInfo,
    InformationNeed,
    QueryInfo,
    FunctionalDependency,
    DataTypeInfo,
    ConstraintSpec,
    GenerationStrategy,
)

# Re-export for backward compatibility
__all__ = [
    "GenerationState",
    "EntityInfo",
    "RelationInfo",
    "AttributeInfo",
    "ForeignKeyInfo",
    "ConstraintInfo",
    "InformationNeed",
    "QueryInfo",
    "FunctionalDependency",
    "DataTypeInfo",
    "ConstraintSpec",
    "GenerationStrategy",
]

