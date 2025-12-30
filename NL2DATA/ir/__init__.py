"""Intermediate Representation (IR) models and compilation."""

from .models import (
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
from .state_utils import (
    create_empty_state,
    get_entity_names,
    get_attributes_for_entity,
    has_entity,
)

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
    "create_empty_state",
    "get_entity_names",
    "get_attributes_for_entity",
    "has_entity",
]

