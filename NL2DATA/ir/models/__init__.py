"""IR (Intermediate Representation) models."""

from .state import (
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
from .er_relational import (
    ERAttribute,
    EREntity,
    ERRelation,
    ERDesign,
    Column,
    ForeignKeyConstraint,
    Table,
    RelationalSchema,
    NormalizedSchema,
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
    "ERAttribute",
    "EREntity",
    "ERRelation",
    "ERDesign",
    "Column",
    "ForeignKeyConstraint",
    "Table",
    "RelationalSchema",
    "NormalizedSchema",
]

