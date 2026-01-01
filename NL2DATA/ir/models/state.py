"""Generation state models - accumulates all discovered information."""

from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.ir.models.relation_type import RelationType


# Supporting models for state components

class EntityInfo(BaseModel):
    """Entity information from Phase 1."""
    name: str
    description: str
    # Evidence-grounded extraction metadata (Phase 1.4+)
    mention_type: Literal["explicit", "implied"] = Field(
        description="Whether the entity was explicitly named or implied",
    )
    evidence: str = Field(description="Short verbatim snippet (≤20 words) from input supporting this entity")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score (0.0 to 1.0)")
    reasoning: Optional[str] = None
    cardinality: Optional[str] = None  # "small", "medium", "large", "very_large"
    table_type: Optional[str] = None  # "fact", "dimension"
    
    model_config = ConfigDict(extra="forbid")


class RelationInfo(BaseModel):
    """Relation information from Phase 1."""
    entities: List[str]
    type: RelationType
    description: str
    arity: int
    reasoning: Optional[str] = None
    # Optional auditability fields (Phase 1.9+)
    source: Optional[Literal["explicit_in_text", "schema_inferred"]] = None
    evidence: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    entity_cardinalities: Optional[Dict[str, str]] = None  # entity -> "1" or "N"
    entity_participations: Optional[Dict[str, str]] = None  # entity -> "total" or "partial"
    
    model_config = ConfigDict(extra="forbid")


class AttributeInfo(BaseModel):
    """Attribute information from Phase 2."""
    name: str
    description: str
    type_hint: Optional[str] = None
    reasoning: Optional[str] = None
    nullable: Optional[bool] = None
    default_value: Optional[str] = None
    is_derived: bool = False
    is_multivalued: bool = False
    
    model_config = ConfigDict(extra="forbid")


class ForeignKeyInfo(BaseModel):
    """Foreign key information from Phase 2."""
    from_entity: str
    from_attributes: List[str]
    to_entity: str
    to_attributes: List[str]
    referential_integrity: Optional[str] = None  # "CASCADE", "SET_NULL", "RESTRICT"
    realization_type: str  # "foreign_key" or "junction_table"
    
    model_config = ConfigDict(extra="forbid")


class ConstraintInfo(BaseModel):
    """Constraint information from Phases 2 and 6."""
    name: Optional[str] = None
    entity: str
    attribute: Optional[str] = None
    constraint_type: str  # "unique", "check", "not_null", "default", etc.
    condition: Optional[str] = None
    description: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class InformationNeed(BaseModel):
    """Information need from Phase 3."""
    description: str
    frequency: Optional[str] = None
    entities_involved: List[str] = []
    reasoning: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class QueryInfo(BaseModel):
    """SQL query information from Phase 5."""
    query_id: Optional[str] = None
    sql: str
    description: Optional[str] = None
    information_need: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class FunctionalDependency(BaseModel):
    """Functional dependency from Phase 4."""
    entity: str
    lhs: List[str]  # Left-hand side attributes
    rhs: List[str]  # Right-hand side attributes
    reasoning: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class DataTypeInfo(BaseModel):
    """Data type information from Phase 4."""
    sql_type: str  # "VARCHAR", "INT", "DECIMAL", etc.
    size: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    reasoning: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class ConstraintSpec(BaseModel):
    """Constraint specification from Phase 6."""
    constraint_id: Optional[str] = None
    constraint_type: str  # "statistical", "structural", "distribution"
    description: str
    affected_entities: List[str] = []
    affected_attributes: List[str] = []
    dsl_expression: Optional[str] = None
    enforcement_type: Optional[str] = None
    reasoning: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class GenerationStrategy(BaseModel):
    """Generation strategy from Phase 7."""
    distribution_type: Optional[str] = None  # "uniform", "normal", "lognormal", "categorical", etc.
    parameters: Dict[str, Any] = {}
    provider: Optional[str] = None  # "faker", "mimesis", "lookup", etc.
    provider_method: Optional[str] = None
    reasoning: Optional[str] = None
    
    model_config = ConfigDict(extra="forbid")


class GenerationState(BaseModel):
    """Complete generation state - accumulates all discovered information."""
    
    # Phase 1: Domain & Entity Discovery
    domain: Optional[str] = None
    entities: List[EntityInfo] = Field(default_factory=list)
    relations: List[RelationInfo] = Field(default_factory=list)
    
    # Phase 2: Attribute Discovery & Schema Design
    attributes: Dict[str, List[AttributeInfo]] = Field(default_factory=dict)  # entity -> attributes
    primary_keys: Dict[str, List[str]] = Field(default_factory=dict)  # entity -> PK attributes
    foreign_keys: List[ForeignKeyInfo] = Field(default_factory=list)
    constraints: List[ConstraintInfo] = Field(default_factory=list)
    
    # Phase 3: Query Requirements & Schema Refinement
    information_needs: List[InformationNeed] = Field(default_factory=list)
    sql_queries: List[QueryInfo] = Field(default_factory=list)
    
    # Phase 4: Functional Dependencies & Data Types
    functional_dependencies: List[FunctionalDependency] = Field(default_factory=list)
    data_types: Dict[str, Dict[str, DataTypeInfo]] = Field(default_factory=dict)  # entity -> attribute -> type
    categorical_attributes: Dict[str, List[str]] = Field(default_factory=dict)  # entity -> categorical attrs
    
    # Phase 5: DDL & SQL Generation
    ddl_statements: List[str] = Field(default_factory=list)
    
    # Phase 6: Constraints & Distributions
    constraint_specs: List[ConstraintSpec] = Field(default_factory=list)
    
    # Phase 7: Generation Strategies
    generation_strategies: Dict[str, Dict[str, GenerationStrategy]] = Field(
        default_factory=dict
    )  # entity -> attribute -> strategy
    
    # Metadata
    description: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)  # Flexible metadata storage
    
    model_config = ConfigDict(extra="forbid")

