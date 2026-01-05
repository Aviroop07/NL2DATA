"""Request models for API endpoints."""

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List


class ProcessStartRequest(BaseModel):
    """Request to start NL2DATA processing."""
    nl_description: str = Field(..., min_length=1, description="Natural language description of the database")


class SuggestionsRequest(BaseModel):
    """Request for keyword suggestions."""
    nl_description: str = Field(..., min_length=1, description="Current NL description")


class SaveChangesRequest(BaseModel):
    """Request to save schema changes."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    edit_mode: str = Field(..., pattern=r"^(er_diagram|relational_schema)$")
    changes: Dict[str, Any] = Field(..., description="Changes to apply")


class ToyDataGenerationRequest(BaseModel):
    """Request to generate toy dataset."""
    job_id: str
    config: Dict[str, Any]
    state: Dict[str, Any]


class CSVGenerationRequest(BaseModel):
    """Request to generate CSV files."""
    job_id: str
    config: Dict[str, Any]
    state: Dict[str, Any]


class CheckpointProceedRequest(BaseModel):
    """Request to proceed to next checkpoint."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class DomainEditRequest(BaseModel):
    """Request to save domain edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    domain: str = Field(..., min_length=1)


class EntitiesEditRequest(BaseModel):
    """Request to save entity edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    entities: List[Dict[str, Any]] = Field(..., description="List of entities with name, description, etc.")


class RelationsEditRequest(BaseModel):
    """Request to save relation edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    relations: List[Dict[str, Any]] = Field(..., description="List of relations with entities, type, cardinalities, etc.")


class AttributesEditRequest(BaseModel):
    """Request to save attribute edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    attributes: Dict[str, List[Dict[str, Any]]] = Field(..., description="entity -> list of attributes")


class PrimaryKeysEditRequest(BaseModel):
    """Request to save primary key edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    primary_keys: Dict[str, List[str]] = Field(..., description="entity -> list of primary key attribute names")


class ERDiagramEditRequest(BaseModel):
    """Request to save ER diagram edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    er_design: Dict[str, Any] = Field(..., description="ER design structure with entities, relations, attributes")


class MultivaluedDerivedEditRequest(BaseModel):
    """Request to save multivalued/derived attributes edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    multivalued_derived: Dict[str, Any] = Field(..., description="Multivalued/derived detection results by entity")
    derived_formulas: Dict[str, Any] = Field(..., description="Derived attribute formulas (DSL) by Entity.attr")


class NullabilityEditRequest(BaseModel):
    """Request to save nullability edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    nullability: Dict[str, Any] = Field(..., description="Nullability constraints by entity")


class DatatypesEditRequest(BaseModel):
    """Request to save datatype edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    data_types: Dict[str, Any] = Field(..., description="Data types by entity (entity -> attribute_types)")

class RelationalSchemaEditRequest(BaseModel):
    """Request to save relational schema edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    relational_schema: Dict[str, Any] = Field(..., description="Relational schema structure with tables, columns, keys")


class InformationMiningEditRequest(BaseModel):
    """Request to save information mining edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    information_needs: List[Dict[str, Any]] = Field(..., description="List of information needs with description and SQL query")


class FunctionalDependenciesEditRequest(BaseModel):
    """Request to save functional dependencies edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    functional_dependencies: List[Dict[str, Any]] = Field(..., description="List of functional dependencies with lhs (list of attributes) and rhs (list of attributes)")


class ConstraintsEditRequest(BaseModel):
    """Request to save constraints edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    constraints: List[Dict[str, Any]] = Field(..., description="List of constraints with category, description, affected components, etc.")


class GenerationStrategiesEditRequest(BaseModel):
    """Request to save generation strategies edits."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    generation_strategies: Dict[str, Dict[str, Any]] = Field(..., description="Generation strategies by entity and attribute (entity -> attribute -> strategy)")



