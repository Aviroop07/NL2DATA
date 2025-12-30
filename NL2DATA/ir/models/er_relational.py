"""Pydantic models for deterministic ER -> relational compilation and normalization.

These models are used internally by deterministic steps to avoid raw dict/JSON handling.
Public step functions may still accept/return dicts for backward compatibility.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field


class ERAttribute(BaseModel):
    name: str
    description: Optional[str] = None
    type_hint: Optional[str] = None
    nullable: Optional[bool] = None
    is_derived: bool = False
    is_multivalued: bool = False
    is_composite: bool = False
    decomposition: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class EREntity(BaseModel):
    name: str
    attributes: List[ERAttribute] = Field(default_factory=list)
    primary_key: List[str] = Field(default_factory=list)
    description: Optional[str] = None

    model_config = {"extra": "allow"}


class ERRelation(BaseModel):
    entities: List[str] = Field(default_factory=list)
    type: str = ""
    description: Optional[str] = None
    arity: int = 0
    entity_cardinalities: Dict[str, Literal["1", "N"]] = Field(default_factory=dict)
    entity_participations: Dict[str, Literal["total", "partial"]] = Field(default_factory=dict)
    attributes: List[ERAttribute] = Field(default_factory=list)  # relationship attributes

    model_config = {"extra": "allow"}


class ERDesign(BaseModel):
    entities: List[EREntity] = Field(default_factory=list)
    relations: List[ERRelation] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)  # legacy; kept for compatibility

    model_config = {"extra": "allow"}


class ForeignKeyConstraint(BaseModel):
    attributes: List[str]
    references_table: str
    referenced_attributes: List[str]

    model_config = {"extra": "allow"}


class Column(BaseModel):
    name: str
    description: Optional[str] = None
    type_hint: Optional[str] = None
    nullable: Optional[bool] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references_table: Optional[str] = None
    references_attribute: Optional[str] = None
    is_unique: bool = False  # used for enforcing 1:1 and other uniqueness

    model_config = {"extra": "allow"}


class Table(BaseModel):
    name: str
    columns: List[Column] = Field(default_factory=list)
    primary_key: List[str] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyConstraint] = Field(default_factory=list)
    unique_constraints: List[List[str]] = Field(default_factory=list)
    is_junction_table: bool = False
    is_multivalued_table: bool = False

    # Normalization metadata
    is_normalized: bool = False
    is_decomposed: bool = False
    original_table: Optional[str] = None
    join_attributes: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class RelationalSchema(BaseModel):
    tables: List[Table] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class NormalizedSchema(BaseModel):
    normalized_tables: List[Table] = Field(default_factory=list)
    decomposition_steps: List[str] = Field(default_factory=list)
    attribute_mapping: Dict[str, str] = Field(default_factory=dict)
    dependency_preservation_report: Dict[str, bool] = Field(default_factory=dict)
    key_preservation_report: Dict[str, bool] = Field(default_factory=dict)
    join_paths: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}


