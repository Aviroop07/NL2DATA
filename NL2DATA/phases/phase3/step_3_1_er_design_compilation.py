"""Phase 3, Step 3.1: ER Design Compilation.

Convert all extracted information into a structured ER model.
Deterministic transformation - no LLM needed, pure data structure conversion.
"""

import json
from typing import List, Optional, Dict
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class CompiledEntity(BaseModel):
    """Compiled entity with attributes and primary key."""
    name: str = Field(description="Entity name")
    description: str = Field(description="Entity description")
    attributes: List = Field(default_factory=list, description="List of attributes for this entity")
    primary_key: List[str] = Field(default_factory=list, description="Primary key attributes")
    cardinality: Optional[str] = Field(default=None, description="Entity cardinality if available")
    table_type: Optional[str] = Field(default=None, description="Table type (fact/dimension) if available")

    model_config = ConfigDict(extra="forbid")


class CompiledRelation(BaseModel):
    """Compiled relation with entities and metadata."""
    entities: List[str] = Field(description="List of entity names in this relation")
    type: str = Field(description="Relation type")
    description: str = Field(description="Relation description")
    arity: int = Field(description="Number of entities in the relation")
    entity_cardinalities: Optional[Dict[str, str]] = Field(default=None, description="Entity cardinalities if available")
    entity_participations: Optional[Dict[str, str]] = Field(default=None, description="Entity participations if available")
    reasoning: Optional[str] = Field(default=None, description="Reasoning if available")

    model_config = ConfigDict(extra="forbid")


class EntityAttributesEntry(BaseModel):
    """Entry mapping an entity name to its attributes."""
    entity_name: str = Field(description="Name of the entity")
    attributes: List = Field(default_factory=list, description="List of attributes for this entity")

    model_config = ConfigDict(extra="forbid")


class ERDesignCompilationOutput(BaseModel):
    """Output structure for ER design compilation."""
    entities: List[CompiledEntity] = Field(
        default_factory=list,
        description="List of compiled entities with attributes and primary keys"
    )
    relations: List[CompiledRelation] = Field(
        default_factory=list,
        description="List of compiled relations"
    )
    entity_attributes: List[EntityAttributesEntry] = Field(
        default_factory=list,
        description="List of entity-attribute mappings"
    )

    model_config = ConfigDict(extra="forbid")


# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False


def step_3_1_er_design_compilation(
    entities: List,  # All entities from Phase 1
    relations: List,  # All relations from Phase 1
    attributes,  # entity -> attributes from Phase 2 (Dict[str, List] or similar)
    primary_keys,  # entity -> PK from Phase 2 (Dict[str, List[str]] or similar)
    foreign_keys: List,  # Foreign keys from Phase 2
    constraints: Optional[List] = None,  # Constraints from Phase 2
) -> ERDesignCompilationOutput:
    """
    Step 3.1 (deterministic): Compile ER design from Phase 1-2 outputs.
    
    This is a deterministic transformation that converts all extracted information
    into a structured ER model format. No LLM call needed.
    
    Args:
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        constraints: Optional list of constraints from Phase 2
        
    Returns:
        dict: ER design structure with entities, relations, and attributes
        
    Example:
        >>> er_design = step_3_1_er_design_compilation(
        ...     entities=[{"name": "Customer"}],
        ...     relations=[],
        ...     attributes={"Customer": [{"name": "customer_id"}]},
        ...     primary_keys={"Customer": ["customer_id"]},
        ...     foreign_keys=[]
        ... )
        >>> len(er_design["entities"]) > 0
        True
    """
    logger.info("Starting Step 3.1: ER Design Compilation (deterministic)")
    
    # Convert attributes and primary_keys to dicts if they're not already
    if not isinstance(attributes, dict):
        attributes = dict(attributes) if hasattr(attributes, '__iter__') else {}
    if not isinstance(primary_keys, dict):
        primary_keys = dict(primary_keys) if hasattr(primary_keys, '__iter__') else {}
    
    # Compile entities with their attributes
    compiled_entities = []
    for entity in entities:
        entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", "")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        entity_cardinality = entity.get("cardinality") if isinstance(entity, dict) else getattr(entity, "cardinality", None)
        entity_table_type = entity.get("table_type") if isinstance(entity, dict) else getattr(entity, "table_type", None)
        
        entity_attrs = attributes.get(entity_name, [])
        entity_pk = primary_keys.get(entity_name, [])
        
        compiled_entity = CompiledEntity(
            name=entity_name,
            description=entity_desc,
            attributes=entity_attrs,
            primary_key=entity_pk,
            cardinality=entity_cardinality,
            table_type=entity_table_type,
        )
        
        compiled_entities.append(compiled_entity)
    
    # Compile relations (already in good format, but ensure consistency)
    compiled_relations = []
    for relation in relations:
        rel_entities = relation.get("entities", []) if isinstance(relation, dict) else getattr(relation, "entities", [])
        rel_type = relation.get("type", "") if isinstance(relation, dict) else getattr(relation, "type", "")
        rel_desc = relation.get("description", "") if isinstance(relation, dict) else getattr(relation, "description", "")
        rel_arity = relation.get("arity", len(rel_entities)) if isinstance(relation, dict) else getattr(relation, "arity", len(rel_entities))
        
        # Add cardinality if available
        entity_cardinalities = relation.get("entity_cardinalities") if isinstance(relation, dict) else getattr(relation, "entity_cardinalities", None)
        entity_participations = relation.get("entity_participations") if isinstance(relation, dict) else getattr(relation, "entity_participations", None)
        reasoning = relation.get("reasoning") if isinstance(relation, dict) else getattr(relation, "reasoning", None)
        
        # Convert empty dicts to None for Optional fields (Pydantic handles None correctly)
        # But ensure we have proper dict types if values exist
        if entity_cardinalities is not None and not isinstance(entity_cardinalities, dict):
            entity_cardinalities = None
        if entity_participations is not None and not isinstance(entity_participations, dict):
            entity_participations = None
        
        compiled_relation = CompiledRelation(
            entities=rel_entities,
            type=rel_type,
            description=rel_desc,
            arity=rel_arity,
            entity_cardinalities=entity_cardinalities,
            entity_participations=entity_participations,
            reasoning=reasoning,
        )
        
        compiled_relations.append(compiled_relation)
    
    # Compile attributes as list of EntityAttributesEntry
    compiled_entity_attributes = []
    for entity_name, attr_list in attributes.items():
        compiled_entity_attributes.append(
            EntityAttributesEntry(
                entity_name=entity_name,
                attributes=attr_list
            )
        )
    
    logger.info(
        f"ER design compilation completed: {len(compiled_entities)} entities, "
        f"{len(compiled_relations)} relations, {len(compiled_entity_attributes)} entities with attributes"
    )
    
    return ERDesignCompilationOutput(
        entities=compiled_entities,
        relations=compiled_relations,
        entity_attributes=compiled_entity_attributes,
    )
