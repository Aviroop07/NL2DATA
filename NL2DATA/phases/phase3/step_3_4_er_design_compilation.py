"""Phase 3, Step 3.4: ER Design Compilation.

Convert all extracted information into a structured ER model.
Deterministic transformation - no LLM needed, pure data structure conversion.
"""

import json
from typing import Dict, Any, List, Optional

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False


def step_3_4_er_design_compilation(
    entities: List[Dict[str, Any]],  # All entities from Phase 1
    relations: List[Dict[str, Any]],  # All relations from Phase 1
    attributes: Dict[str, List[Dict[str, Any]]],  # entity -> attributes from Phase 2
    primary_keys: Dict[str, List[str]],  # entity -> PK from Phase 2
    foreign_keys: List[Dict[str, Any]],  # Foreign keys from Phase 2
    constraints: Optional[List[Dict[str, Any]]] = None,  # Constraints from Phase 2
) -> Dict[str, Any]:
    """
    Step 3.4 (deterministic): Compile ER design from Phase 1-2 outputs.
    
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
        >>> er_design = step_3_4_er_design_compilation(
        ...     entities=[{"name": "Customer"}],
        ...     relations=[],
        ...     attributes={"Customer": [{"name": "customer_id"}]},
        ...     primary_keys={"Customer": ["customer_id"]},
        ...     foreign_keys=[]
        ... )
        >>> len(er_design["entities"]) > 0
        True
    """
    logger.info("Starting Step 3.4: ER Design Compilation (deterministic)")
    
    # Compile entities with their attributes
    compiled_entities = []
    for entity in entities:
        entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", "")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        entity_cardinality = entity.get("cardinality") if isinstance(entity, dict) else getattr(entity, "cardinality", None)
        entity_table_type = entity.get("table_type") if isinstance(entity, dict) else getattr(entity, "table_type", None)
        
        entity_attrs = attributes.get(entity_name, [])
        entity_pk = primary_keys.get(entity_name, [])
        
        compiled_entity = {
            "name": entity_name,
            "description": entity_desc,
            "attributes": entity_attrs,
            "primary_key": entity_pk,
        }
        
        if entity_cardinality:
            compiled_entity["cardinality"] = entity_cardinality
        if entity_table_type:
            compiled_entity["table_type"] = entity_table_type
        
        compiled_entities.append(compiled_entity)
    
    # Compile relations (already in good format, but ensure consistency)
    compiled_relations = []
    for relation in relations:
        compiled_relation = {
            "entities": relation.get("entities", []),
            "type": relation.get("type", ""),
            "description": relation.get("description", ""),
            "arity": relation.get("arity", len(relation.get("entities", []))),
        }
        
        # Add cardinality if available and not None
        entity_cardinalities = relation.get("entity_cardinalities")
        if entity_cardinalities is not None:
            compiled_relation["entity_cardinalities"] = entity_cardinalities
        
        # Add participation if available and not None
        entity_participations = relation.get("entity_participations")
        if entity_participations is not None:
            compiled_relation["entity_participations"] = entity_participations
        
        # Add reasoning if available
        if "reasoning" in relation:
            compiled_relation["reasoning"] = relation.get("reasoning")
        
        compiled_relations.append(compiled_relation)
    
    # Compile attributes dictionary (entity -> List[Attribute])
    compiled_attributes = {}
    for entity_name, attr_list in attributes.items():
        compiled_attributes[entity_name] = attr_list
    
    logger.info(
        f"ER design compilation completed: {len(compiled_entities)} entities, "
        f"{len(compiled_relations)} relations, {len(compiled_attributes)} entities with attributes"
    )
    
    er_design = {
        "entities": compiled_entities,
        "relations": compiled_relations,
        "attributes": compiled_attributes,
    }
    
    return er_design


