"""Existence validation tools for checking if entities, attributes, and components exist in schema."""

from typing import List, Dict, Any
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


def _check_entity_exists_impl(entity: str, schema_state: Dict[str, Any]) -> bool:
    """
    Pure (non-LangChain-tool) implementation.

    IMPORTANT: Functions decorated with @tool become StructuredTool objects and are not
    safely callable from regular Python code. Internal logic should use these *_impl
    helpers to avoid "'StructuredTool' object is not callable" errors.
    """
    entities = schema_state.get("entities", [])
    if not entities:
        return False

    for e in entities:
        entity_name = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        if entity_name.lower() == entity.lower():
            return True

    return False


def _verify_entities_exist_impl(entities: List[str], schema_state: Dict[str, Any]) -> Dict[str, bool]:
    """Pure (non-LangChain-tool) implementation of verify_entities_exist."""
    result: Dict[str, bool] = {}
    for entity in entities:
        result[entity] = _check_entity_exists_impl(entity, schema_state)
    return result


@tool
def check_entity_exists(entity: str, schema_state: Dict[str, Any]) -> bool:
    """Check if an entity exists in the current schema state.
    
    Args:
        entity: Name of the entity to check
        schema_state: Current schema state dictionary containing 'entities' list
        
    Returns:
        True if entity exists, False otherwise
        
    Purpose: Prevents referencing non-existent entities in relations or attributes.
    The LLM can call this tool before finalizing its response to ensure all
    referenced entities are valid.
    """
    return _check_entity_exists_impl(entity, schema_state)


@tool
def verify_entities_exist(entities: List[str], schema_state: Dict[str, Any]) -> Dict[str, bool]:
    """Verify that all specified entities exist in the schema.
    
    Args:
        entities: List of entity names to verify
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary mapping entity name to existence status (True/False)
        
    Purpose: Validates all entities in a relation exist before creating the relation.
    """
    return _verify_entities_exist_impl(entities, schema_state)


def _validate_attributes_exist_impl(entity: str, attributes: List[str], schema_state: Dict[str, Any]) -> Dict[str, bool]:
    """Pure (non-LangChain-tool) implementation of validate_attributes_exist."""
    result: Dict[str, bool] = {}

    entity_attributes = schema_state.get("attributes", {}).get(entity, [])
    if not entity_attributes:
        for attr in attributes:
            result[attr] = False
        return result

    attr_names = set()
    for attr in entity_attributes:
        if isinstance(attr, dict):
            attr_names.add((attr.get("name", "") or "").lower())
        else:
            attr_names.add((getattr(attr, "name", "") or "").lower())

    for attr in attributes:
        result[attr] = (attr or "").lower() in attr_names

    return result


@tool
def validate_attributes_exist(entity: str, attributes: List[str], schema_state: Dict[str, Any]) -> Dict[str, bool]:
    """Validate that all specified attributes exist for the given entity.
    
    Args:
        entity: Name of the entity to check
        attributes: List of attribute names to verify
        schema_state: Current schema state dictionary containing 'attributes' mapping
        
    Returns:
        Dictionary mapping attribute name to existence status (True/False)
        
    Purpose: Prevents referencing non-existent attributes in primary keys, constraints,
    or formulas. The LLM can call this tool before finalizing its response to ensure
    all referenced attributes are valid.
    """
    return _validate_attributes_exist_impl(entity, attributes, schema_state)


def _check_schema_component_exists_impl(component_type: str, name: str, schema_state: Dict[str, Any]) -> bool:
    """Pure (non-LangChain-tool) implementation of check_schema_component_exists."""
    ct = (component_type or "").lower()
    n = (name or "").lower()

    if ct == "entity":
        return _check_entity_exists_impl(name, schema_state)

    if ct == "attribute":
        attributes = schema_state.get("attributes", {})
        for entity_attrs in attributes.values():
            for attr in entity_attrs:
                attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
                if (attr_name or "").lower() == n:
                    return True
        return False

    if ct in ("relation", "relationship"):
        relations = schema_state.get("relations", [])
        for rel in relations:
            if isinstance(rel, dict):
                rel_desc = (rel.get("description", "") or "").lower()
                rel_entities = rel.get("entities", []) or []
                if n in rel_desc or n in [(e or "").lower() for e in rel_entities]:
                    return True
        return False

    return False


@tool
def check_schema_component_exists(component_type: str, name: str, schema_state: Dict[str, Any]) -> bool:
    """Check if a schema component (entity, attribute, relation, etc.) exists.
    
    Args:
        component_type: Type of component ("entity", "attribute", "relation", "table", "column")
        name: Name of the component to check
        schema_state: Current schema state dictionary
        
    Returns:
        True if component exists, False otherwise
    """
    return _check_schema_component_exists_impl(component_type, name, schema_state)

