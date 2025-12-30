"""Utilities for extracting and formatting entity/relation data from various formats."""

from typing import Dict, Any, List, Optional, Union


def extract_entity_name(entity: Union[Dict[str, Any], Any]) -> str:
    """
    Extract entity name from dict or object.
    
    Args:
        entity: Entity as dict or object with 'name' attribute
        
    Returns:
        Entity name or "Unknown" if not found
    """
    if isinstance(entity, dict):
        return entity.get("name", "Unknown")
    else:
        return getattr(entity, "name", "Unknown")


def extract_entity_description(entity: Union[Dict[str, Any], Any], default: str = "") -> str:
    """
    Extract entity description from dict or object.
    
    Args:
        entity: Entity as dict or object with 'description' attribute
        default: Default value if description not found
        
    Returns:
        Entity description or default value
    """
    if isinstance(entity, dict):
        return entity.get("description", default)
    else:
        return getattr(entity, "description", default)


def extract_attribute_name(attr: Union[Dict[str, Any], Any]) -> str:
    """
    Extract attribute name from dict or object.
    
    Args:
        attr: Attribute as dict or object with 'name' attribute
        
    Returns:
        Attribute name or "Unknown" if not found
    """
    if isinstance(attr, dict):
        return attr.get("name", "Unknown")
    else:
        return getattr(attr, "name", "Unknown")


def extract_attribute_description(attr: Union[Dict[str, Any], Any], default: str = "") -> str:
    """
    Extract attribute description from dict or object.
    
    Args:
        attr: Attribute as dict or object with 'description' attribute
        default: Default value if description not found
        
    Returns:
        Attribute description or default value
    """
    if isinstance(attr, dict):
        return attr.get("description", default)
    else:
        return getattr(attr, "description", default)


def extract_attribute_type_hint(attr: Union[Dict[str, Any], Any]) -> str:
    """
    Extract attribute type hint from dict or object.
    
    Args:
        attr: Attribute as dict or object with 'type_hint' attribute
        
    Returns:
        Attribute type hint or empty string if not found
    """
    if isinstance(attr, dict):
        return attr.get("type_hint", "")
    else:
        return getattr(attr, "type_hint", "")


def extract_attribute_field(attr: Union[Dict[str, Any], Any], field_name: str, default: Any = None) -> Any:
    """
    Extract a field from an attribute object.
    
    Args:
        attr: Attribute object (dict or Pydantic model)
        field_name: Name of the field to extract
        default: Default value if field not found
        
    Returns:
        Field value or default
    """
    if isinstance(attr, dict):
        return attr.get(field_name, default)
    return getattr(attr, field_name, default)


def extract_entity_info(entity: Union[Dict[str, Any], Any]) -> Dict[str, str]:
    """
    Extract both name and description from entity.
    
    Args:
        entity: Entity as dict or object
        
    Returns:
        Dictionary with 'name' and 'description' keys
    """
    return {
        "name": extract_entity_name(entity),
        "description": extract_entity_description(entity),
    }


def build_entity_list_string(
    entities: List[Union[Dict[str, Any], Any]],
    include_descriptions: bool = True,
    prefix: str = "- ",
) -> str:
    """
    Build a formatted string listing entities.
    
    Args:
        entities: List of entities (dicts or objects)
        include_descriptions: Whether to include entity descriptions
        prefix: Prefix for each line (default: "- ")
        
    Returns:
        Formatted string with entity list
    """
    lines = []
    for entity in entities:
        name = extract_entity_name(entity)
        if include_descriptions:
            desc = extract_entity_description(entity)
            if desc:
                lines.append(f"{prefix}{name}: {desc}")
            else:
                lines.append(f"{prefix}{name}")
        else:
            lines.append(f"{prefix}{name}")
    return "\n".join(lines)


def build_relation_list_string(
    relations: List[Dict[str, Any]],
    include_cardinalities: Optional[Dict[tuple, Dict[str, str]]] = None,
    include_participations: Optional[Dict[tuple, Dict[str, str]]] = None,
) -> str:
    """
    Build a formatted string listing relations.
    
    Args:
        relations: List of relation dictionaries
        include_cardinalities: Optional dict mapping (sorted entity tuple) -> cardinality dict
        include_participations: Optional dict mapping (sorted entity tuple) -> participation dict
        
    Returns:
        Formatted string with relation list
    """
    lines = []
    for relation in relations:
        entities_in_rel = relation.get("entities", [])
        rel_type = relation.get("type", "unknown")
        rel_desc = relation.get("description", "")
        
        if entities_in_rel:
            rel_str = f"- {', '.join(entities_in_rel)} ({rel_type})"
            if rel_desc:
                rel_str += f": {rel_desc}"
            
            # Add cardinality and participation info if available
            key = tuple(sorted(entities_in_rel))
            info_parts = []
            
            if include_cardinalities and key in include_cardinalities:
                card_info = include_cardinalities[key]
                card_str = ", ".join(f"{e}={card_info.get(e, '?')}" for e in entities_in_rel)
                info_parts.append(f"Cardinalities: {card_str}")
            
            if include_participations and key in include_participations:
                part_info = include_participations[key]
                part_str = ", ".join(f"{e}={part_info.get(e, '?')}" for e in entities_in_rel)
                info_parts.append(f"Participations: {part_str}")
            
            if info_parts:
                rel_str += f" [{', '.join(info_parts)}]"
            
            lines.append(rel_str)
    
    return "\n".join(lines) if lines else "No relations found"


def get_entities_in_relation(relation: Dict[str, Any]) -> List[str]:
    """
    Extract entity names from a relation.
    
    Args:
        relation: Relation dictionary
        
    Returns:
        List of entity names in the relation
    """
    return relation.get("entities", [])


def find_entity_by_name(entities: List[Union[Dict[str, Any], Any]], name: str) -> Optional[Union[Dict[str, Any], Any]]:
    """
    Find an entity by name in a list of entities.
    
    Args:
        entities: List of entity objects
        name: Entity name to find
        
    Returns:
        Entity object if found, None otherwise
    """
    for entity in entities:
        if extract_entity_name(entity) == name:
            return entity
    return None


def find_attribute_by_name(attributes: List[Union[Dict[str, Any], Any]], name: str) -> Optional[Union[Dict[str, Any], Any]]:
    """
    Find an attribute by name in a list of attributes.
    
    Args:
        attributes: List of attribute objects
        name: Attribute name to find
        
    Returns:
        Attribute object if found, None otherwise
    """
    for attr in attributes:
        if extract_attribute_name(attr) == name:
            return attr
    return None

