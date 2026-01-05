"""Schema-anchored validation functions for LLM outputs.

These functions validate that LLM outputs use correct entity/attribute names
from the canonical schema, with similarity-based suggestions for corrections.
"""

from typing import Dict, Any, List, Set, Optional
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.similarity.attribute_name_suggestion import suggest_attribute_name, suggest_attribute_candidates
from NL2DATA.phases.phase1.utils.data_extraction import extract_attribute_name

logger = get_logger(__name__)


def validate_entity_names(
    output: Dict[str, Any],
    allowed_entities: Set[str],
    context: str = "output"
) -> Dict[str, Any]:
    """
    Validate all entity names in output against canonical schema.
    
    Args:
        output: LLM output dictionary that may contain entity names
        allowed_entities: Set of valid entity names from canonical schema
        context: Context string for logging (e.g., "functional_dependency", "information_completeness")
        
    Returns:
        Dictionary with:
        - valid: bool - Whether all entity names are valid
        - errors: List[str] - List of validation errors
        - suggestions: Dict[str, str] - Mapping of invalid names to suggested corrections
        - corrected_output: Dict[str, Any] - Output with corrected entity names (if auto-correct enabled)
    """
    errors: List[str] = []
    suggestions: Dict[str, str] = {}
    
    # Extract entity names from common output structures
    entity_names_to_check: List[str] = []
    
    # Check for entities in various output formats
    if "entities" in output:
        entities = output.get("entities", [])
        if isinstance(entities, list):
            for entity in entities:
                if isinstance(entity, dict):
                    entity_name = entity.get("name", "")
                elif isinstance(entity, str):
                    entity_name = entity
                else:
                    continue
                if entity_name:
                    entity_names_to_check.append(entity_name)
    
    # Check for entity names in missing_entities
    if "missing_entities" in output:
        missing = output.get("missing_entities", [])
        if isinstance(missing, list):
            entity_names_to_check.extend([e for e in missing if isinstance(e, str)])
    
    # Check for entity names in missing_relations
    if "missing_relations" in output:
        missing_rels = output.get("missing_relations", [])
        if isinstance(missing_rels, list):
            for rel in missing_rels:
                if isinstance(rel, dict):
                    rel_entities = rel.get("entities", [])
                    if isinstance(rel_entities, list):
                        entity_names_to_check.extend([e for e in rel_entities if isinstance(e, str)])
    
    # Check for entity names in missing_intrinsic_attributes
    if "missing_intrinsic_attributes" in output:
        missing_attrs = output.get("missing_intrinsic_attributes", [])
        if isinstance(missing_attrs, list):
            for attr in missing_attrs:
                if isinstance(attr, dict):
                    entity_name = attr.get("entity", "")
                    if entity_name:
                        entity_names_to_check.append(entity_name)
    
    # Check for entity names in missing_derived_attributes
    if "missing_derived_attributes" in output:
        missing_derived = output.get("missing_derived_attributes", [])
        if isinstance(missing_derived, list):
            for attr in missing_derived:
                if isinstance(attr, dict):
                    entity_name = attr.get("entity", "")
                    if entity_name:
                        entity_names_to_check.append(entity_name)
    
    # Validate each entity name
    for entity_name in entity_names_to_check:
        if not entity_name:
            continue
        
        # Case-insensitive check
        entity_name_lc = entity_name.lower()
        allowed_lc = {e.lower() for e in allowed_entities}
        
        if entity_name_lc not in allowed_lc:
            # Try to find suggestion
            suggestion = suggest_attribute_name(entity_name, list(allowed_entities), threshold=0.7)
            if suggestion:
                suggestions[entity_name] = suggestion
                errors.append(
                    f"Invalid entity name '{entity_name}' in {context}. "
                    f"Did you mean '{suggestion}'?"
                )
            else:
                errors.append(
                    f"Invalid entity name '{entity_name}' in {context}. "
                    f"Not found in schema and no similar entity found."
                )
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "suggestions": suggestions,
    }


def validate_attribute_names(
    output: Dict[str, Any],
    schema_state: Dict[str, Any],
    context: str = "output"
) -> Dict[str, Any]:
    """
    Validate all attribute names in output against canonical schema with similarity suggestions.
    
    Args:
        output: LLM output dictionary that may contain attribute names
        schema_state: Schema state with entities and attributes
        context: Context string for logging
        
    Returns:
        Dictionary with:
        - valid: bool - Whether all attribute names are valid
        - errors: List[str] - List of validation errors
        - suggestions: Dict[str, Dict[str, str]] - Mapping of entity.attribute to suggested corrections
    """
    errors: List[str] = []
    suggestions: Dict[str, Dict[str, str]] = {}  # entity -> {invalid_attr: suggested_attr}
    
    # Extract entities and attributes from schema_state
    entities = schema_state.get("entities", [])
    attributes = schema_state.get("attributes", {})  # entity -> List[Dict[str, Any]]
    
    # Build entity name set
    entity_names = {e.get("name", "") if isinstance(e, dict) else str(e) for e in entities}
    entity_names = {e for e in entity_names if e}
    
    # Build attribute name sets per entity
    attr_names_by_entity: Dict[str, Set[str]] = {}
    for entity_name, attr_list in attributes.items():
        if entity_name not in entity_names:
            continue
        attr_names = set()
        for attr in attr_list or []:
            attr_name = extract_attribute_name(attr)
            if attr_name:
                attr_names.add(attr_name.lower())
        attr_names_by_entity[entity_name] = attr_names
    
    # Check for attribute names in missing_intrinsic_attributes
    if "missing_intrinsic_attributes" in output:
        missing_attrs = output.get("missing_intrinsic_attributes", [])
        if isinstance(missing_attrs, list):
            for attr in missing_attrs:
                if not isinstance(attr, dict):
                    continue
                entity_name = attr.get("entity", "")
                attr_name = attr.get("attribute", "")
                
                if not entity_name or not attr_name:
                    continue
                
                # Check entity exists
                entity_name_lc = entity_name.lower()
                valid_entity = any(e.lower() == entity_name_lc for e in entity_names)
                
                if not valid_entity:
                    errors.append(
                        f"Invalid entity '{entity_name}' for attribute '{attr_name}' in {context}"
                    )
                    continue
                
                # Find matching entity (case-insensitive)
                matching_entity = next((e for e in entity_names if e.lower() == entity_name_lc), None)
                if not matching_entity:
                    continue
                
                # Check attribute exists
                attr_name_lc = attr_name.lower()
                valid_attrs = attr_names_by_entity.get(matching_entity, set())
                
                if attr_name_lc not in valid_attrs:
                    # Try to find suggestion
                    if matching_entity in attr_names_by_entity:
                        suggestion = suggest_attribute_name(
                            attr_name,
                            list(attr_names_by_entity[matching_entity]),
                            threshold=0.7
                        )
                        if suggestion:
                            if matching_entity not in suggestions:
                                suggestions[matching_entity] = {}
                            suggestions[matching_entity][attr_name] = suggestion
                            errors.append(
                                f"Invalid attribute name '{entity_name}.{attr_name}' in {context}. "
                                f"Did you mean '{entity_name}.{suggestion}'?"
                            )
                        else:
                            errors.append(
                                f"Invalid attribute name '{entity_name}.{attr_name}' in {context}. "
                                f"Not found in schema and no similar attribute found."
                            )
    
    # Check for attribute names in missing_derived_attributes
    if "missing_derived_attributes" in output:
        missing_derived = output.get("missing_derived_attributes", [])
        if isinstance(missing_derived, list):
            for attr in missing_derived:
                if not isinstance(attr, dict):
                    continue
                entity_name = attr.get("entity", "")
                attr_name = attr.get("attribute", "")
                
                if not entity_name or not attr_name:
                    continue
                
                # Similar validation as above
                entity_name_lc = entity_name.lower()
                matching_entity = next((e for e in entity_names if e.lower() == entity_name_lc), None)
                
                if not matching_entity:
                    errors.append(
                        f"Invalid entity '{entity_name}' for derived attribute '{attr_name}' in {context}"
                    )
                    continue
                
                attr_name_lc = attr_name.lower()
                valid_attrs = attr_names_by_entity.get(matching_entity, set())
                
                if attr_name_lc not in valid_attrs:
                    # For derived attributes, we might want to allow new names (they're being created)
                    # But we can still check if a similar attribute exists
                    if matching_entity in attr_names_by_entity:
                        suggestion = suggest_attribute_name(
                            attr_name,
                            list(attr_names_by_entity[matching_entity]),
                            threshold=0.7
                        )
                        if suggestion:
                            logger.debug(
                                f"Derived attribute '{entity_name}.{attr_name}' is similar to existing "
                                f"attribute '{entity_name}.{suggestion}' - ensure this is intentional"
                            )
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "suggestions": suggestions,
    }


def validate_entity_attribute_consistency(
    entities: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]]
) -> List[str]:
    """
    Validate all attributes belong to valid entities.
    
    Args:
        entities: List of entity dictionaries
        attributes: Dictionary mapping entity names to their attributes
        
    Returns:
        List of error messages (empty if all valid)
    """
    errors: List[str] = []
    entity_names = {e.get("name", "") if isinstance(e, dict) else str(e) for e in entities}
    entity_names = {e for e in entity_names if e}
    
    for entity_name, attrs in attributes.items():
        if entity_name not in entity_names:
            errors.append(f"Attributes defined for non-existent entity: {entity_name}")
    
    return errors


def validate_phase_transition(
    from_phase: int,
    to_phase: int,
    state: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate state is consistent for phase transition.
    
    Args:
        from_phase: Source phase number
        to_phase: Target phase number
        state: Current pipeline state
        
    Returns:
        Dictionary with:
        - valid: bool - Whether state is consistent
        - errors: List[str] - List of consistency errors
        - warnings: List[str] - List of consistency warnings
    """
    errors: List[str] = []
    warnings: List[str] = []
    
    # Extract entities and attributes from state
    entities = state.get("entities", [])
    attributes = state.get("attributes", {})
    
    # Check entity-attribute consistency
    entity_attr_errors = validate_entity_attribute_consistency(entities, attributes)
    if entity_attr_errors:
        errors.extend(entity_attr_errors)
    
    # Check entity names consistency (no duplicates)
    entity_names = []
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        if entity_name:
            entity_names.append(entity_name)
    
    seen_entities = set()
    for entity_name in entity_names:
        entity_name_lc = entity_name.lower()
        if entity_name_lc in seen_entities:
            errors.append(f"Duplicate entity name (case-insensitive): {entity_name}")
        seen_entities.add(entity_name_lc)
    
    # Check attribute names consistency per entity
    for entity_name, attr_list in attributes.items():
        if not attr_list:
            continue
        
        attr_names = []
        for attr in attr_list:
            attr_name = extract_attribute_name(attr)
            if attr_name:
                attr_names.append(attr_name.lower())
        
        seen_attrs = set()
        for attr_name in attr_names:
            if attr_name in seen_attrs:
                warnings.append(f"Duplicate attribute name in {entity_name}: {attr_name}")
            seen_attrs.add(attr_name)
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }
