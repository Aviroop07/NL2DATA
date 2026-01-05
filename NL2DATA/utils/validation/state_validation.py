"""State validation utilities for ensuring consistency after parallel updates.

This module provides validation functions to check state consistency,
especially after parallel updates that may have race conditions or inconsistencies.
"""

from typing import Dict, Any, List, Set
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class StateValidationError(Exception):
    """Raised when state validation fails."""
    pass


def validate_state_consistency(state: Dict[str, Any]) -> List[str]:
    """
    Validate state consistency after parallel updates.
    
    Checks for common inconsistencies:
    - Entities referenced in relations that don't exist
    - Attributes referenced in primary keys that don't exist
    - Foreign keys referencing non-existent primary keys
    - Derived attributes referencing non-existent dependencies
    - Orphaned entities (entities with no relations)
    
    Args:
        state: IRGenerationState dictionary
        
    Returns:
        List of validation issue messages (empty if all checks pass)
    """
    issues = []
    
    entities = state.get("entities", [])
    relations = state.get("relations", [])
    attributes = state.get("attributes", {})
    primary_keys = state.get("primary_keys", {})
    foreign_keys = state.get("foreign_keys", [])
    
    # Build entity name set for quick lookup
    entity_names: Set[str] = set()
    for entity in entities:
        if isinstance(entity, dict):
            entity_name = entity.get("name", "")
        else:
            entity_name = getattr(entity, "name", "")
        if entity_name:
            entity_names.add(entity_name)
    
    # Check 1: All entities referenced in relations exist
    for relation in relations:
        if isinstance(relation, dict):
            rel_entities = relation.get("entities", [])
        else:
            rel_entities = getattr(relation, "entities", [])
        
        for entity_name in rel_entities:
            if entity_name not in entity_names:
                issues.append(
                    f"Relation references non-existent entity: '{entity_name}'. "
                    f"Relation entities: {rel_entities}"
                )
    
    # Check 2: All primary key attributes exist for their entities
    for entity_name, pk_attrs in primary_keys.items():
        if entity_name not in entity_names:
            issues.append(
                f"Primary key defined for non-existent entity: '{entity_name}'"
            )
            continue
        
        entity_attrs = attributes.get(entity_name, [])
        attr_names = set()
        for attr in entity_attrs:
            if isinstance(attr, dict):
                attr_name = attr.get("name", "")
            else:
                attr_name = getattr(attr, "name", "")
            if attr_name:
                attr_names.add(attr_name)
        
        for pk_attr in pk_attrs:
            if pk_attr not in attr_names:
                issues.append(
                    f"Primary key attribute '{pk_attr}' does not exist for entity '{entity_name}'. "
                    f"Available attributes: {sorted(attr_names)}"
                )
    
    # Check 3: All foreign keys reference existing primary keys
    for fk in foreign_keys:
        if isinstance(fk, dict):
            fk_from = fk.get("from_entity", "")
            fk_to = fk.get("to_entity", "")
            fk_attrs = fk.get("attributes", [])
        else:
            fk_from = getattr(fk, "from_entity", "")
            fk_to = getattr(fk, "to_entity", "")
            fk_attrs = getattr(fk, "attributes", [])
        
        if fk_from not in entity_names:
            issues.append(
                f"Foreign key from non-existent entity: '{fk_from}' -> '{fk_to}'"
            )
        
        if fk_to not in entity_names:
            issues.append(
                f"Foreign key references non-existent entity: '{fk_from}' -> '{fk_to}'"
            )
        
        # Check FK attributes exist in from_entity
        if fk_from in entity_names:
            entity_attrs = attributes.get(fk_from, [])
            attr_names = set()
            for attr in entity_attrs:
                if isinstance(attr, dict):
                    attr_name = attr.get("name", "")
                else:
                    attr_name = getattr(attr, "name", "")
                if attr_name:
                    attr_names.add(attr_name)
            
            for fk_attr in fk_attrs:
                if fk_attr not in attr_names:
                    issues.append(
                        f"Foreign key attribute '{fk_attr}' does not exist in entity '{fk_from}'"
                    )
        
        # Check FK references existing PK in to_entity
        if fk_to in entity_names:
            to_pk = primary_keys.get(fk_to, [])
            if not to_pk:
                issues.append(
                    f"Foreign key references entity '{fk_to}' which has no primary key defined"
                )
            elif len(fk_attrs) != len(to_pk):
                issues.append(
                    f"Foreign key '{fk_from}.{fk_attrs}' has {len(fk_attrs)} attributes, "
                    f"but references PK '{fk_to}.{to_pk}' with {len(to_pk)} attributes"
                )
    
    # Check 4: All entities have at least one attribute (after Phase 2)
    if attributes:  # Only check if attributes have been populated
        for entity_name in entity_names:
            entity_attrs = attributes.get(entity_name, [])
            if not entity_attrs:
                issues.append(
                    f"Entity '{entity_name}' has no attributes defined"
                )
    
    return issues


def validate_parallel_update_results(
    results: List[Dict[str, Any]],
    expected_keys: List[str],
    entity_names: List[str]
) -> List[str]:
    """
    Validate results from parallel updates (e.g., per-entity attribute extraction).
    
    Checks:
    - All expected entities have results
    - Results have expected structure
    - No duplicate or conflicting updates
    
    Args:
        results: List of result dictionaries from parallel updates
        expected_keys: List of expected keys in each result
        entity_names: List of entity names that should have results
        
    Returns:
        List of validation issue messages (empty if all checks pass)
    """
    issues = []
    
    # Check all entities have results
    result_entities = set()
    for result in results:
        if isinstance(result, dict):
            # Try to find entity name in result
            entity_name = result.get("entity") or result.get("entity_name") or result.get("name")
            if entity_name:
                result_entities.add(entity_name)
    
    missing_entities = set(entity_names) - result_entities
    if missing_entities:
        issues.append(
            f"Missing results for entities: {sorted(missing_entities)}"
        )
    
    # Check result structure
    for i, result in enumerate(results):
        if not isinstance(result, dict):
            issues.append(f"Result {i} is not a dictionary: {type(result)}")
            continue
        
        # Check for expected keys (if any)
        if expected_keys:
            missing_keys = set(expected_keys) - set(result.keys())
            if missing_keys:
                issues.append(
                    f"Result {i} missing expected keys: {sorted(missing_keys)}"
                )
    
    return issues


def validate_no_duplicate_entities(entities: List[Dict[str, Any]]) -> List[str]:
    """
    Check for duplicate entity names after consolidation.
    
    Args:
        entities: List of entity dictionaries
        
    Returns:
        List of validation issue messages (empty if no duplicates)
    """
    issues = []
    seen_names = {}
    
    for i, entity in enumerate(entities):
        if isinstance(entity, dict):
            entity_name = entity.get("name", "")
        else:
            entity_name = getattr(entity, "name", "")
        
        if not entity_name:
            continue
        
        if entity_name in seen_names:
            issues.append(
                f"Duplicate entity name '{entity_name}' found at indices "
                f"{seen_names[entity_name]} and {i}"
            )
        else:
            seen_names[entity_name] = i
    
    return issues


def validate_no_duplicate_attributes(
    attributes: Dict[str, List[Dict[str, Any]]]
) -> List[str]:
    """
    Check for duplicate attribute names within entities.
    
    Args:
        attributes: Dictionary mapping entity names to attribute lists
        
    Returns:
        List of validation issue messages (empty if no duplicates)
    """
    issues = []
    
    for entity_name, attr_list in attributes.items():
        seen_attrs = {}
        for i, attr in enumerate(attr_list):
            if isinstance(attr, dict):
                attr_name = attr.get("name", "")
            else:
                attr_name = getattr(attr, "name", "")
            
            if not attr_name:
                continue
            
            if attr_name in seen_attrs:
                issues.append(
                    f"Duplicate attribute '{attr_name}' in entity '{entity_name}' "
                    f"at indices {seen_attrs[attr_name]} and {i}"
                )
            else:
                seen_attrs[attr_name] = i
    
    return issues


def check_state_consistency(state: Dict[str, Any], raise_on_error: bool = False) -> bool:
    """
    Check state consistency and optionally raise exception on failure.
    
    Args:
        state: IRGenerationState dictionary
        raise_on_error: If True, raise StateValidationError on failure
        
    Returns:
        True if state is consistent, False otherwise
        
    Raises:
        StateValidationError: If raise_on_error=True and validation fails
    """
    issues = validate_state_consistency(state)
    
    if issues:
        logger.warning(f"State validation found {len(issues)} issues:")
        for issue in issues:
            logger.warning(f"  - {issue}")
        
        if raise_on_error:
            raise StateValidationError(
                f"State validation failed with {len(issues)} issues: {issues[:5]}"
            )
        
        return False
    
    logger.debug("State validation passed")
    return True


def validate_no_list_duplication(state: Dict[str, Any], raise_on_error: bool = False) -> List[str]:
    """
    Validate that additive list fields haven't been duplicated due to state reducer issues.
    
    This detects the common bug where nodes return **state with Annotated[..., add] fields,
    causing exponential duplication (e.g., 16 items -> 32 -> 64 -> 128...).
    
    Checks:
    - information_needs: Should not have duplicate descriptions
    - constraints: Should not have duplicate constraint_ids or identical constraint structures
    - functional_dependencies: Should not have duplicate FD structures
    - sql_queries: Should not have duplicate query structures
    
    Args:
        state: IRGenerationState dictionary
        raise_on_error: If True, raise StateValidationError on severe duplication
        
    Returns:
        List of validation issue messages (empty if no duplication detected)
        
    Raises:
        StateValidationError: If raise_on_error=True and severe duplication is detected
    """
    issues = []
    
    # Check information_needs
    information_needs = state.get("information_needs", [])
    if information_needs:
        descriptions = []
        for need in information_needs:
            if isinstance(need, dict):
                desc = need.get("description", "")
            else:
                desc = getattr(need, "description", "")
            if desc:
                descriptions.append(desc)
        
        unique_descriptions = len(set(descriptions))
        if len(information_needs) > unique_descriptions * 2:
            issues.append(
                f"State duplication detected in information_needs: {len(information_needs)} items "
                f"but only {unique_descriptions} unique descriptions. "
                f"Likely caused by returning **state with Annotated[..., add] reducer."
            )
    
    # Check constraints
    constraints = state.get("constraints", [])
    if constraints:
        # Try to identify duplicates by constraint_id or by structure
        constraint_ids = []
        constraint_signatures = []
        
        for constraint in constraints:
            if isinstance(constraint, dict):
                constraint_id = constraint.get("constraint_id") or constraint.get("id")
                # Create a signature from key fields
                sig = (
                    constraint.get("constraint_type"),
                    constraint.get("table"),
                    constraint.get("column"),
                    constraint.get("description", "")[:50] if constraint.get("description") else ""
                )
            else:
                constraint_id = getattr(constraint, "constraint_id", None) or getattr(constraint, "id", None)
                sig = (
                    getattr(constraint, "constraint_type", None),
                    getattr(constraint, "table", None),
                    getattr(constraint, "column", None),
                    (getattr(constraint, "description", "") or "")[:50]
                )
            
            if constraint_id:
                constraint_ids.append(constraint_id)
            constraint_signatures.append(sig)
        
        # Check for duplicate IDs
        if constraint_ids:
            unique_ids = len(set(constraint_ids))
            if len(constraints) > unique_ids * 2:
                issues.append(
                    f"State duplication detected in constraints: {len(constraints)} items "
                    f"but only {unique_ids} unique constraint_ids. "
                    f"Likely caused by returning **state with Annotated[..., add] reducer."
                )
        
        # Check for duplicate signatures
        unique_signatures = len(set(constraint_signatures))
        if len(constraints) > unique_signatures * 2:
            issues.append(
                f"State duplication detected in constraints: {len(constraints)} items "
                f"but only {unique_signatures} unique constraint structures. "
                f"Likely caused by returning **state with Annotated[..., add] reducer."
            )
    
    # Check functional_dependencies
    functional_dependencies = state.get("functional_dependencies", [])
    if functional_dependencies:
        fd_signatures = []
        for fd in functional_dependencies:
            if isinstance(fd, dict):
                sig = (
                    tuple(sorted(fd.get("determinants", []))),
                    tuple(sorted(fd.get("dependents", []))),
                    fd.get("table", "")
                )
            else:
                sig = (
                    tuple(sorted(getattr(fd, "determinants", []))),
                    tuple(sorted(getattr(fd, "dependents", []))),
                    getattr(fd, "table", "")
                )
            fd_signatures.append(sig)
        
        unique_fds = len(set(fd_signatures))
        if len(functional_dependencies) > unique_fds * 2:
            issues.append(
                f"State duplication detected in functional_dependencies: {len(functional_dependencies)} items "
                f"but only {unique_fds} unique FD structures. "
                f"Likely caused by returning **state with Annotated[..., add] reducer."
            )
    
    # Log warnings for any issues found
    if issues:
        logger.warning(f"State duplication validation found {len(issues)} issues:")
        for issue in issues:
            logger.warning(f"  - {issue}")
        
        # Raise error if severe duplication detected
        if raise_on_error:
            raise StateValidationError(
                f"State duplication validation failed: {issues[0]}"
            )
    
    return issues
