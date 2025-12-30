"""Phase 2, Step 2.6: Naming Convention Validation.

Deterministic validation of naming conventions, reserved keywords, and conflicts.
This step uses code-based validation, not LLM calls.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class NamingConflict(BaseModel):
    """Information about a naming conflict."""
    name: str = Field(description="The conflicting name")
    conflict_type: str = Field(description="Type of conflict: 'duplicate_entity', 'duplicate_attribute', 'reserved_keyword', 'invalid_character', 'naming_convention'")
    entities_affected: List[str] = Field(description="List of entities affected by this conflict")
    suggestion: Optional[str] = Field(default=None, description="Suggested alternative name")


class NamingViolation(BaseModel):
    """Information about a naming convention violation."""
    name: str = Field(description="The name with the violation")
    violation: str = Field(description="Description of the violation")
    suggestion: str = Field(description="Suggested fix")
    entity: Optional[str] = Field(default=None, description="Entity this violation belongs to")


class NamingValidationOutput(BaseModel):
    """Output structure for naming convention validation."""
    naming_conflicts: List[NamingConflict] = Field(
        default_factory=list,
        description="List of naming conflicts found"
    )
    naming_violations: List[NamingViolation] = Field(
        default_factory=list,
        description="List of naming convention violations found"
    )
    validation_passed: bool = Field(
        description="Whether validation passed (True if no conflicts or violations)"
    )
    summary: str = Field(description="Summary of validation results")


# Common SQL reserved keywords (subset of most common ones)
SQL_RESERVED_KEYWORDS = {
    "select", "from", "where", "insert", "update", "delete", "create", "drop", "alter",
    "table", "index", "view", "database", "schema", "constraint", "primary", "key",
    "foreign", "references", "unique", "not", "null", "default", "check", "and", "or",
    "as", "on", "join", "inner", "left", "right", "full", "outer", "union", "order", "by",
    "group", "having", "distinct", "count", "sum", "avg", "max", "min", "case", "when",
    "then", "else", "end", "if", "exists", "in", "like", "between", "is", "true", "false",
    "int", "integer", "varchar", "char", "text", "decimal", "numeric", "float", "double",
    "date", "time", "timestamp", "boolean", "bool", "enum", "set", "auto_increment",
    "limit", "offset", "top", "desc", "asc", "like", "ilike", "similar", "to",
}


def _normalize_name(name: str) -> str:
    """Normalize name for comparison (lowercase, strip whitespace)."""
    return name.lower().strip()


def _check_reserved_keyword(name: str) -> bool:
    """Check if name is a SQL reserved keyword."""
    normalized = _normalize_name(name)
    return normalized in SQL_RESERVED_KEYWORDS


def _check_naming_convention(name: str) -> tuple[bool, Optional[str]]:
    """
    Check if name follows common database naming conventions.
    
    Conventions checked:
    - Starts with letter or underscore
    - Contains only letters, digits, underscores
    - Not empty
    - Reasonable length (not too long)
    
    Returns:
        (is_valid, violation_message)
    """
    if not name or not name.strip():
        return False, "Name cannot be empty"
    
    normalized = name.strip()
    
    # Check first character
    if not (normalized[0].isalpha() or normalized[0] == '_'):
        return False, "Name must start with a letter or underscore"
    
    # Check all characters are valid
    if not all(c.isalnum() or c == '_' for c in normalized):
        return False, "Name can only contain letters, digits, and underscores"
    
    # Check length (reasonable limit)
    if len(normalized) > 64:  # Common database limit
        return False, f"Name too long (max 64 characters, got {len(normalized)})"
    
    if len(normalized) < 1:
        return False, "Name must be at least 1 character"
    
    return True, None


def step_2_6_naming_convention_validation(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> list of attribute names
) -> Dict[str, Any]:
    """
    Step 2.6: Validate naming conventions for entities and attributes.
    
    This is a deterministic validation step (no LLM calls). It checks:
    - Duplicate entity names
    - Duplicate attribute names within entities
    - SQL reserved keywords
    - Naming convention compliance (valid characters, length, etc.)
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to their attribute name lists
        
    Returns:
        dict: Validation result with naming_conflicts, naming_violations, validation_passed, and summary
        
    Example:
        >>> result = step_2_6_naming_convention_validation(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": ["name", "email"]}
        ... )
        >>> result["validation_passed"]
        True
    """
    logger.info("Starting Step 2.6: Naming Convention Validation (deterministic)")
    
    naming_conflicts: List[Dict[str, Any]] = []
    naming_violations: List[Dict[str, Any]] = []
    
    # Collect all entity names and check for duplicates
    entity_names = []
    entity_name_map = {}  # normalized -> list of original names
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_names.append(entity_name)
        normalized = _normalize_name(entity_name)
        if normalized in entity_name_map:
            entity_name_map[normalized].append(entity_name)
        else:
            entity_name_map[normalized] = [entity_name]
    
    # Check for duplicate entity names (case-insensitive)
    for normalized, original_names in entity_name_map.items():
        if len(original_names) > 1:
            # Duplicate entity name found (case-insensitive match)
            naming_conflicts.append({
                "name": original_names[0],
                "conflict_type": "duplicate_entity",
                "entities_affected": original_names,
                "suggestion": f"{original_names[0]}_2"
            })
    
    # Check each entity name
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        
        # Check reserved keyword
        if _check_reserved_keyword(entity_name):
            naming_conflicts.append({
                "name": entity_name,
                "conflict_type": "reserved_keyword",
                "entities_affected": [entity_name],
                "suggestion": f"{entity_name}_tbl"
            })
        
        # Check naming convention
        is_valid, violation_msg = _check_naming_convention(entity_name)
        if not is_valid:
            naming_violations.append({
                "name": entity_name,
                "violation": violation_msg or "Naming convention violation",
                "suggestion": entity_name.replace(" ", "_").replace("-", "_"),
                "entity": entity_name
            })
    
    # Check each attribute name
    all_attribute_names = {}  # normalized -> (entity, original_name)
    for entity_name, attributes in entity_attributes.items():
        seen_in_entity = set()
        
        for attr_name in attributes:
            normalized_attr = _normalize_name(attr_name)
            
            # Check for duplicate within entity
            if normalized_attr in seen_in_entity:
                naming_conflicts.append({
                    "name": attr_name,
                    "conflict_type": "duplicate_attribute",
                    "entities_affected": [entity_name],
                    "suggestion": f"{attr_name}_2"
                })
            seen_in_entity.add(normalized_attr)
            
            # Check reserved keyword
            if _check_reserved_keyword(attr_name):
                naming_conflicts.append({
                    "name": attr_name,
                    "conflict_type": "reserved_keyword",
                    "entities_affected": [entity_name],
                    "suggestion": f"{attr_name}_col"
                })
            
            # Check naming convention
            is_valid, violation_msg = _check_naming_convention(attr_name)
            if not is_valid:
                naming_violations.append({
                    "name": attr_name,
                    "violation": violation_msg or "Naming convention violation",
                    "suggestion": attr_name.replace(" ", "_").replace("-", "_"),
                    "entity": entity_name
                })
            
            # Track for cross-entity duplicate detection (optional - can be same name in different entities)
            # We'll only flag if it's a reserved keyword or convention violation
    
    # Build summary
    conflict_count = len(naming_conflicts)
    violation_count = len(naming_violations)
    validation_passed = conflict_count == 0 and violation_count == 0
    
    if validation_passed:
        summary = f"Naming validation passed: {len(entity_names)} entities and all attributes validated successfully"
    else:
        summary = (
            f"Naming validation found {conflict_count} conflict(s) and {violation_count} violation(s). "
            f"Please review and fix before proceeding."
        )
    
    logger.info(f"Naming convention validation completed: {conflict_count} conflicts, {violation_count} violations")
    if not validation_passed:
        logger.warning(f"Naming validation failed: {summary}")
    
    # Convert to Pydantic models for structured output
    conflicts = [NamingConflict(**conflict) for conflict in naming_conflicts]
    violations = [NamingViolation(**violation) for violation in naming_violations]
    
    result = NamingValidationOutput(
        naming_conflicts=conflicts,
        naming_violations=violations,
        validation_passed=validation_passed,
        summary=summary
    )
    
    return result.model_dump()

