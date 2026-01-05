"""Schema freeze point validation utilities.

Validates that the frozen schema (from Phase 4) is not modified in subsequent phases.
The frozen schema represents the immutable foundation that analytics phases build upon.
"""

from typing import Dict, Any, List, Optional
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def validate_frozen_schema_immutability(
    phase: int,
    state: Dict[str, Any],
    modifications: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Validate that frozen schema is not being modified in phases after Phase 4.
    
    Args:
        phase: Current phase number (should be > 4)
        state: Current pipeline state
        modifications: Optional dict of modifications being made (for detection)
        
    Returns:
        dict with keys:
            - valid: bool - Whether frozen schema remains unchanged
            - warnings: List[str] - Warnings about potential modifications
            - errors: List[str] - Errors if frozen schema was modified
    """
    if phase <= 4:
        # Phase 4 and earlier can modify schema - no validation needed
        return {
            "valid": True,
            "warnings": [],
            "errors": []
        }
    
    metadata = state.get("metadata", {})
    frozen_schema = metadata.get("frozen_schema", {})
    
    if not frozen_schema:
        # No frozen schema yet - this is an error if we're past Phase 4
        return {
            "valid": False,
            "warnings": [],
            "errors": [
                f"Phase {phase}: No frozen schema found. Phase 4 must complete successfully before Phase {phase}."
            ]
        }
    
    warnings = []
    errors = []
    
    # Check if entities/attributes are being modified (should not happen after Phase 4)
    current_entities = state.get("entities", [])
    current_attributes = state.get("attributes", {})
    
    # Extract entity names from frozen schema
    frozen_tables = frozen_schema.get("tables", []) or frozen_schema.get("normalized_tables", [])
    frozen_entity_names = {table.get("name", "") for table in frozen_tables if table.get("name", "")}
    
    # Check if new entities are being added (should only happen via Phase 5.3 re-execution)
    current_entity_names = {
        (e.get("name") if isinstance(e, dict) else getattr(e, "name", ""))
        for e in current_entities
    }
    current_entity_names = {e for e in current_entity_names if e}
    
    new_entities = current_entity_names - frozen_entity_names
    if new_entities and phase > 5:
        # After Phase 5, new entities should not be added (unless via controlled re-execution)
        warnings.append(
            f"Phase {phase}: New entities detected that are not in frozen schema: {new_entities}. "
            f"This may indicate an issue if not part of a controlled Phase 2 re-execution."
        )
    
    # Check if modifications dict indicates schema changes
    if modifications:
        # Check for entity/attribute modifications
        if "entities" in modifications or "attributes" in modifications:
            warnings.append(
                f"Phase {phase}: Schema modifications detected. "
                f"Frozen schema should remain immutable after Phase 4. "
                f"Modifications: {list(modifications.keys())}"
            )
    
    return {
        "valid": len(errors) == 0,
        "warnings": warnings,
        "errors": errors
    }


def check_frozen_schema_access(
    phase: int,
    state: Dict[str, Any],
) -> bool:
    """
    Check if frozen schema is available for access (read-only).
    
    Args:
        phase: Current phase number
        state: Current pipeline state
        
    Returns:
        bool: True if frozen schema exists and can be accessed
    """
    if phase <= 4:
        # Before Phase 4, frozen schema doesn't exist yet
        return False
    
    metadata = state.get("metadata", {})
    frozen_schema = metadata.get("frozen_schema", {})
    
    if not frozen_schema:
        logger.warning(f"Phase {phase}: Attempting to access frozen schema but it doesn't exist")
        return False
    
    return True
