"""Validation utilities for NL2DATA."""

from .state_validation import (
    validate_state_consistency,
    validate_parallel_update_results,
    validate_no_duplicate_entities,
    validate_no_duplicate_attributes,
    check_state_consistency,
    validate_no_list_duplication,
    StateValidationError,
)

from .schema_anchored import (
    validate_entity_names,
    validate_attribute_names,
    validate_entity_attribute_consistency,
    validate_phase_transition,
)
from .schema_freeze import (
    validate_frozen_schema_immutability,
    check_frozen_schema_access,
)

__all__ = [
    "validate_state_consistency",
    "validate_parallel_update_results",
    "validate_no_duplicate_entities",
    "validate_no_duplicate_attributes",
    "check_state_consistency",
    "validate_no_list_duplication",
    "StateValidationError",
    "validate_entity_names",
    "validate_attribute_names",
    "validate_entity_attribute_consistency",
    "validate_phase_transition",
    "validate_frozen_schema_immutability",
    "check_frozen_schema_access",
]
