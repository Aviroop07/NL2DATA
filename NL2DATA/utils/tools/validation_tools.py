"""Validation tools for LLM self-validation.

These tools allow LLMs to validate their responses before finalizing them,
check against the current schema state, and ensure consistency.

NOTE: This module now imports from the refactored validation/ submodules
for better organization. All functions are re-exported here for backward compatibility.
"""

# Import all validation functions from modular submodules
from .validation.existence import (
    _check_entity_exists_impl,
    _verify_entities_exist_impl,
    _validate_attributes_exist_impl,
    _check_schema_component_exists_impl,
    check_entity_exists,
    verify_entities_exist,
    validate_attributes_exist,
    check_schema_component_exists,
)

from .validation.syntax import (
    _validate_dsl_expression_impl,
    validate_sql_syntax,
    validate_sql_type,
    validate_formula_syntax,
    validate_dsl_expression,
)

from .validation.semantic import (
    validate_cardinality_range,
    validate_entity_cardinality,
    validate_range,
    validate_distribution_sum,
    validate_distribution_type,
)

from .validation.feasibility import (
    check_foreign_key_feasibility,
    check_generator_exists,
    check_partition_feasibility,
)

from .validation.naming import (
    _check_entity_name_validity_impl,
    check_entity_name_validity,
    check_attribute_name_validity,
    check_naming_convention,
    check_name_reserved,
)

from .validation.evidence import (
    _verify_evidence_substring_impl,
    _verify_entity_in_known_entities_impl,
    _validate_subset_impl,
    _validate_merge_decision_impl,
    _validate_final_entities_impl,
    verify_evidence_substring,
    verify_entity_in_known_entities,
    validate_subset,
    validate_merge_decision,
    validate_final_entities,
    validate_relation_cardinality_output,
)

from .validation.connectivity import (
    _check_entity_connectivity_impl,
    _detect_circular_dependencies_impl,
    _validate_cardinality_consistency_impl,
    check_entity_name_similarity,
    check_entity_connectivity,
    detect_circular_dependencies,
    validate_cardinality_consistency,
)

from .validation.query import (
    _validate_query_against_schema_impl,
    validate_query_against_schema,
)

from .validation.constraints import (
    verify_schema_components,
    check_constraint_satisfiability,
)

# Re-export everything for backward compatibility
__all__ = [
    # Existence validation (impl functions for internal use)
    "_check_entity_exists_impl",
    "_verify_entities_exist_impl",
    "_validate_attributes_exist_impl",
    "_check_schema_component_exists_impl",
    # Existence validation (tool functions)
    "check_entity_exists",
    "verify_entities_exist",
    "validate_attributes_exist",
    "check_schema_component_exists",
    # Syntax validation (impl functions)
    "_validate_dsl_expression_impl",
    # Syntax validation (tool functions)
    "validate_sql_syntax",
    "validate_sql_type",
    "validate_formula_syntax",
    "validate_dsl_expression",
    # Semantic validation
    "validate_cardinality_range",
    "validate_entity_cardinality",
    "validate_range",
    "validate_distribution_sum",
    "validate_distribution_type",
    # Feasibility validation
    "check_foreign_key_feasibility",
    "check_generator_exists",
    "check_partition_feasibility",
    # Naming validation
    "_check_entity_name_validity_impl",
    "check_entity_name_validity",
    "check_attribute_name_validity",
    "check_naming_convention",
    "check_name_reserved",
    # Evidence / constraint validation
    "_verify_evidence_substring_impl",
    "_verify_entity_in_known_entities_impl",
    "_validate_subset_impl",
    "_validate_merge_decision_impl",
    "_validate_final_entities_impl",
    "verify_evidence_substring",
    "verify_entity_in_known_entities",
    "validate_subset",
    "validate_merge_decision",
    "validate_final_entities",
    "validate_relation_cardinality_output",
    # Connectivity validation (impl functions)
    "_check_entity_connectivity_impl",
    "_detect_circular_dependencies_impl",
    "_validate_cardinality_consistency_impl",
    # Connectivity validation (tool functions)
    "check_entity_name_similarity",
    "check_entity_connectivity",
    "detect_circular_dependencies",
    "validate_cardinality_consistency",
    # Query validation (impl functions)
    "_validate_query_against_schema_impl",
    # Query validation (tool functions)
    "validate_query_against_schema",
    # Constraint validation
    "verify_schema_components",
    "check_constraint_satisfiability",
]
