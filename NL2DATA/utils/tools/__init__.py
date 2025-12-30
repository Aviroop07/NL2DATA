"""Validation tools for LLM self-validation.

This module provides LangChain tools that LLMs can use to validate
their responses before finalizing them.
"""

from .validation_tools import (
    check_entity_exists,
    verify_entities_exist,
    validate_attributes_exist,
    check_schema_component_exists,
    validate_sql_syntax,
    validate_sql_type,
    validate_formula_syntax,
    validate_dsl_expression,
    validate_cardinality_range,
    validate_entity_cardinality,
    validate_range,
    validate_distribution_sum,
    validate_distribution_type,
    check_foreign_key_feasibility,
    validate_query_against_schema,
    check_generator_exists,
    check_partition_feasibility,
    check_entity_name_validity,
    check_attribute_name_validity,
    check_naming_convention,
    check_name_reserved,
    check_entity_name_similarity,
    check_entity_connectivity,
    detect_circular_dependencies,
    validate_cardinality_consistency,
    verify_schema_components,
    check_constraint_satisfiability,
    verify_evidence_substring,
    verify_entity_in_known_entities,
    validate_subset,
    validate_merge_decision,
    validate_final_entities,
    validate_relation_cardinality_output,
)

__all__ = [
    # Existence validation
    "check_entity_exists",
    "verify_entities_exist",
    "validate_attributes_exist",
    "check_schema_component_exists",
    # Syntax validation
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
    "validate_query_against_schema",
    "check_generator_exists",
    "check_partition_feasibility",
    # Naming validation
    "check_entity_name_validity",
    "check_attribute_name_validity",
    "check_naming_convention",
    "check_name_reserved",
    # Graph analysis
    "check_entity_name_similarity",
    "check_entity_connectivity",
    "detect_circular_dependencies",
    "validate_cardinality_consistency",
    # Schema component verification
    "verify_schema_components",
    # Constraint satisfiability
    "check_constraint_satisfiability",
    # Evidence / constraint validation
    "verify_evidence_substring",
    "verify_entity_in_known_entities",
    "validate_subset",
    "validate_merge_decision",
    "validate_final_entities",
    "validate_relation_cardinality_output",
]

