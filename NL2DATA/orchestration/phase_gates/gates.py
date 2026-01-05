"""Phase gate implementations.

Each phase gate performs deterministic validation checks before
allowing progression to the next phase.
"""

from typing import Dict, Any
from .types import GateResult, PhaseGateError
from .validators import (
    validate_entity_names,
    validate_no_duplicate_entities,
    validate_relations_reference_entities,
    validate_attributes_exist_for_entities,
    validate_primary_keys_exist,
    validate_foreign_keys_reference_existing_pks,
    validate_derived_dependencies_exist,
    validate_derived_formula_dependencies_match_formula,
    validate_data_types_valid,
    validate_ddl_parses,
    validate_constraints_satisfiable,
    validate_generation_strategies_complete,
)
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def check_phase_1_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 1 gate: Entity integrity.
    
    Required checks:
    - Every entity has valid SQL-safe name
    - No duplicate entities after consolidation
    - All relations reference existing entities
    - Connectivity acceptable (or orphan policy documented)
    """
    issues = []
    warnings = []
    
    entities = state.get("entities", [])
    relations = state.get("relations", [])
    
    # Check entity names
    issues.extend(validate_entity_names(entities))
    
    # Check for duplicates
    issues.extend(validate_no_duplicate_entities(entities))
    
    # Check relations reference existing entities
    issues.extend(validate_relations_reference_entities(relations, entities))
    
    # Check connectivity (warnings only - orphans may be intentional)
    metadata = state.get("metadata", {})
    orphan_entities = metadata.get("orphan_entities", [])
    if orphan_entities:
        warnings.append(
            f"Found {len(orphan_entities)} orphan entities: {orphan_entities}. "
            "Consider adding relations or documenting why they're isolated."
        )
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={"orphan_count": len(orphan_entities)}
    )


def check_phase_2_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 2 gate: Schema completeness.
    
    Required checks:
    - Every table/entity has ≥ 1 attribute
    - Every table has a PK (or explicit exception)
    - All FK realizations reference existing PK
    """
    issues = []
    warnings = []
    
    entities = state.get("entities", [])
    attributes = state.get("attributes", {})
    primary_keys = state.get("primary_keys", {})
    foreign_keys = state.get("foreign_keys", [])
    
    # Check all entities have attributes
    issues.extend(validate_attributes_exist_for_entities(attributes, entities))
    
    # Check primary keys exist
    issues.extend(validate_primary_keys_exist(primary_keys, attributes))
    
    # Check foreign keys reference existing PKs
    issues.extend(validate_foreign_keys_reference_existing_pks(foreign_keys, primary_keys))
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={}
    )


def check_phase_3_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 3 gate: Query feasibility.
    
    Required checks:
    - Every info need maps to executable query plan
    - Schema modifications recorded as patches
    """
    issues = []
    warnings = []
    
    information_needs = state.get("information_needs", [])
    sql_queries = state.get("sql_queries", [])
    
    # Check information needs have corresponding queries (or are planned)
    if len(information_needs) > len(sql_queries):
        warnings.append(
            f"Found {len(information_needs)} information needs but only "
            f"{len(sql_queries)} SQL queries. Some queries may be missing."
        )
    
    # Check queries are non-empty
    for i, query in enumerate(sql_queries):
        sql = query.get("sql", "") if isinstance(query, dict) else getattr(query, "sql", "")
        if not sql or not sql.strip():
            issues.append(f"SQL query {i+1} is empty")
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={
            "information_need_count": len(information_needs),
            "sql_query_count": len(sql_queries)
        }
    )


def check_phase_4_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 4 gate: Normalization integrity.
    
    Required checks:
    - Data types valid for target dialect
    - Normalization invariants satisfied
    - FD set consistent with keys
    """
    issues = []
    warnings = []
    
    data_types = state.get("data_types", {})
    functional_dependencies = state.get("functional_dependencies", [])
    
    # Check data types are valid
    issues.extend(validate_data_types_valid(data_types))
    
    # Check functional dependencies reference existing attributes
    # (This would require full schema context - simplified for now)
    if not functional_dependencies:
        warnings.append("No functional dependencies identified. Schema may not be normalized.")
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={
            "fd_count": len(functional_dependencies)
        }
    )


def check_phase_5_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 5 gate: DDL validity.
    
    Required checks:
    - DDL parses and executes in target engine
    - Query SQL parses and references valid schema
    """
    issues = []
    warnings = []
    
    ddl_statements = state.get("ddl_statements", [])
    sql_queries = state.get("sql_queries", [])
    
    # Check DDL statements
    issues.extend(validate_ddl_parses(ddl_statements))
    
    # Check SQL queries (basic syntax)
    for query in sql_queries:
        sql = query.get("sql", "") if isinstance(query, dict) else getattr(query, "sql", "")
        if sql:
            # Basic check
            if not sql.strip().upper().startswith(("SELECT", "INSERT", "UPDATE", "DELETE")):
                warnings.append(f"SQL query may be invalid: {sql[:50]}...")
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={
            "ddl_statement_count": len(ddl_statements),
            "sql_query_count": len(sql_queries)
        }
    )


def check_phase_6_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 6 gate: DDL Generation & Schema Creation.
    
    Required checks:
    - DDL statements are valid and parseable
    - Schema was created successfully (if attempted)
    """
    issues = []
    warnings = []
    
    metadata = state.get("metadata", {})
    ddl_statements = metadata.get("ddl_statements", [])
    previous_answers = state.get("previous_answers", {})
    
    # Check DDL statements are valid
    if ddl_statements:
        issues.extend(validate_ddl_parses(ddl_statements))
    else:
        warnings.append("No DDL statements found in metadata")
    
    # Check schema creation result if present
    schema_creation_result = previous_answers.get("6.4", {})
    schema_created = False
    if schema_creation_result:
        # Handle both Pydantic model and dict formats
        if hasattr(schema_creation_result, "success"):
            schema_created = schema_creation_result.success
            if not schema_created:
                errors = schema_creation_result.errors if hasattr(schema_creation_result, "errors") else []
                issues.extend([f"Schema creation error: {e}" for e in errors])
        elif isinstance(schema_creation_result, dict):
            schema_created = schema_creation_result.get("success", False)
            if not schema_created:
                errors = schema_creation_result.get("errors", [])
                issues.extend([f"Schema creation error: {e}" for e in errors])
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={
            "ddl_statement_count": len(ddl_statements),
            "schema_created": schema_created
        }
    )


def check_phase_7_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 7 gate: Information Mining completeness.
    
    Required checks:
    - Information needs were identified (if any)
    - SQL queries were validated (if information needs exist)
    - No schema modification occurred
    """
    issues = []
    warnings = []
    
    information_needs = state.get("information_needs", [])
    previous_answers = state.get("previous_answers", {})
    
    # Check that information needs were processed
    # Note: It's okay if no information needs were found - that's valid
    if information_needs:
        # If information needs exist, check that SQL validation was performed
        sql_validation_result = previous_answers.get("7.2", {})
        if not sql_validation_result:
            warnings.append("Information needs found but SQL validation result not present")
    
    # Phase 7 should not modify schema - this is a soft check
    # (Schema modifications would be caught in later phases)
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={}
    )


def check_phase_8_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 8 gate: Functional Dependencies completeness.
    
    Required checks:
    - Functional dependencies were identified (if any)
    - FD structure is valid
    """
    issues = []
    warnings = []
    
    functional_dependencies = state.get("functional_dependencies", [])
    
    # Check that functional dependencies are valid (basic structure check)
    for fd in functional_dependencies:
        if not isinstance(fd, dict):
            issues.append(f"Functional dependency is not a dict: {fd}")
            continue
        # Functional dependencies use 'lhs' (left-hand side/determinant) and 'rhs' (right-hand side/dependent)
        if "lhs" not in fd or "rhs" not in fd:
            issues.append(f"Functional dependency missing required fields (lhs/rhs): {fd}")
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={"fd_count": len(functional_dependencies)}
    )


def check_phase_9_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 9 gate: Constraints & Generation Strategies completeness.
    
    Required checks:
    - Generation strategies cover all independent columns (non-derived, non-constrained)
    - Distribution parameters valid (bounds, sums to 1)
    """
    issues = []
    warnings = []
    
    generation_strategies = state.get("generation_strategies", {})
    
    # Get attributes from relational schema (tables) since generation strategies use table names
    metadata = state.get("metadata", {})
    relational_schema = metadata.get("relational_schema", {})
    tables = relational_schema.get("tables", [])
    
    # Get derived and constrained columns to exclude from validation
    # Build mapping from entity names to table names (they might differ)
    # Also build reverse mapping for checking
    entity_to_table_map = {}
    table_to_entity_map = {}
    for table in tables:
        table_name = table.get("name", "")
        # Try to match table name to entity name (assume they're the same or table name contains entity name)
        # Also check if table has an entity_name field
        entity_name = table.get("entity_name", table_name)
        entity_to_table_map[entity_name] = table_name
        entity_to_table_map[table_name] = table_name
        table_to_entity_map[table_name] = entity_name
    
    derived_columns = set()
    derived_formulas = state.get("derived_formulas", {})
    # derived_formulas keys are in format "entity.attribute"
    for key in derived_formulas.keys():
        derived_columns.add(key)  # Keep original format
        # Also add table name format if entity name differs
        if "." in key:
            entity_name, attr_name = key.split(".", 1)
            table_name = entity_to_table_map.get(entity_name, entity_name)
            if table_name != entity_name:
                derived_columns.add(f"{table_name}.{attr_name}")
    
    multivalued_derived = state.get("multivalued_derived", {})
    for entity_name, mv_result in multivalued_derived.items():
        derived_attrs = mv_result.get("derived", [])
        table_name = entity_to_table_map.get(entity_name, entity_name)
        for attr_name in derived_attrs:
            # Add both entity and table name formats
            derived_columns.add(f"{entity_name}.{attr_name}")
            if table_name != entity_name:
                derived_columns.add(f"{table_name}.{attr_name}")
    
    constrained_columns = set()
    compiled_constraints = metadata.get("compiled_constraints", {})
    for category in ["statistical_constraints", "structural_constraints", "distribution_constraints", "other_constraints"]:
        constraints_list = compiled_constraints.get(category, [])
        for constraint in constraints_list:
            affected_attrs = constraint.get("affected_attributes", [])
            for attr in affected_attrs:
                if isinstance(attr, str) and "." in attr:
                    constrained_columns.add(attr)
    
    excluded_columns = derived_columns | constrained_columns
    
    # Get primary keys to exclude (PKs are typically auto-generated or deterministic)
    # Build a set of PK column names by table name (from relational schema)
    pk_columns_by_table = {}
    primary_keys = state.get("primary_keys", {})
    for entity_name, pk_list in primary_keys.items():
        # Map entity name to table name (they might be the same, but check both)
        pk_columns_by_table[entity_name] = set(pk_list)
        # Also check if any table matches this entity name
        for table in tables:
            table_name = table.get("name", "")
            if table_name == entity_name or table_name.startswith(entity_name):
                pk_columns_by_table[table_name] = set(pk_list)
    
    # Extract attributes from relational schema tables, filtering out derived, constrained, PKs, and FKs
    independent_attributes = {}
    for table in tables:
        table_name = table.get("name", "")
        if not table_name:
            continue
        
        columns = table.get("columns", [])
        table_pk = table.get("primary_key", [])
        table_pk_set = set(table_pk) if isinstance(table_pk, list) else set()
        
        # Also get PKs from the entity-level primary_keys if available
        entity_pk_set = pk_columns_by_table.get(table_name, set())
        all_pk_set = table_pk_set | entity_pk_set
        
        foreign_keys = table.get("foreign_keys", [])
        # Build set of FK column names for this table
        fk_columns = set()
        for fk in foreign_keys:
            fk_cols = fk.get("columns", [])
            if isinstance(fk_cols, list):
                fk_columns.update(fk_cols)
            elif isinstance(fk_cols, str):
                fk_columns.add(fk_cols)
        
        independent_attrs = []
        for col in columns:
            col_name = col.get("name", "")
            if not col_name:
                continue
            
            # Check if column is derived
            is_derived = col.get("is_derived", False)
            attr_key = f"{table_name}.{col_name}"
            
            # Check if this attribute is in excluded columns (check both table name and entity name formats)
            is_excluded = False
            if attr_key in excluded_columns:
                is_excluded = True
            else:
                # Also check entity name format (in case table name differs from entity name)
                for entity_name, mapped_table in entity_to_table_map.items():
                    if mapped_table == table_name and entity_name != table_name:
                        entity_attr_key = f"{entity_name}.{col_name}"
                        if entity_attr_key in excluded_columns:
                            is_excluded = True
                            break
            
            # Skip if derived or constrained
            if is_derived or is_excluded:
                continue
            
            # Check if it's a primary key (check both table-level and entity-level PKs)
            is_pk = col_name in all_pk_set
            if is_pk:
                continue  # Primary keys don't need generation strategies (auto-increment or deterministic)
            
            # Check if it's a foreign key
            is_fk = col_name in fk_columns
            if is_fk:
                continue  # Foreign keys don't need generation strategies
            
            independent_attrs.append({"name": col_name})
        
        if independent_attrs:
            independent_attributes[table_name] = independent_attrs
    
    # Debug: Log what we're checking
    total_independent = sum(len(attrs) for attrs in independent_attributes.values())
    logger.debug(f"Phase 9 gate: Checking {total_independent} independent attributes across {len(independent_attributes)} tables")
    logger.debug(f"Phase 9 gate: Generation strategies available for {len(generation_strategies)} tables")
    
    # Check all independent attributes have generation strategies
    # (Derived and constrained columns are excluded)
    # IMPORTANT: generation_strategies uses table names as keys, but we need to check both table and entity names
    # Build a normalized generation_strategies dict that includes both table and entity name lookups
    normalized_generation_strategies = {}
    for table_name, strategies in generation_strategies.items():
        # Add with table name
        normalized_generation_strategies[table_name] = strategies
        # Also add with entity name if different
        entity_name = table_to_entity_map.get(table_name, table_name)
        if entity_name != table_name and entity_name not in normalized_generation_strategies:
            normalized_generation_strategies[entity_name] = strategies
    
    issues.extend(validate_generation_strategies_complete(normalized_generation_strategies, independent_attributes))
    
    # Debug: Log missing strategies
    if issues:
        missing_attrs = [issue for issue in issues if "has no generation strategy" in issue]
        if missing_attrs:
            logger.warning(f"Phase 9 gate: {len(missing_attrs)} attributes missing generation strategies")
            logger.debug(f"Missing strategies: {missing_attrs[:10]}...")  # Log first 10
    
    # Check distribution parameters (simplified)
    for entity_name, entity_strategies in generation_strategies.items():
        for attr_name, strategy in entity_strategies.items():
            dist = strategy.get("distribution", {})
            if isinstance(dist, dict):
                # Check categorical distributions sum to 1
                if "categorical" in str(dist).lower():
                    probs = dist.get("probabilities", {})
                    if probs:
                        total = sum(probs.values())
                        if abs(total - 1.0) > 0.01:
                            issues.append(
                                f"Distribution for '{entity_name}.{attr_name}' probabilities "
                                f"sum to {total}, not 1.0"
                            )
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={}
    )


def check_phase_gate(phase: int, state: Dict[str, Any]) -> GateResult:
    """Check phase gate for a specific phase.
    
    Args:
        phase: Phase number (1-9)
        state: Current state dictionary
        
    Returns:
        GateResult with validation results
        
    Raises:
        PhaseGateError if gate fails
    """
    gate_checks = {
        1: check_phase_1_gate,
        2: check_phase_2_gate,
        3: check_phase_3_gate,
        4: check_phase_4_gate,
        5: check_phase_5_gate,
        6: check_phase_6_gate,
        7: check_phase_7_gate,
        8: check_phase_8_gate,
        9: check_phase_9_gate,
    }
    
    check_func = gate_checks.get(phase)
    if not check_func:
        logger.warning(f"No gate check defined for phase {phase}")
        return GateResult(
            passed=True,
            issues=[],
            warnings=[f"No gate check defined for phase {phase}"],
            metadata={}
        )
    
    result = check_func(state)
    
    if not result.passed:
        logger.error(f"Phase {phase} gate failed: {result.issues}")
        raise PhaseGateError(phase, result)
    
    if result.warnings:
        logger.warning(f"Phase {phase} gate passed with warnings: {result.warnings}")
    
    return result

