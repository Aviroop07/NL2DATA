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
    - Derived attributes: dependencies exist
    """
    issues = []
    warnings = []
    
    entities = state.get("entities", [])
    attributes = state.get("attributes", {})
    primary_keys = state.get("primary_keys", {})
    foreign_keys = state.get("foreign_keys", [])
    derived_formulas = state.get("derived_formulas", {})
    
    # Check all entities have attributes
    issues.extend(validate_attributes_exist_for_entities(attributes, entities))
    
    # Check primary keys exist
    issues.extend(validate_primary_keys_exist(primary_keys, attributes))
    
    # Check foreign keys reference existing PKs
    issues.extend(validate_foreign_keys_reference_existing_pks(foreign_keys, primary_keys))
    
    # Check derived attribute dependencies
    issues.extend(validate_derived_dependencies_exist(attributes, derived_formulas))
    
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
    """Check Phase 6 gate: Constraint satisfiability.
    
    Required checks:
    - Constraints are satisfiable (or marked "soft")
    - No hard conflicts between constraints
    """
    issues = []
    warnings = []
    
    constraints = state.get("constraints", [])
    constraint_specs = state.get("constraint_specs", [])
    
    # Check constraints are satisfiable
    all_constraints = constraints + constraint_specs
    issues.extend(validate_constraints_satisfiable(all_constraints))
    
    # Check for hard conflicts
    hard_constraints = [
        c for c in all_constraints
        if c.get("type", "").lower() == "hard" or c.get("hard_vs_soft", "").lower() == "hard"
    ]
    
    if len(hard_constraints) > 0:
        # Additional conflict checking for hard constraints
        pass  # Placeholder for full conflict detection
    
    passed = len(issues) == 0
    
    return GateResult(
        passed=passed,
        issues=issues,
        warnings=warnings,
        metadata={
            "constraint_count": len(all_constraints),
            "hard_constraint_count": len(hard_constraints)
        }
    )


def check_phase_7_gate(state: Dict[str, Any]) -> GateResult:
    """Check Phase 7 gate: Generation completeness.
    
    Required checks:
    - Generation strategies cover all columns
    - Distribution parameters valid (bounds, sums to 1)
    - GenerationIR compiled successfully
    """
    issues = []
    warnings = []
    
    generation_strategies = state.get("generation_strategies", {})
    attributes = state.get("attributes", {})
    
    # Check all attributes have generation strategies
    issues.extend(validate_generation_strategies_complete(generation_strategies, attributes))
    
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
        phase: Phase number (1-7)
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

