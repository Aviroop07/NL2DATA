"""Validation functions for phase gates.

Each validator checks a specific aspect of schema integrity.
"""

from typing import List, Dict, Any, Set
import re

from NL2DATA.utils.dsl.analysis import dsl_identifiers_used
from NL2DATA.utils.dsl.validator import validate_dsl_expression


def validate_entity_names(entities: List[Dict[str, Any]]) -> List[str]:
    """Validate entity names are SQL-safe.
    
    Returns:
        List of issues (empty if all valid)
    """
    issues = []
    reserved_keywords = {
        "select", "from", "where", "insert", "update", "delete", "create",
        "drop", "alter", "table", "index", "view", "database", "schema"
    }
    
    for entity in entities:
        name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
        if not name:
            issues.append(f"Entity has empty name: {entity}")
            continue
        
        # Check SQL identifier rules
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            issues.append(f"Entity name '{name}' contains invalid characters")
        
        # Check reserved keywords
        if name.lower() in reserved_keywords:
            issues.append(f"Entity name '{name}' is a reserved SQL keyword")
    
    return issues


def validate_no_duplicate_entities(entities: List[Dict[str, Any]]) -> List[str]:
    """Check for duplicate entity names."""
    issues = []
    seen_names: Set[str] = set()
    
    for entity in entities:
        name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
        if not name:
            continue
        
        name_lower = name.lower()
        if name_lower in seen_names:
            issues.append(f"Duplicate entity name: '{name}'")
        seen_names.add(name_lower)
    
    return issues


def validate_relations_reference_entities(
    relations: List[Dict[str, Any]],
    entities: List[Dict[str, Any]]
) -> List[str]:
    """Verify all relations reference existing entities."""
    issues = []
    
    # Get entity names
    entity_names = set()
    for entity in entities:
        name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
        if name:
            entity_names.add(name.lower())
    
    # Check relations
    for relation in relations:
        rel_entities = relation.get("entities", []) if isinstance(relation, dict) else getattr(relation, "entities", [])
        for entity_name in rel_entities:
            if entity_name.lower() not in entity_names:
                issues.append(f"Relation references non-existent entity: '{entity_name}'")
    
    return issues


def validate_attributes_exist_for_entities(
    attributes: Dict[str, List[Dict[str, Any]]],
    entities: List[Dict[str, Any]]
) -> List[str]:
    """Verify all entities have at least one attribute."""
    issues = []
    
    entity_names = set()
    for entity in entities:
        name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
        if name:
            entity_names.add(name)
    
    for entity_name in entity_names:
        if entity_name not in attributes or not attributes[entity_name]:
            issues.append(f"Entity '{entity_name}' has no attributes")
    
    return issues


def validate_primary_keys_exist(
    primary_keys: Dict[str, List[str]],
    attributes: Dict[str, List[Dict[str, Any]]]
) -> List[str]:
    """Verify all entities have primary keys and PK attributes exist."""
    issues = []
    
    for entity_name, pk_attrs in primary_keys.items():
        if not pk_attrs:
            issues.append(f"Entity '{entity_name}' has no primary key")
            continue
        
        # Check PK attributes exist
        entity_attrs = attributes.get(entity_name, [])
        attr_names = set()
        for attr in entity_attrs:
            name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
            if name:
                attr_names.add(name.lower())
        
        for pk_attr in pk_attrs:
            if pk_attr.lower() not in attr_names:
                issues.append(f"Primary key attribute '{pk_attr}' does not exist for entity '{entity_name}'")
    
    return issues


def validate_foreign_keys_reference_existing_pks(
    foreign_keys: List[Dict[str, Any]],
    primary_keys: Dict[str, List[str]]
) -> List[str]:
    """Verify foreign keys reference existing primary keys."""
    issues = []
    
    for fk in foreign_keys:
        from_entity = fk.get("from_entity", "") if isinstance(fk, dict) else getattr(fk, "from_entity", "")
        to_entity = fk.get("to_entity", "") if isinstance(fk, dict) else getattr(fk, "to_entity", "")
        to_attrs = fk.get("to_attributes", []) if isinstance(fk, dict) else getattr(fk, "to_attributes", [])
        
        if to_entity not in primary_keys:
            issues.append(f"Foreign key references entity '{to_entity}' with no primary key")
            continue
        
        # Check FK attributes match PK
        pk_attrs = primary_keys[to_entity]
        if sorted(to_attrs) != sorted(pk_attrs):
            issues.append(
                f"Foreign key from '{from_entity}' to '{to_entity}' references attributes {to_attrs}, "
                f"but primary key is {pk_attrs}"
            )
    
    return issues


def validate_derived_dependencies_exist(
    attributes: Dict[str, List[Dict[str, Any]]],
    derived_formulas: Dict[str, Dict[str, Any]]
) -> List[str]:
    """Verify derived attribute dependencies exist."""
    issues = []
    
    for entity_name, attrs in attributes.items():
        for attr in attrs:
            attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
            if not attr_name:
                continue
            
            # Check if this is a derived attribute
            key = f"{entity_name}.{attr_name}"
            formula_info = derived_formulas.get(key, {})
            if not formula_info:
                continue
            
            # Get dependencies
            dependencies = formula_info.get("dependencies", [])
            attr_names = {a.get("name", "").lower() if isinstance(a, dict) else getattr(a, "name", "").lower() 
                         for a in attrs}
            
            for dep in dependencies:
                if dep.lower() not in attr_names:
                    issues.append(
                        f"Derived attribute '{entity_name}.{attr_name}' depends on "
                        f"non-existent attribute '{dep}'"
                    )
    
    return issues


def validate_derived_formula_dependencies_match_formula(
    attributes: Dict[str, List[Dict[str, Any]]],
    derived_formulas: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Verify formula identifiers are entity-local and match the declared dependencies list.

    Requirements:
    - If a column name is mentioned nakedly, it must belong to the SAME entity.
    - We deterministically extract identifiers used in the DSL formula and compare against
      `dependencies` returned by the LLM.
    """
    issues: List[str] = []

    # Build entity -> set(attr_names_lower)
    entity_attr_names: Dict[str, Set[str]] = {}
    for entity_name, attrs in (attributes or {}).items():
        s: Set[str] = set()
        if isinstance(attrs, list):
            for a in attrs:
                n = a.get("name", "") if isinstance(a, dict) else getattr(a, "name", "")
                if n:
                    s.add(str(n).lower())
        entity_attr_names[entity_name] = s

    for key, info in (derived_formulas or {}).items():
        if not isinstance(key, str) or "." not in key:
            continue
        if not isinstance(info, dict):
            continue

        entity_name, attr_name = key.split(".", 1)
        allowed = entity_attr_names.get(entity_name, set())

        formula = (info.get("formula") or "").strip()
        if not formula:
            issues.append(f"Derived attribute '{key}' has empty formula")
            continue

        v = validate_dsl_expression(formula)
        if not v.get("valid", False):
            issues.append(f"Derived attribute '{key}' has invalid DSL formula: {v.get('error')}")
            continue

        used = dsl_identifiers_used(formula)
        dotted = sorted([u for u in used if isinstance(u, str) and "." in u])
        if dotted:
            issues.append(f"Derived attribute '{key}' formula uses dotted identifiers (not allowed): {dotted}")

        used_bare = {u.lower() for u in used if isinstance(u, str) and u and "." not in u}

        # Naked identifiers must be entity-local.
        outside = sorted([u for u in used_bare if u not in allowed])
        if outside:
            issues.append(
                f"Derived attribute '{key}' formula references identifiers not in entity '{entity_name}': {outside}"
            )

        deps = info.get("dependencies", []) or []
        if not isinstance(deps, list):
            deps = []
        deps_norm = {str(d).lower() for d in deps if isinstance(d, str) and d}

        missing = sorted(list(used_bare - deps_norm))
        extra = sorted(list(deps_norm - used_bare))
        if missing:
            issues.append(f"Derived attribute '{key}' dependencies missing identifiers used in formula: {missing}")
        if extra:
            issues.append(f"Derived attribute '{key}' dependencies contain unused identifiers: {extra}")

    return issues


def validate_data_types_valid(
    data_types: Dict[str, Dict[str, Dict[str, Any]]]
) -> List[str]:
    """Validate data types are valid SQL types."""
    issues = []
    valid_types = {
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
        "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL",
        "VARCHAR", "CHAR", "TEXT", "CLOB",
        "DATE", "TIME", "TIMESTAMP", "DATETIME",
        "BOOLEAN", "BOOL",
        "BLOB", "BINARY", "VARBINARY"
    }
    
    for entity_name, entity_types in data_types.items():
        for attr_name, type_info in entity_types.items():
            type_str = type_info.get("type", "").upper()
            if type_str and type_str not in valid_types:
                # Check if it's a parameterized type (e.g., VARCHAR(255))
                base_type = type_str.split("(")[0]
                if base_type not in valid_types:
                    issues.append(
                        f"Invalid data type '{type_str}' for '{entity_name}.{attr_name}'"
                    )
    
    return issues


def validate_ddl_parses(ddl_statements: List[str]) -> List[str]:
    """Basic validation that DDL statements are parseable.
    
    Note: Full parsing would require a SQL parser library.
    """
    issues = []
    
    for ddl in ddl_statements:
        if not ddl.strip():
            issues.append("Empty DDL statement")
            continue
        
        # Basic checks
        if not ddl.upper().startswith("CREATE"):
            issues.append(f"DDL statement does not start with CREATE: {ddl[:50]}...")
        
        # Check balanced parentheses
        if ddl.count("(") != ddl.count(")"):
            issues.append(f"Unbalanced parentheses in DDL: {ddl[:50]}...")
    
    return issues


def validate_constraints_satisfiable(
    constraints: List[Dict[str, Any]]
) -> List[str]:
    """Check if constraints are satisfiable.
    
    Note: This is a simplified check. Full implementation would use
    constraint satisfaction logic.
    """
    issues = []
    
    # Group constraints by attribute
    attr_constraints: Dict[str, List[Dict[str, Any]]] = {}
    
    for constraint in constraints:
        affected = constraint.get("affected_attributes", [])
        for attr in affected:
            if attr not in attr_constraints:
                attr_constraints[attr] = []
            attr_constraints[attr].append(constraint)
    
    # Check for obvious conflicts (simplified)
    for attr, attr_consts in attr_constraints.items():
        if len(attr_consts) > 1:
            # Check for contradictory ranges (simplified check)
            dsl_exprs = [
                c.get("dsl_expression", "") or c.get("condition", "")
                for c in attr_consts
            ]
            # This is a placeholder - full implementation needed
            # For now, just check if we have multiple constraints on same attribute
    
    return issues


def validate_generation_strategies_complete(
    generation_strategies: Dict[str, Dict[str, Dict[str, Any]]],
    attributes: Dict[str, List[Dict[str, Any]]]
) -> List[str]:
    """Verify all attributes have generation strategies."""
    issues = []
    
    for entity_name, attrs in attributes.items():
        # Try to get strategies by entity name first, then try variations
        entity_strategies = generation_strategies.get(entity_name, {})
        
        # If not found, try to find by checking all keys (in case of name mismatches)
        if not entity_strategies:
            # Check if any key in generation_strategies matches (case-insensitive or partial match)
            for key in generation_strategies.keys():
                if key.lower() == entity_name.lower() or entity_name.lower() in key.lower() or key.lower() in entity_name.lower():
                    entity_strategies = generation_strategies.get(key, {})
                    if entity_strategies:
                        break
        
        for attr in attrs:
            attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
            if not attr_name:
                continue
            
            if attr_name not in entity_strategies:
                issues.append(
                    f"Attribute '{entity_name}.{attr_name}' has no generation strategy"
                )
    
    return issues

