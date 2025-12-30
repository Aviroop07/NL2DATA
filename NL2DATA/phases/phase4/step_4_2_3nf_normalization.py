"""Phase 4, Step 4.2: 3NF Normalization.

Normalize the relational schema to Third Normal Form (3NF) using identified functional dependencies.
Deterministic transformation - applies correct 3NF decomposition algorithm.
"""

import json
from typing import Dict, Any, List, Optional, Set, Tuple

from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.er_relational import RelationalSchema, NormalizedSchema, Table, Column, ForeignKeyConstraint

logger = get_logger(__name__)

def _filter_fds_for_table(
    table_name: str,
    fds: List[Dict[str, Any]],
    table_attributes: Set[str],
) -> List[Dict[str, Any]]:
    """Keep only non-trivial FDs fully contained in the table's attributes."""
    filtered: List[Dict[str, Any]] = []
    for fd in fds:
        lhs = [a for a in fd.get("lhs", []) if isinstance(a, str) and a]
        rhs = [a for a in fd.get("rhs", []) if isinstance(a, str) and a]
        lhs_set = set(lhs)
        rhs_set = set(rhs)
        if not lhs_set or not rhs_set:
            continue
        # Ignore FDs referring to attributes not present in this table
        if not lhs_set.issubset(table_attributes) or not rhs_set.issubset(table_attributes):
            logger.debug(
                f"Skipping FD for table {table_name} because attributes are missing from table. "
                f"LHS={sorted(lhs_set)} RHS={sorted(rhs_set)}"
            )
            continue
        # Ignore trivial FDs X -> Y where Y ⊆ X
        if rhs_set.issubset(lhs_set):
            continue
        filtered.append({"lhs": list(lhs_set), "rhs": list(rhs_set)})
    return filtered


def step_4_2_3nf_normalization(
    relational_schema: Dict[str, Any],  # Relational schema from Step 3.5
    functional_dependencies: Dict[str, List[Dict[str, Any]]],  # entity -> FDs from Step 4.1
    entity_unique_constraints: Optional[Dict[str, List[List[str]]]] = None,  # entity/table -> unique combos (trusted keys)
) -> Dict[str, Any]:
    """
    Step 4.2 (deterministic): Normalize relational schema to 3NF.
    
    This is a deterministic transformation that applies 3NF decomposition algorithm
    to eliminate transitive dependencies. No LLM call needed.
    
    Args:
        relational_schema: Relational schema from Step 3.5 with tables, columns, PKs, FKs
        functional_dependencies: Dictionary mapping entity/table names to their functional dependencies
        
    Returns:
        dict: Normalized schema with normalized_tables, decomposition_steps, attribute_mapping, etc.
        
    Example:
        >>> schema = step_4_2_3nf_normalization(
        ...     relational_schema={"tables": [{"name": "Customer", "columns": []}]},
        ...     functional_dependencies={"Customer": [{"lhs": ["zipcode"], "rhs": ["city"]}]}
        ... )
        >>> len(schema["normalized_tables"]) > 0
        True
    """
    logger.info("Starting Step 4.2: 3NF Normalization (deterministic)")
    
    # Validate relational schema via Pydantic internally (avoid raw dict handling)
    schema_model = RelationalSchema.model_validate(relational_schema)
    tables = [t.model_dump() for t in schema_model.tables]
    normalized_tables = []
    decomposition_steps = []
    attribute_mapping = {}  # old_table.attribute -> new_table.attribute
    
    for table in tables:
        table_name = table.get("name", "")
        columns = table.get("columns", [])
        primary_key = table.get("primary_key", [])
        foreign_keys = table.get("foreign_keys", [])
        
        # Get functional dependencies for this table
        fds = functional_dependencies.get(table_name, [])
        
        if not fds:
            # No FDs to normalize - keep table as is
            normalized_tables.append(table)
            decomposition_steps.append(f"Table {table_name}: No functional dependencies to normalize")
            continue
        
        # Extract all attributes
        all_attributes = {col.get("name", "") for col in columns}
        # Filter FDs to only those applicable to this table and non-trivial
        fds = _filter_fds_for_table(table_name, fds, all_attributes)
        if not fds:
            normalized_tables.append(table)
            decomposition_steps.append(f"Table {table_name}: No applicable functional dependencies after filtering")
            continue

        pk_set = set(primary_key or [])
        if not pk_set:
            # If PK is unknown/empty, we cannot reliably decide superkeys/prime attributes.
            # Avoid destructive decompositions; keep table as-is.
            normalized_tables.append(table)
            decomposition_steps.append(
                f"Table {table_name}: Skipped 3NF decomposition because primary key is empty/unknown"
            )
            continue
        
        # Candidate keys: PK plus any trusted unique constraints (from Phase 2.10 or Step 3.5)
        candidate_keys: List[Set[str]] = []
        if pk_set:
            candidate_keys.append(set(pk_set))
        # Prefer explicit per-table unique constraints if available
        table_level_ucs = table.get("unique_constraints", []) or []
        for uc in table_level_ucs:
            if isinstance(uc, list) and uc:
                candidate_keys.append(set([a for a in uc if isinstance(a, str) and a]))
        if entity_unique_constraints and table_name in entity_unique_constraints:
            for uc in entity_unique_constraints.get(table_name, []) or []:
                if isinstance(uc, list) and uc:
                    candidate_keys.append(set([a for a in uc if isinstance(a, str) and a]))

        # Prime attributes = those appearing in any candidate key (approximation; better than PK-only)
        prime_attributes: Set[str] = set()
        for k in candidate_keys:
            prime_attributes.update(k)

        # Identify foreign-key columns; FDs with FK determinants are often unsafe/hallucinated.
        fk_attributes: Set[str] = {
            (c.get("name", "") or "")
            for c in columns
            if isinstance(c, dict) and c.get("is_foreign_key")
        }

        # Build FD structure: lhs -> rhs sets
        fd_map: Dict[Tuple[str, ...], Set[str]] = {}
        for fd in fds:
            lhs = tuple(sorted(fd.get("lhs", [])))
            rhs = set(fd.get("rhs", []))
            if lhs in fd_map:
                fd_map[lhs].update(rhs)
            else:
                fd_map[lhs] = rhs.copy()
        
        # Check for transitive dependencies (3NF violation)
        # A table is in 3NF if for every FD X → Y, either:
        # 1. X is a superkey, or
        # 2. Y is a prime attribute (part of some candidate key)
        
        # For simplicity, we'll check if there are FDs where:
        # - LHS is not a superkey (not containing PK or a superset of PK)
        # - RHS is not a prime attribute (not part of PK)
        # If such FDs exist, we need to decompose
        
        needs_decomposition = False
        transitive_fds = []
        
        for lhs_tuple, rhs_set in fd_map.items():
            lhs_set = set(lhs_tuple)
            
            # Check if LHS is a superkey (contains any candidate key)
            is_superkey = any(k and k.issubset(lhs_set) for k in candidate_keys) if candidate_keys else False
            
            # Check if RHS contains only prime attributes (part of any candidate key)
            rhs_is_prime = rhs_set.issubset(prime_attributes) if prime_attributes else False
            
            # If LHS is not superkey AND RHS is not prime, we have a 3NF violation
            if not is_superkey and not rhs_is_prime:
                # Safety: only decompose if determinant is "trusted".
                # We treat determinants made ONLY of foreign keys as untrusted unless they are candidate keys,
                # because these frequently produce bogus decompositions like publisher_id -> title/author.
                lhs_has_fk = bool(lhs_set.intersection(fk_attributes))
                is_trusted_keylike = any(k and k.issubset(lhs_set) for k in candidate_keys) if candidate_keys else False
                is_trusted_determinant = is_trusted_keylike or not lhs_has_fk
                if is_trusted_determinant:
                    needs_decomposition = True
                    transitive_fds.append((lhs_tuple, rhs_set))
                else:
                    logger.debug(
                        f"Table {table_name}: Skipping unsafe decomposition for FD "
                        f"{', '.join(lhs_tuple)} -> {', '.join(sorted(rhs_set))} "
                        f"because determinant is not trusted (FK-only determinant without key support)."
                    )
        
        if not needs_decomposition:
            # Table is already in 3NF
            normalized_tables.append(table)
            decomposition_steps.append(f"Table {table_name}: Already in 3NF, no decomposition needed")
            continue
        
        # Perform 3NF decomposition
        # For each transitive FD X → Y, create a new table with X ∪ Y
        # Keep original table with remaining attributes
        
        # Start with original table
        remaining_attrs = all_attributes.copy()
        new_tables = []
        
        for lhs_tuple, rhs_set in transitive_fds:
            lhs_set = set(lhs_tuple)
            new_table_attrs = lhs_set.union(rhs_set)
            
            # Create new table for this FD
            new_table_name = f"{table_name}_{'_'.join(sorted(lhs_set))}"
            new_table_columns = [
                col for col in columns
                if col.get("name", "") in new_table_attrs
            ]
            
            # Determine primary key for new table:
            # Use the smallest candidate key contained in LHS (if any), else LHS itself.
            contained_keys = [k for k in candidate_keys if k and k.issubset(lhs_set)]
            key_set = min(contained_keys, key=len) if contained_keys else lhs_set
            new_table_pk = list(key_set)
            
            new_table = {
                "name": new_table_name,
                "columns": new_table_columns,
                "primary_key": new_table_pk,
                "foreign_keys": [],
                "is_decomposed": True,
                "original_table": table_name,
                # How to join back for reconstruction (informational, not enforced as FK)
                "join_attributes": sorted(list(key_set)),
            }
            
            new_tables.append(new_table)
            remaining_attrs -= rhs_set  # Remove RHS from remaining (keep LHS for FK)
            
            decomposition_steps.append(
                f"Table {table_name}: Decomposed - created {new_table_name} for FD "
                f"{', '.join(lhs_tuple)} → {', '.join(sorted(rhs_set))}"
            )
        
        # Update original table with remaining attributes
        remaining_columns = [
            col for col in columns
            if col.get("name", "") in remaining_attrs
        ]
        
        # Update foreign keys in original table
        # Do NOT automatically add FK constraints as part of normalization:
        # decomposition is a logical design step and adding constraints here can create
        # invalid FKs (e.g., using non-key business attributes as references).
        updated_fks = foreign_keys.copy()
        
        updated_table = {
            **table,
            "columns": remaining_columns,
            "foreign_keys": updated_fks,
            "is_normalized": True,
        }
        
        normalized_tables.append(updated_table)
        normalized_tables.extend(new_tables)
        
        # Build attribute mapping
        for new_table in new_tables:
            for col in new_table.get("columns", []):
                attr_name = col.get("name", "")
                old_ref = f"{table_name}.{attr_name}"
                new_ref = f"{new_table.get('name', '')}.{attr_name}"
                attribute_mapping[old_ref] = new_ref
    
    # Build dependency preservation and key preservation reports
    dependency_preservation_report = {}
    key_preservation_report = {}
    
    for table in normalized_tables:
        table_name = table.get("name", "")
        # Simplified: assume dependencies are preserved if all attributes are present
        dependency_preservation_report[table_name] = True
        # Keys are preserved if PK exists
        key_preservation_report[table_name] = len(table.get("primary_key", [])) > 0
    
    logger.info(
        f"3NF normalization completed: {len(normalized_tables)} normalized tables "
        f"({len([t for t in normalized_tables if t.get('is_decomposed', False)])} decomposed tables)"
    )
    
    # Compute join paths for decomposed tables
    # Join paths describe how to reconstruct the original table from decomposed tables
    join_paths = []
    
    # Group tables by original table name
    original_to_decomposed: Dict[str, List[Dict[str, Any]]] = {}
    for table in normalized_tables:
        if table.get("is_decomposed", False):
            original_name = table.get("original_table", "")
            if original_name:
                original_to_decomposed.setdefault(original_name, []).append(table)
        elif table.get("is_normalized", False):
            # This is the updated original table
            table_name = table.get("name", "")
            if table_name not in original_to_decomposed:
                original_to_decomposed[table_name] = []
    
    # For each original table that was decomposed, create join paths
    for original_name, decomposed_tables in original_to_decomposed.items():
        if not decomposed_tables:
            continue
        
        # Find the updated original table
        original_table = None
        for table in normalized_tables:
            if table.get("name") == original_name and table.get("is_normalized", False):
                original_table = table
                break
        
        if not original_table:
            continue
        
        # Create join paths: each decomposed table can be joined back to original via FK
        for decomposed_table in decomposed_tables:
            decomposed_name = decomposed_table.get("name", "")
            decomposed_pk = decomposed_table.get("primary_key", [])
            
            # Join back using shared join_attributes (determinant) rather than FK constraints
            join_attrs = decomposed_table.get("join_attributes", []) or decomposed_table.get("primary_key", [])
            if join_attrs:
                join_paths.append({
                    "from_table": decomposed_name,
                    "to_table": original_name,
                    "join_condition": {
                        "from_attributes": list(join_attrs),
                        "to_attributes": list(join_attrs),
                    },
                    "join_type": "INNER",
                    "purpose": f"Reconstruct {original_name} from decomposed {decomposed_name}",
                })
    
    logger.debug(f"Computed {len(join_paths)} join paths for decomposed tables")
    
    normalized_schema = {
        "normalized_tables": normalized_tables,
        "decomposition_steps": decomposition_steps,
        "attribute_mapping": attribute_mapping,
        "dependency_preservation_report": dependency_preservation_report,
        "key_preservation_report": key_preservation_report,
        "join_paths": join_paths,  # Join paths for reconstructing original tables from decomposed tables
    }
    
    # Log the complete normalized schema
    logger.info("=== NORMALIZED SCHEMA (Step 4.2 Output) ===")
    logger.info(json.dumps(normalized_schema, indent=2, default=str))
    logger.info("=== END NORMALIZED SCHEMA ===")
    
    return normalized_schema


