"""Phase 3, Step 3.5: Relational Schema Compilation.

Convert ER model to normalized relational tables.
Deterministic transformation - applies ER-to-relational mapping algorithm.
Outputs initial LogicalIR schema structure (before 3NF normalization).

Algorithm: ER → Relational Schema Conversion
- Handles entities, attributes (simple, composite, multivalued, derived)
- Handles binary and n-ary relationships
- Applies participation constraints (total → NOT NULL, partial → nullable)
"""

import json
from typing import Dict, Any, List, Optional, Set, Tuple

from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
    extract_attribute_type_hint,
    extract_attribute_field,
)
from NL2DATA.ir.models.er_relational import ERDesign, RelationalSchema, Table, Column, ForeignKeyConstraint

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False

def _to_pydantic_table(table: Dict[str, Any]) -> Table:
    """Convert a legacy dict table into Pydantic Table (best-effort)."""
    cols: List[Column] = []
    for c in table.get("columns", []) or []:
        if not isinstance(c, dict) or not c.get("name"):
            continue
        cols.append(Column(**c))
    fks: List[ForeignKeyConstraint] = []
    for fk in table.get("foreign_keys", []) or []:
        if isinstance(fk, dict) and fk.get("attributes") and fk.get("references_table") and fk.get("referenced_attributes"):
            fks.append(ForeignKeyConstraint(**fk))
    return Table(
        name=table.get("name", ""),
        columns=cols,
        primary_key=table.get("primary_key", []) or [],
        foreign_keys=fks,
        unique_constraints=table.get("unique_constraints", []) or [],
        is_junction_table=bool(table.get("is_junction_table", False)),
        is_multivalued_table=bool(table.get("is_multivalued_table", False)),
        is_normalized=bool(table.get("is_normalized", False)),
        is_decomposed=bool(table.get("is_decomposed", False)),
        original_table=table.get("original_table"),
        join_attributes=table.get("join_attributes", []) or [],
    )


def _from_pydantic_schema(schema: RelationalSchema) -> Dict[str, Any]:
    """Convert Pydantic RelationalSchema to legacy dict structure."""
    return {"tables": [t.model_dump() for t in schema.tables]}


def _is_associative_entity_candidate(
    entity_name: str,
    entity_pk: List[str],
    entity_table: Optional[Dict[str, Any]],
    *,
    other_entities: Tuple[str, str],
) -> bool:
    """
    Heuristic: decide if an entity in a ternary relation is an associative entity.
    We prefer a conservative rule:
    - has no declared PK (from Phase 2) OR
    - name indicates line/item/detail AND it has some non-key attributes
    """
    name_l = (entity_name or "").lower()
    looks_like_line = any(tok in name_l for tok in ["item", "line", "detail", "assoc", "link"])
    has_pk = bool(entity_pk)
    has_some_attrs = bool(entity_table and (entity_table.get("columns") or []))
    # If no PK, it is very likely association-like in the pipeline outputs.
    if not has_pk and has_some_attrs:
        return True
    return looks_like_line and has_some_attrs


def _add_unique_constraint(table: Dict[str, Any], cols: List[str]) -> None:
    """Record a uniqueness constraint at table level (deterministic)."""
    cols = [c for c in cols if c]
    if not cols:
        return
    ucs = table.setdefault("unique_constraints", [])
    if not any(isinstance(x, list) and x == cols for x in ucs):
        ucs.append(cols)

def _ensure_column(
    table: Dict[str, Any],
    column_name: str,
    *,
    description: str = "",
    type_hint: Optional[str] = None,
) -> Dict[str, Any]:
    """Ensure a column exists on table; return the column dict."""
    cols = table.setdefault("columns", [])
    existing = next((c for c in cols if c.get("name") == column_name), None)
    if existing:
        # Fill in missing metadata if needed
        if description and not existing.get("description"):
            existing["description"] = description
        if type_hint and not existing.get("type_hint"):
            existing["type_hint"] = type_hint
        return existing

    col: Dict[str, Any] = {"name": column_name}
    if description:
        col["description"] = description
    if type_hint:
        col["type_hint"] = type_hint
    cols.append(col)
    return col


def _infer_primary_key_for_table(table: Dict[str, Any]) -> List[str]:
    """
    Infer a primary key for tables that ended up without one.
    Priority:
    - Junction tables: all FK columns (composite PK)
    - Association-like tables (2+ FKs, no obvious id): all FK columns (composite PK)
    - Otherwise: add surrogate '<table>_id'
    """
    table_name = table.get("name", "table")
    cols = table.get("columns", [])
    fk_cols = [c.get("name") for c in cols if c.get("is_foreign_key") and c.get("name")]
    fk_cols = [c for c in fk_cols if isinstance(c, str)]

    # Junction tables: composite PK from all FK columns
    if table.get("is_junction_table", False) and fk_cols:
        return list(dict.fromkeys(fk_cols))

    # Association-like: multiple foreign keys and no obvious id column
    has_id_like = any(
        (c.get("name") or "").lower() in {f"{table_name.lower()}_id", "id"}
        or (c.get("name") or "").lower().endswith("_id") and not c.get("is_foreign_key", False)
        for c in cols
    )
    if len(fk_cols) >= 2 and not has_id_like:
        return list(dict.fromkeys(fk_cols))

    # Surrogate key fallback
    surrogate = f"{table_name.lower()}_id"
    return [surrogate]


def _finalize_table_keys_and_nullability(table: Dict[str, Any]) -> None:
    """
    Enforce key invariants:
    - Every table has a PK (inferred if missing)
    - Every PK column exists and is marked is_primary_key
    - PK columns are NOT NULL (nullable = False)
    """
    pk = table.get("primary_key") or []
    if not pk:
        pk = _infer_primary_key_for_table(table)
        table["primary_key"] = pk

    # Deduplicate PK while preserving order
    pk = [p for p in pk if p]
    seen: Set[str] = set()
    pk = [p for p in pk if not (p in seen or seen.add(p))]
    table["primary_key"] = pk

    for pk_attr in pk:
        col = _ensure_column(
            table,
            pk_attr,
            description=f"Primary key for {table.get('name', '')}",
            type_hint="integer" if pk_attr.lower().endswith("_id") else None,
        )
        col["is_primary_key"] = True
        # PK columns cannot be nullable
        col["nullable"] = False


def determine_foreign_key_name(
    pk_column_name: str,
    referenced_table_name: str,
    target_table_columns: List[Dict[str, Any]],
) -> str:
    """
    Determine foreign key column name with collision detection.
    
    **FK Naming Convention**:
    1. Try to use the same name as the referenced PK column
    2. If collision (name already exists in target table), use format: `<referenced_table_name>_<pk_column_name>`
    
    Args:
        pk_column_name: Name of the primary key column in the referenced table
        referenced_table_name: Name of the referenced table
        target_table_columns: List of columns in the target table (to check for collisions)
        
    Returns:
        str: Foreign key column name (either pk_column_name or <table>_<column> format)
        
    Example:
        >>> determine_foreign_key_name("customer_id", "Customer", [{"name": "order_id"}])
        "customer_id"
        >>> determine_foreign_key_name("customer_id", "Customer", [{"name": "order_id"}, {"name": "customer_id"}])
        "Customer_customer_id"
    """
    # Get existing column names in target table
    existing_column_names = {col.get("name", "") for col in target_table_columns}
    
    # Try to use PK column name as-is
    if pk_column_name not in existing_column_names:
        return pk_column_name
    
    # Collision detected - use <table>_<column> format
    fk_name = f"{referenced_table_name}_{pk_column_name}"
    logger.debug(
        f"FK naming collision: '{pk_column_name}' already exists in target table. "
        f"Using '{fk_name}' instead."
    )
    return fk_name


def _get_relation_key(entities: List[str]) -> str:
    """Generate a deterministic key for a relation based on entity names."""
    return "_".join(sorted(entities))


def step_3_5_relational_schema_compilation(
    er_design: Dict[str, Any],  # ER design from Step 3.4
    foreign_keys: List[Dict[str, Any]],  # Foreign keys from Phase 2 (legacy, may be used for reference)
    primary_keys: Dict[str, List[str]],  # entity -> PK from Phase 2 (needed for junction table FKs)
    constraints: Optional[List[Dict[str, Any]]] = None,  # Constraints from Phase 2
    junction_table_names: Optional[Dict[str, str]] = None,  # relation_key -> suggested table name from Step 3.45
) -> Dict[str, Any]:
    """
    Step 3.5 (deterministic): Compile relational schema from ER design.
    
    Implements the ER-to-relational schema conversion algorithm:
    1. For each entity: Create relation with PK, simple attributes, derived attributes
    2. For multivalued attributes: Create separate relations
    3. For binary relationships: Handle 1:1, 1:N, M:N cases
    4. For n-ary relationships: Create junction tables
    5. Apply participation constraints (total → NOT NULL, partial → nullable)
    
    Args:
        er_design: ER design from Step 3.4 with entities, relations, attributes
        foreign_keys: List of foreign key specifications from Phase 2 (legacy, for reference)
        primary_keys: Dictionary mapping entity names to their primary keys
        constraints: Optional list of constraints from Phase 2
        
    Returns:
        dict: Relational schema structure with tables, columns, primary keys, foreign keys
        
    Example:
        >>> schema = step_3_5_relational_schema_compilation(
        ...     er_design={"entities": [{"name": "Customer", "attributes": []}]},
        ...     foreign_keys=[],
        ...     primary_keys={"Customer": ["customer_id"]}
        ... )
        >>> len(schema["tables"]) > 0
        True
    """
    logger.info("Starting Step 3.5: Relational Schema Compilation (deterministic)")
    
    # Validate ER design via Pydantic (avoid raw dict usage internally)
    er_model = ERDesign.model_validate(er_design)
    entities = [e.model_dump() for e in er_model.entities]
    relations = [r.model_dump() for r in er_model.relations]
    attributes = er_model.attributes or {}
    
    tables = []
    processed_multivalued = set()  # Track which multivalued attributes have been processed
    
    # Step 1: For each entity E, create a relation R(E)
    for entity in entities:
        entity_name = entity.get("name", "")
        entity_attrs = entity.get("attributes", [])
        entity_pk = entity.get("primary_key", [])
        
        if not entity_pk:
            # Fallback to primary_keys dict if not in entity
            entity_pk = primary_keys.get(entity_name, [])
        
        # Convert attributes to columns
        columns = []
        multivalued_attrs = []  # Track multivalued attributes for separate tables
        
        for attr in entity_attrs:
            attr_name = extract_attribute_name(attr)
            attr_desc = extract_attribute_description(attr)
            attr_type_hint = extract_attribute_type_hint(attr)
            is_multivalued = extract_attribute_field(attr, "is_multivalued", False)
            is_derived = extract_attribute_field(attr, "is_derived", False)
            is_composite = extract_attribute_field(attr, "is_composite", False)
            decomposition = extract_attribute_field(attr, "decomposition", [])
            
            # Step 1.3: For composite attributes, replace with atomic components
            if is_composite and decomposition:
                # Add decomposed components instead of composite attribute
                for component_name in decomposition:
                    column = {
                        "name": component_name,
                        "description": f"Component of {attr_name}",
                    }
                    if attr_type_hint:
                        column["type_hint"] = attr_type_hint
                    if component_name in entity_pk:
                        column["is_primary_key"] = True
                    columns.append(column)
            elif is_multivalued:
                # Step 2: Multivalued attributes will be handled separately
                multivalued_attrs.append(attr)
            else:
                # Simple or derived attribute
                column = {
                    "name": attr_name,
                    "description": attr_desc,
                }
                if attr_type_hint:
                    column["type_hint"] = attr_type_hint
                if attr_name in entity_pk:
                    column["is_primary_key"] = True
                if is_derived:
                    column["is_derived"] = True
                columns.append(column)
        
        # Build table structure
        table = {
            "name": entity_name,
            "columns": columns,
            "primary_key": entity_pk,
            "foreign_keys": [],
        }
        tables.append(table)
        
        # Step 2: For each multivalued attribute A of entity E, create R(E_A)
        for attr in multivalued_attrs:
            attr_name = extract_attribute_name(attr)
            attr_desc = extract_attribute_description(attr)
            attr_type_hint = extract_attribute_type_hint(attr)
            
            # Create relation R(E_A)
            multivalued_table_name = f"{entity_name}_{attr_name}"
            
            # Add primary key of E as foreign key
            # Create a dummy table structure to check for collisions
            multivalued_table_columns_so_far = []
            fk_columns = []
            for pk_attr in entity_pk:
                # Use FK naming convention with collision detection
                fk_name = determine_foreign_key_name(
                    pk_column_name=pk_attr,
                    referenced_table_name=entity_name,
                    target_table_columns=multivalued_table_columns_so_far,
                )
                fk_columns.append({
                    "name": fk_name,
                    "description": f"Foreign key to {entity_name}.{pk_attr}",
                    "is_foreign_key": True,
                    "references_table": entity_name,
                    "references_attribute": pk_attr,
                })
                multivalued_table_columns_so_far.append({"name": fk_name})
            
            # Add the multivalued attribute
            attr_column = {
                "name": attr_name,
                "description": attr_desc,
            }
            if attr_type_hint:
                attr_column["type_hint"] = attr_type_hint
            
            # Primary key is (E_FK, A) where E_FK might be renamed if collision
            fk_names = [col.get("name") for col in fk_columns]
            multivalued_pk = fk_names + [attr_name]
            
            multivalued_table = {
                "name": multivalued_table_name,
                "columns": fk_columns + [attr_column],
                "primary_key": multivalued_pk,
                "foreign_keys": [
                    {
                        "attributes": [fk_col.get("name")],
                        "references_table": entity_name,
                        "referenced_attributes": [fk_col.get("references_attribute")],
                    }
                    for fk_col in fk_columns
                ],
                "is_multivalued_table": True,
                "source_entity": entity_name,
                "source_attribute": attr_name,
            }
            tables.append(multivalued_table)
            processed_multivalued.add((entity_name, attr_name))
            logger.debug(f"Created multivalued attribute table {multivalued_table_name} for {entity_name}.{attr_name}")
    
    # Step 3: For each binary relationship R between entities E1 and E2
    # Step 4: For each n-ary relationship R(E1, E2, ..., En) where n >= 3
    for relation in relations:
        relation_entities = relation.get("entities", [])
        relation_arity = relation.get("arity", len(relation_entities))
        relation_type = relation.get("type", "")
        relation_desc = relation.get("description", "")
        # Handle None values - if key exists but value is None, use default {}
        entity_cardinalities = relation.get("entity_cardinalities") or {}
        entity_participations = relation.get("entity_participations") or {}
        relation_attributes = relation.get("attributes", [])  # Relationship attributes
        
        if len(relation_entities) < 2:
            logger.warning(f"Skipping relation with < 2 entities: {relation_entities}")
            continue
        
        if relation_arity == 2:
            # Binary relationship
            e1_name = relation_entities[0]
            e2_name = relation_entities[1]
            e1_cardinality = entity_cardinalities.get(e1_name, "N") if entity_cardinalities else "N"
            e2_cardinality = entity_cardinalities.get(e2_name, "N") if entity_cardinalities else "N"
            e1_participation = entity_participations.get(e1_name, "partial") if entity_participations else "partial"
            e2_participation = entity_participations.get(e2_name, "partial") if entity_participations else "partial"
            
            e1_pk = primary_keys.get(e1_name, [])
            e2_pk = primary_keys.get(e2_name, [])
            
            # CRITICAL: Check if FK attributes already exist from Step 2.14
            # Look for FK attributes in the entity's columns that reference the other entity
            e1_table = next((t for t in tables if t["name"] == e1_name), None)
            e2_table = next((t for t in tables if t["name"] == e2_name), None)
            
            # Check if FK already exists in E2 that references E1
            existing_fk_e2_to_e1 = None
            if e2_table:
                for col in e2_table.get("columns", []):
                    if col.get("is_foreign_key") and col.get("references_table") == e1_name:
                        existing_fk_e2_to_e1 = col
                        break
            
            # Check if FK already exists in E1 that references E2
            existing_fk_e1_to_e2 = None
            if e1_table:
                for col in e1_table.get("columns", []):
                    if col.get("is_foreign_key") and col.get("references_table") == e2_name:
                        existing_fk_e1_to_e2 = col
                        break
            
            # Determine relationship type from cardinalities
            if e1_cardinality == "1" and e2_cardinality == "1":
                # 1:1 relationship
                # Choose entity (prefer total participation side)
                # For 1:1, we only reference the first PK column (single-column FK)
                if e2_participation == "total" and e1_participation != "total":
                    # Add E1's PK as FK to E2
                    target_table = e2_table
                    if target_table:
                        # Use existing FK if it exists, otherwise create new one
                        if existing_fk_e2_to_e1:
                            fk_attr = existing_fk_e2_to_e1.get("name")
                            ref_attr = existing_fk_e2_to_e1.get("references_attribute", e1_pk[0] if e1_pk else fk_attr)
                            # Update nullable based on participation
                            existing_fk_e2_to_e1["nullable"] = e2_participation == "partial"
                            # Ensure FK constraint exists
                            if not any(fk.get("attributes") == [fk_attr] for fk in target_table.get("foreign_keys", [])):
                                target_table["foreign_keys"].append({
                                    "attributes": [fk_attr],
                                    "references_table": e1_name,
                                    "referenced_attributes": [ref_attr],
                                })
                        else:
                            # Use FK naming convention with collision detection
                            pk_attr = e1_pk[0] if e1_pk else f"{e1_name.lower()}_id"
                            ref_attr = pk_attr
                            fk_attr = determine_foreign_key_name(
                                pk_column_name=pk_attr,
                                referenced_table_name=e1_name,
                                target_table_columns=target_table.get("columns", []),
                            )
                            # Check if column already exists (might be a regular attribute)
                            existing_col = next((c for c in target_table.get("columns", []) if c.get("name") == fk_attr), None)
                            if existing_col:
                                # Update existing column to be FK
                                existing_col["is_foreign_key"] = True
                                existing_col["references_table"] = e1_name
                                existing_col["references_attribute"] = ref_attr
                                existing_col["nullable"] = e2_participation == "partial"
                            else:
                                target_table["columns"].append({
                                    "name": fk_attr,
                                    "description": f"Foreign key to {e1_name}",
                                    "is_foreign_key": True,
                                    "references_table": e1_name,
                                    "references_attribute": ref_attr,
                                    "nullable": e2_participation == "partial",
                                })
                            target_table["foreign_keys"].append({
                                "attributes": [fk_attr],
                                "references_table": e1_name,
                                "referenced_attributes": [ref_attr],  # Single column reference
                            })
                        # Enforce 1:1 semantics: FK on the referencing side must be UNIQUE
                        _add_unique_constraint(target_table, [fk_attr])
                        # Add relationship attributes to E2
                        for rel_attr in relation_attributes:
                            attr_name = extract_attribute_name(rel_attr)
                            target_table["columns"].append({
                                "name": attr_name,
                                "description": extract_attribute_description(rel_attr),
                            })
                else:
                    # Add E2's PK as FK to E1
                    target_table = next((t for t in tables if t["name"] == e1_name), None)
                    if target_table:
                        # Use FK naming convention with collision detection
                        pk_attr = e2_pk[0] if e2_pk else f"{e2_name.lower()}_id"
                        ref_attr = pk_attr
                        fk_attr = determine_foreign_key_name(
                            pk_column_name=pk_attr,
                            referenced_table_name=e2_name,
                            target_table_columns=target_table.get("columns", []),
                        )
                        target_table["columns"].append({
                            "name": fk_attr,
                            "description": f"Foreign key to {e2_name}",
                            "is_foreign_key": True,
                            "references_table": e2_name,
                            "references_attribute": ref_attr,
                            "nullable": e1_participation == "partial",
                        })
                        target_table["foreign_keys"].append({
                            "attributes": [fk_attr],
                            "references_table": e2_name,
                            "referenced_attributes": [ref_attr],  # Single column reference
                        })
                        # Enforce 1:1 semantics
                        _add_unique_constraint(target_table, [fk_attr])
                        # Add relationship attributes to E1
                        for rel_attr in relation_attributes:
                            attr_name = extract_attribute_name(rel_attr)
                            target_table["columns"].append({
                                "name": attr_name,
                                "description": extract_attribute_description(rel_attr),
                            })
            elif (e1_cardinality == "1" and e2_cardinality == "N") or (e1_cardinality == "N" and e2_cardinality == "1"):
                # 1:N relationship (also covers N:1; FK goes on the N-side)
                # IMPORTANT: for composite PKs, add ALL PK columns.
                if e1_cardinality == "1":
                    one_side = e1_name
                    many_side = e2_name
                    one_pk = e1_pk
                    many_participation = e2_participation
                    existing_fk = existing_fk_e2_to_e1
                else:
                    one_side = e2_name
                    many_side = e1_name
                    one_pk = e2_pk
                    many_participation = e1_participation
                    existing_fk = existing_fk_e1_to_e2
                
                target_table = next((t for t in tables if t["name"] == many_side), None)
                if target_table:
                    # Use existing FK if it exists, otherwise create new one
                    if existing_fk and one_pk:
                        # If Step 2.14 created a single FK column, keep it; otherwise create all needed columns.
                        fk_attr = existing_fk.get("name")
                        ref_attr = existing_fk.get("references_attribute", one_pk[0])
                        existing_fk["nullable"] = many_participation == "partial"
                        if not any(fk.get("attributes") == [fk_attr] for fk in target_table.get("foreign_keys", [])):
                            target_table["foreign_keys"].append({
                                "attributes": [fk_attr],
                                "references_table": one_side,
                                "referenced_attributes": [ref_attr],
                            })
                    else:
                        # Create ALL PK columns as FK(s)
                        fk_attrs: List[str] = []
                        if not one_pk:
                            one_pk = [f"{one_side.lower()}_id"]
                        for pk_attr in one_pk:
                            fk_name = determine_foreign_key_name(
                                pk_column_name=pk_attr,
                                referenced_table_name=one_side,
                                target_table_columns=target_table.get("columns", []),
                            )
                            fk_attrs.append(fk_name)
                            existing_col = next((c for c in target_table.get("columns", []) if c.get("name") == fk_name), None)
                            if existing_col:
                                existing_col["is_foreign_key"] = True
                                existing_col["references_table"] = one_side
                                existing_col["references_attribute"] = pk_attr
                                existing_col["nullable"] = many_participation == "partial"
                            else:
                                target_table["columns"].append({
                                    "name": fk_name,
                                    "description": f"Foreign key to {one_side}.{pk_attr}",
                                    "is_foreign_key": True,
                                    "references_table": one_side,
                                    "references_attribute": pk_attr,
                                    "nullable": many_participation == "partial",
                                })
                        # One FK constraint for composite references
                        if not any(
                            fk.get("attributes") == fk_attrs and fk.get("references_table") == one_side
                            for fk in target_table.get("foreign_keys", [])
                        ):
                            target_table["foreign_keys"].append({
                                "attributes": fk_attrs,
                                "references_table": one_side,
                                "referenced_attributes": list(one_pk),
                            })
                    # Add relationship attributes to N-side
                    for rel_attr in relation_attributes:
                        attr_name = extract_attribute_name(rel_attr)
                        target_table["columns"].append({
                            "name": attr_name,
                            "description": extract_attribute_description(rel_attr),
                        })
            else:
                # M:N relationship (both are "N")
                # Create new relation R
                # Check if we have a suggested name from Step 3.45
                relation_key = _get_relation_key([e1_name, e2_name])
                if junction_table_names and relation_key in junction_table_names:
                    junction_table_name = junction_table_names[relation_key]
                else:
                    junction_table_name = "_".join(sorted([e1_name, e2_name]))
                junction_columns = []
                junction_pk = []
                junction_fks = []
                
                # Add primary keys of both entities as foreign keys
                # CRITICAL: For composite primary keys, we must add ALL PK columns, not just the first one
                for entity_name, entity_pk_list in [(e1_name, e1_pk), (e2_name, e2_pk)]:
                    if not entity_pk_list:
                        # Fallback: create a single-column FK
                        fk_attr = f"{entity_name.lower()}_id"
                        entity_participation = entity_participations.get(entity_name, "partial") if entity_participations else "partial"
                        junction_columns.append({
                            "name": fk_attr,
                            "description": f"Foreign key to {entity_name}",
                            "is_foreign_key": True,
                            "references_table": entity_name,
                            "references_attribute": fk_attr,
                            "nullable": entity_participation == "partial",
                        })
                        junction_pk.append(fk_attr)
                        junction_fks.append({
                            "attributes": [fk_attr],
                            "references_table": entity_name,
                            "referenced_attributes": [fk_attr],
                        })
                    else:
                        # Add ALL primary key columns (handles composite PKs correctly)
                        # CRITICAL: Use determine_foreign_key_name to avoid collision
                        entity_participation = entity_participations.get(entity_name, "partial") if entity_participations else "partial"
                        fk_attrs = []
                        for pk_attr in entity_pk_list:
                            # Use collision detection for FK naming
                            fk_name = determine_foreign_key_name(
                                pk_column_name=pk_attr,
                                referenced_table_name=entity_name,
                                target_table_columns=junction_columns,
                            )
                            junction_columns.append({
                                "name": fk_name,
                                "description": f"Foreign key to {entity_name}.{pk_attr}",
                                "is_foreign_key": True,
                                "references_table": entity_name,
                                "references_attribute": pk_attr,
                                "nullable": entity_participation == "partial",
                            })
                            junction_pk.append(fk_name)
                            fk_attrs.append(fk_name)
                        # Create a single FK constraint for all PK columns
                        junction_fks.append({
                            "attributes": fk_attrs,
                            "references_table": entity_name,
                            "referenced_attributes": entity_pk_list,
                        })
                
                # Add relationship attributes
                for rel_attr in relation_attributes:
                    attr_name = extract_attribute_name(rel_attr)
                    # Step 5: Decompose composite relationship attributes
                    is_composite = extract_attribute_field(rel_attr, "is_composite", False)
                    decomposition = extract_attribute_field(rel_attr, "decomposition", [])
                    if is_composite and decomposition:
                        for component_name in decomposition:
                            junction_columns.append({
                                "name": component_name,
                                "description": f"Component of {attr_name}",
                            })
                    else:
                        junction_columns.append({
                            "name": attr_name,
                            "description": extract_attribute_description(rel_attr),
                        })
                
                # Primary key is (E1_PK, E2_PK)
                junction_table = {
                    "name": junction_table_name,
                    "columns": junction_columns,
                    "primary_key": junction_pk,
                    "foreign_keys": junction_fks,
                    "is_junction_table": True,
                    "relation_description": relation_desc,
                }
                tables.append(junction_table)
                logger.debug(f"Created junction table {junction_table_name} for M:N relation between {e1_name} and {e2_name}")
        
        elif relation_arity >= 3:
            # Step 4: N-ary relationship (n >= 3)
            # Special case: ternary relation with an associative entity already present.
            # Example: Order - OrderItem - Book where OrderItem holds relationship attributes.
            if relation_arity == 3 and len(relation_entities) == 3:
                a, b, c = relation_entities[0], relation_entities[1], relation_entities[2]
                candidates = []
                for x in (a, b, c):
                    x_pk = primary_keys.get(x, [])
                    x_table = next((t for t in tables if t["name"] == x), None)
                    others = tuple(e for e in (a, b, c) if e != x)
                    if _is_associative_entity_candidate(x, x_pk, x_table, other_entities=(others[0], others[1])):
                        candidates.append(x)
                if len(candidates) == 1:
                    assoc = candidates[0]
                    assoc_table = next((t for t in tables if t["name"] == assoc), None)
                    if assoc_table:
                        other_entities = [e for e in relation_entities if e != assoc]
                        fk_pair: List[str] = []
                        for other in other_entities:
                            other_pk = primary_keys.get(other, []) or [f"{other.lower()}_id"]
                            for pk_attr in other_pk:
                                fk_name = determine_foreign_key_name(
                                    pk_column_name=pk_attr,
                                    referenced_table_name=other,
                                    target_table_columns=assoc_table.get("columns", []),
                                )
                                fk_pair.append(fk_name)
                                existing_col = next((c for c in assoc_table.get("columns", []) if c.get("name") == fk_name), None)
                                if existing_col:
                                    existing_col["is_foreign_key"] = True
                                    existing_col["references_table"] = other
                                    existing_col["references_attribute"] = pk_attr
                                    existing_col.setdefault("nullable", False)
                                else:
                                    assoc_table["columns"].append({
                                        "name": fk_name,
                                        "description": f"Foreign key to {other}.{pk_attr}",
                                        "is_foreign_key": True,
                                        "references_table": other,
                                        "references_attribute": pk_attr,
                                        "nullable": False,
                                    })
                            # Composite FK constraint per referenced entity
                            assoc_table.setdefault("foreign_keys", []).append({
                                "attributes": fk_pair[-len(other_pk):],
                                "references_table": other,
                                "referenced_attributes": list(other_pk),
                            })
                        # If associative entity had no PK, use composite key of the two FKs; otherwise enforce uniqueness.
                        if not (assoc_table.get("primary_key") or []):
                            assoc_table["primary_key"] = list(dict.fromkeys(fk_pair))
                        else:
                            _add_unique_constraint(assoc_table, list(dict.fromkeys(fk_pair)))
                        logger.debug(
                            f"Handled ternary relation {relation_entities} by enriching associative entity {assoc} "
                            f"with FKs to {other_entities}"
                        )
                        continue

            # Default: Create new relation R (junction table)
            # Check if we have a suggested name from Step 3.45
            relation_key = _get_relation_key(relation_entities)
            if junction_table_names and relation_key in junction_table_names:
                junction_table_name = junction_table_names[relation_key]
            else:
                junction_table_name = "_".join(sorted(relation_entities))
            junction_columns = []
            junction_pk = []
            junction_fks = []
            
            # Add primary keys of all participating entities as foreign keys
            # CRITICAL: For composite primary keys, we must add ALL PK columns, not just the first one
            for entity_name in relation_entities:
                entity_pk_list = primary_keys.get(entity_name, [])
                if not entity_pk_list:
                    # Fallback: create a single-column FK
                    fk_attr = f"{entity_name.lower()}_id"
                    entity_participation = entity_participations.get(entity_name, "partial") if entity_participations else "partial"
                    junction_columns.append({
                        "name": fk_attr,
                        "description": f"Foreign key to {entity_name}",
                        "is_foreign_key": True,
                        "references_table": entity_name,
                        "references_attribute": fk_attr,
                        "nullable": entity_participation == "partial",
                    })
                    junction_pk.append(fk_attr)
                    junction_fks.append({
                        "attributes": [fk_attr],
                        "references_table": entity_name,
                        "referenced_attributes": [fk_attr],
                    })
                else:
                    # Add ALL primary key columns (handles composite PKs correctly)
                    # CRITICAL: Use determine_foreign_key_name to avoid collision
                    entity_participation = entity_participations.get(entity_name, "partial") if entity_participations else "partial"
                    fk_attrs = []
                    for pk_attr in entity_pk_list:
                        # Use collision detection for FK naming
                        fk_name = determine_foreign_key_name(
                            pk_column_name=pk_attr,
                            referenced_table_name=entity_name,
                            target_table_columns=junction_columns,
                        )
                        junction_columns.append({
                            "name": fk_name,
                            "description": f"Foreign key to {entity_name}.{pk_attr}",
                            "is_foreign_key": True,
                            "references_table": entity_name,
                            "references_attribute": pk_attr,
                            "nullable": entity_participation == "partial",
                        })
                        junction_pk.append(fk_name)
                        fk_attrs.append(fk_name)
                    # Create a single FK constraint for all PK columns
                    junction_fks.append({
                        "attributes": fk_attrs,
                        "references_table": entity_name,
                        "referenced_attributes": entity_pk_list,
                    })
            
            # Add relationship attributes
            for rel_attr in relation_attributes:
                attr_name = extract_attribute_name(rel_attr)
                # Step 5: Decompose composite relationship attributes
                is_composite = extract_attribute_field(rel_attr, "is_composite", False)
                decomposition = extract_attribute_field(rel_attr, "decomposition", [])
                if is_composite and decomposition:
                    for component_name in decomposition:
                        junction_columns.append({
                            "name": component_name,
                            "description": f"Component of {attr_name}",
                        })
                else:
                    junction_columns.append({
                        "name": attr_name,
                        "description": extract_attribute_description(rel_attr),
                    })
            
            # Primary key is (E1_PK, E2_PK, ..., En_PK)
            junction_table = {
                "name": junction_table_name,
                "columns": junction_columns,
                "primary_key": junction_pk,
                "foreign_keys": junction_fks,
                "is_junction_table": True,
                "relation_description": relation_desc,
            }
            tables.append(junction_table)
            logger.debug(f"Created junction table {junction_table_name} for {relation_arity}-ary relation")
    
    logger.info(
        f"Relational schema compilation completed: {len(tables)} tables created "
        f"({len([t for t in tables if not t.get('is_junction_table', False) and not t.get('is_multivalued_table', False)])} entity tables, "
        f"{len([t for t in tables if t.get('is_multivalued_table', False)])} multivalued attribute tables, "
        f"{len([t for t in tables if t.get('is_junction_table', False)])} junction tables)"
    )

    # Final pass: enforce PK invariants & NOT NULL for PK columns
    # This fixes common edge cases:
    # - Junction tables where FK columns were marked nullable due to missing participation info
    # - Association-like entity tables that ended up with no PK (e.g., OrderItem)
    for t in tables:
        _finalize_table_keys_and_nullability(t)
    
    relational_schema = {
        "tables": tables,
    }
    
    return relational_schema
