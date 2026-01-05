"""Phase 6, Step 6.1: DDL Compilation.

Generate CREATE TABLE statements from LogicalIR schema.
Deterministic transformation - converts LogicalIR table structure to SQL DDL syntax.
"""

import json
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class DDLCompilationOutput(BaseModel):
    """Output structure for DDL compilation."""
    ddl_statements: List[str] = Field(
        description="List of CREATE TABLE DDL statements"
    )

    model_config = ConfigDict(extra="forbid")

def _values_compatible_with_sql_type(sql_type: str, values: List[str]) -> bool:
    """Return True if the values look compatible with the SQL type (very conservative).

    We use this to avoid producing invalid DDL like:
      tax_region_id BIGINT CHECK (tax_region_id IN ('state','province',...))
    """
    t = (sql_type or "").upper().strip()
    if not values:
        return True
    numeric_types = {"INT", "INTEGER", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"}
    if any(t.startswith(x) for x in numeric_types):
        # All values must be parseable as numbers
        for v in values:
            s = str(v).strip()
            if s == "":
                return False
            try:
                float(s)
            except Exception:
                return False
        return True
    if t == "BOOLEAN":
        allowed = {"true", "false", "0", "1"}
        return all(str(v).strip().lower() in allowed for v in values)
    # For everything else (VARCHAR/TEXT/DATE/TIMESTAMP), allow strings.
    return True


def _format_check_value_for_type(sql_type: str, value: str) -> Optional[str]:
    """Format a single CHECK-IN value literal based on SQL type; return None if incompatible."""
    t = (sql_type or "").upper().strip()
    v = str(value).strip()
    if t == "BOOLEAN":
        low = v.lower()
        if low in {"true", "false"}:
            return low.upper()
        if low in {"0", "1"}:
            return "TRUE" if low == "1" else "FALSE"
        return None

    numeric_types = {"INT", "INTEGER", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"}
    if any(t.startswith(x) for x in numeric_types):
        try:
            float(v)
            return v
        except Exception:
            return None

    # Default: quote as string literal.
    return f"'{_escape_string(v)}'"


def step_6_1_ddl_compilation(
    normalized_schema: Dict[str, Any],  # Normalized schema
    data_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # entity -> attribute -> type info
    check_constraints: Optional[Dict[str, Dict[str, List[str]]]] = None,  # entity -> attribute -> values
) -> DDLCompilationOutput:
    """
    Step 10.1 (deterministic): Generate CREATE TABLE statements from normalized schema.
    
    This is a deterministic transformation that converts the normalized LogicalIR schema
    into executable SQL DDL statements. No LLM call needed.
    
    Args:
        normalized_schema: Normalized schema with normalized_tables
        data_types: Optional dictionary mapping entity -> attribute -> type info
        check_constraints: Optional dictionary mapping entity -> attribute -> allowed values
        
    Returns:
        dict: DDL compilation result with ddl_statements list
        
    Example:
        >>> schema = {
        ...     "normalized_tables": [{
        ...         "name": "Customer",
        ...         "columns": [{"name": "customer_id", "nullable": False}],
        ...         "primary_key": ["customer_id"]
        ...     }]
        ... }
        >>> result = step_6_1_ddl_compilation(schema)
        >>> len(result["ddl_statements"]) > 0
        True
        >>> "CREATE TABLE" in result["ddl_statements"][0]
        True
    """
    logger.info("Starting Step 10.1: DDL Compilation (deterministic)")
    
    normalized_tables = normalized_schema.get("normalized_tables", [])
    if not normalized_tables:
        logger.warning("No normalized tables found in schema")
        return DDLCompilationOutput(ddl_statements=[])
    
    # Build a lookup map of table names to their column names for validation
    table_column_map = {}
    # Also build FK reference map for circular dependency detection
    fk_references_map = {}  # table_name -> list of FK definitions
    for table in normalized_tables:
        table_name = table.get("name", "")
        if table_name:
            table_column_map[table_name] = {
                col.get("name", "") for col in table.get("columns", [])
            }
            # Store FK definitions for circular reference detection
            fk_references_map[table_name] = table.get("foreign_keys", [])
    
    ddl_statements = []
    
    for table in normalized_tables:
        table_name = table.get("name", "")
        if not table_name:
            logger.warning("Skipping table with no name")
            continue
        
        columns = table.get("columns", [])
        primary_key = table.get("primary_key", [])
        foreign_keys = table.get("foreign_keys", [])
        
        # Build column definitions
        column_defs = []

        def _type_info_to_col_type(type_info: Dict[str, Any]) -> str:
            sql_type = (type_info.get("type", "VARCHAR") or "VARCHAR")
            size = type_info.get("size")
            precision = type_info.get("precision")
            scale = type_info.get("scale")
            if str(sql_type).upper() in ("VARCHAR", "CHAR"):
                if size:
                    return f"{sql_type}({size})"
                return f"{sql_type}(255)"
            if str(sql_type).upper() in ("DECIMAL", "NUMERIC"):
                if precision and scale is not None:
                    return f"{sql_type}({precision},{scale})"
                if precision:
                    return f"{sql_type}({precision})"
            return str(sql_type)

        # Deterministic FK type harmonization for DDL:
        # If we know the referenced key column type, force FK column types to match in DDL.
        fk_type_overrides: Dict[str, Dict[str, Any]] = {}
        if data_types and foreign_keys:
            for fk in foreign_keys:
                fk_attrs = fk.get("attributes", []) or []
                ref_table = fk.get("references_table", "")
                ref_attrs = fk.get("referenced_attributes", []) or []
                if not (ref_table and fk_attrs and ref_attrs) or len(fk_attrs) != len(ref_attrs):
                    continue
                for fk_col, ref_col in zip(fk_attrs, ref_attrs):
                    if not (isinstance(fk_col, str) and isinstance(ref_col, str)):
                        continue
                    ref_type_info = (data_types.get(ref_table, {}) or {}).get(ref_col)
                    if isinstance(ref_type_info, dict) and (ref_type_info.get("type") or "").strip():
                        fk_type_overrides[fk_col] = ref_type_info
        for col in columns:
            col_name = col.get("name", "")
            if not col_name:
                continue
            
            # Get data type information
            col_type = "VARCHAR(255)"  # Default
            col_nullable = col.get("nullable", True)
            col_default = col.get("default", None)
            
            # Try to get type from data_types dict
            if data_types:
                entity_name = table_name
                if entity_name in data_types and col_name in data_types[entity_name]:
                    type_info = data_types[entity_name][col_name]
                    sql_type = type_info.get("type", "VARCHAR")
                    size = type_info.get("size")
                    precision = type_info.get("precision")
                    scale = type_info.get("scale")
                    
                    # Build type string
                    if sql_type.upper() in ("VARCHAR", "CHAR"):
                        if size:
                            col_type = f"{sql_type}({size})"
                        else:
                            col_type = f"{sql_type}(255)"  # Default size
                    elif sql_type.upper() in ("DECIMAL", "NUMERIC"):
                        if precision and scale is not None:
                            col_type = f"{sql_type}({precision},{scale})"
                        elif precision:
                            col_type = f"{sql_type}({precision})"
                        else:
                            col_type = sql_type
                    else:
                        col_type = sql_type
                else:
                    # Fallback: use type_hint from column if available
                    type_hint = col.get("type_hint", "")
                    if type_hint:
                        col_type = _infer_type_from_hint(type_hint)
            else:
                # Fallback: use type_hint from column if available
                type_hint = col.get("type_hint", "")
                if type_hint:
                    col_type = _infer_type_from_hint(type_hint)

            # Apply FK type override if we know the referenced key type.
            if col_name in fk_type_overrides:
                override_type = _type_info_to_col_type(fk_type_overrides[col_name])
                if override_type and override_type != col_type:
                    logger.warning(
                        f"Table {table_name}: Overriding type of FK column '{col_name}' from {col_type} to {override_type} "
                        f"to match referenced key type."
                    )
                col_type = override_type or col_type
            
            # Build column definition
            col_def = f"    {_escape_identifier(col_name)} {col_type}"
            
            # Add NOT NULL if not nullable
            if not col_nullable:
                col_def += " NOT NULL"
            
            # Add DEFAULT if specified
            if col_default is not None:
                if isinstance(col_default, str) and col_default.upper() in ("CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"):
                    col_def += f" DEFAULT {col_default}"
                else:
                    col_def += f" DEFAULT {_format_default_value(col_default)}"
            
            # Add CHECK constraint if categorical
            if check_constraints:
                entity_name = table_name
                if entity_name in check_constraints and col_name in check_constraints[entity_name]:
                    allowed_values = check_constraints[entity_name][col_name]
                    if allowed_values:
                        if not _values_compatible_with_sql_type(col_type, allowed_values):
                            logger.warning(
                                f"Table {table_name}: Skipping CHECK for {col_name} because values are not compatible with type {col_type}. "
                                f"Values={allowed_values}"
                            )
                        else:
                            formatted: List[str] = []
                            ok = True
                            for v in allowed_values:
                                lit = _format_check_value_for_type(col_type, v)
                                if lit is None:
                                    ok = False
                                    break
                                formatted.append(lit)
                            if not ok or not formatted:
                                logger.warning(
                                    f"Table {table_name}: Skipping CHECK for {col_name} due to type/value incompatibility after formatting. "
                                    f"type={col_type}, values={allowed_values}"
                                )
                            else:
                                values_str = ", ".join(formatted)
                                col_def += f" CHECK ({_escape_identifier(col_name)} IN ({values_str}))"
            
            # Add CHECK constraint from Phase 2 if available
            check_condition = col.get("check_condition")
            if check_condition:
                col_def += f" CHECK ({check_condition})"
            
            column_defs.append(col_def)
        
        # Build PRIMARY KEY constraint
        # CRITICAL: Validate that PK columns don't contain expressions (SQLite doesn't support expressions in PK)
        pk_constraint = ""
        if primary_key:
            # Validate PK columns: ensure they're simple column names, not expressions
            valid_pk_cols = []
            for pk in primary_key:
                # Check if PK contains expression-like syntax (parentheses, operators, etc.)
                if any(char in pk for char in ['(', ')', '+', '-', '*', '/', ' ', '||']):
                    logger.warning(
                        f"Table {table_name}: Primary key column '{pk}' appears to be an expression. "
                        f"SQLite doesn't support expressions in PRIMARY KEY. Skipping this PK column."
                    )
                    continue
                # Check if PK column actually exists in the table
                if pk not in {col.get("name", "") for col in columns}:
                    logger.warning(
                        f"Table {table_name}: Primary key column '{pk}' not found in table columns. Skipping."
                    )
                    continue
                valid_pk_cols.append(pk)
            
            if valid_pk_cols:
                pk_cols = ", ".join(_escape_identifier(pk) for pk in valid_pk_cols)
                pk_constraint = f"    PRIMARY KEY ({pk_cols})"
            else:
                logger.warning(f"Table {table_name}: No valid primary key columns found. Table will be created without PRIMARY KEY constraint.")
        
        # Build FOREIGN KEY constraints
        # CRITICAL: Validate foreign keys to ensure columns exist and counts match
        # Also detect circular references
        fk_constraints = []
        current_table_columns = {col.get("name", "") for col in columns}
        fk_references = {}  # Track FK references to detect circular dependencies
        
        for fk in foreign_keys:
            fk_attrs = fk.get("attributes", [])
            ref_table = fk.get("references_table", "")
            ref_attrs = fk.get("referenced_attributes", [])
            on_delete = fk.get("on_delete", None)
            on_update = fk.get("on_update", None)
            
            if not (fk_attrs and ref_table and ref_attrs):
                logger.warning(
                    f"Table {table_name}: Skipping incomplete foreign key: "
                    f"fk_attrs={fk_attrs}, ref_table={ref_table}, ref_attrs={ref_attrs}"
                )
                continue
            
            # Validate: FK column count must match referenced column count
            if len(fk_attrs) != len(ref_attrs):
                logger.warning(
                    f"Table {table_name}: Foreign key column count mismatch. "
                    f"FK columns: {fk_attrs} ({len(fk_attrs)}), "
                    f"Referenced columns: {ref_attrs} ({len(ref_attrs)}). Skipping this FK."
                )
                continue
            
            # Validate: All FK columns must exist in current table
            missing_fk_cols = [attr for attr in fk_attrs if attr not in current_table_columns]
            if missing_fk_cols:
                logger.warning(
                    f"Table {table_name}: Foreign key columns not found in table: {missing_fk_cols}. Skipping this FK."
                )
                continue
            
            # Validate: All referenced columns must exist in referenced table
            if ref_table in table_column_map:
                ref_table_columns = table_column_map[ref_table]
                missing_ref_cols = [attr for attr in ref_attrs if attr not in ref_table_columns]
                if missing_ref_cols:
                    logger.warning(
                        f"Table {table_name}: Referenced columns not found in table {ref_table}: {missing_ref_cols}. Skipping this FK."
                    )
                    continue
                
                # Check for circular reference: if ref_table references back to table_name
                # Check both already-processed FKs and FKs from the schema
                circular_detected = False
                
                # Check already-processed FKs
                if ref_table in fk_references:
                    ref_table_fks = fk_references[ref_table]
                    for ref_fk in ref_table_fks:
                        if ref_fk.get("references_table") == table_name:
                            logger.warning(
                                f"Table {table_name}: Circular foreign key detected! "
                                f"{table_name} -> {ref_table} -> {table_name}. "
                                f"Skipping FK from {table_name} to {ref_table} to break the cycle."
                            )
                            circular_detected = True
                            break
                
                # Also check FKs from schema (for tables processed later)
                if not circular_detected and ref_table in fk_references_map:
                    ref_table_schema_fks = fk_references_map[ref_table]
                    for ref_fk in ref_table_schema_fks:
                        if ref_fk.get("references_table") == table_name:
                            logger.warning(
                                f"Table {table_name}: Circular foreign key detected! "
                                f"{table_name} -> {ref_table} -> {table_name}. "
                                f"Skipping FK from {table_name} to {ref_table} to break the cycle."
                            )
                            circular_detected = True
                            break
                
                if circular_detected:
                    continue
            else:
                # Referenced table not found - might be created later, but log a warning
                logger.warning(
                    f"Table {table_name}: Referenced table '{ref_table}' not found in schema. "
                    f"FK will be created but may fail if table doesn't exist."
                )
            
            # Track this FK reference for circular dependency detection
            if table_name not in fk_references:
                fk_references[table_name] = []
            fk_references[table_name].append(fk)
            
            # All validations passed - create FK constraint
            fk_cols = ", ".join(_escape_identifier(attr) for attr in fk_attrs)
            ref_cols = ", ".join(_escape_identifier(attr) for attr in ref_attrs)
            
            fk_def = f"    FOREIGN KEY ({fk_cols}) REFERENCES {_escape_identifier(ref_table)} ({ref_cols})"
            
            if on_delete:
                fk_def += f" ON DELETE {on_delete}"
            if on_update:
                fk_def += f" ON UPDATE {on_update}"
            
            fk_constraints.append(fk_def)
        
        # Build UNIQUE constraints
        unique_constraints = []
        # Check column-level unique constraints
        for col in columns:
            if col.get("unique", False):
                col_name = col.get("name", "")
                if col_name:
                    unique_constraints.append(f"    UNIQUE ({_escape_identifier(col_name)})")
        # Also check table-level unique constraints
        table_unique = table.get("unique_constraints", [])
        for unique_set in table_unique:
            if isinstance(unique_set, list):
                unique_cols = ", ".join(_escape_identifier(attr) for attr in unique_set)
                unique_constraints.append(f"    UNIQUE ({unique_cols})")
        
        # Validate: Skip tables with no columns (invalid DDL)
        if not column_defs:
            logger.warning(
                f"Table {table_name}: No valid columns found. Skipping table creation. "
                f"This may indicate a normalization issue (empty decomposed table)."
            )
            continue
        
        # Combine all parts
        all_constraints = []
        if pk_constraint:
            all_constraints.append(pk_constraint)
        all_constraints.extend(fk_constraints)
        all_constraints.extend(unique_constraints)
        
        # Build CREATE TABLE statement
        ddl = f"CREATE TABLE {_escape_identifier(table_name)} (\n"
        ddl += ",\n".join(column_defs)
        if all_constraints:
            ddl += ",\n" + ",\n".join(all_constraints)
        ddl += "\n);"
        
        ddl_statements.append(ddl)
    
    logger.info(f"DDL compilation completed: {len(ddl_statements)} CREATE TABLE statements generated")
    
    # Log the complete DDL statements
    logger.info("=== DDL STATEMENTS (Step 6.1 Output) ===")
    for i, ddl in enumerate(ddl_statements, 1):
        logger.info(f"--- DDL Statement {i}/{len(ddl_statements)} ---")
        logger.info(ddl)
    logger.info("=== END DDL STATEMENTS ===")
    
    return DDLCompilationOutput(ddl_statements=ddl_statements)


def _escape_identifier(identifier: str) -> str:
    """Escape SQL identifier (table/column name) for safety."""
    # Use double quotes for PostgreSQL/SQLite, backticks for MySQL
    # For portability, use double quotes (works in PostgreSQL, SQLite; MySQL can be configured)
    if not identifier:
        return '""'
    # Simple escaping: wrap in double quotes and escape any double quotes inside
    return f'"{identifier.replace('"', '""')}"'


def _escape_string(value: str) -> str:
    """Escape string value for SQL."""
    # Escape single quotes by doubling them
    return value.replace("'", "''")


def _format_default_value(value: Any) -> str:
    """Format default value for SQL."""
    if isinstance(value, str):
        return f"'{_escape_string(value)}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif value is None:
        return "NULL"
    else:
        return str(value)


def _infer_type_from_hint(type_hint: str) -> str:
    """Infer SQL type from type hint."""
    hint_lower = type_hint.lower()
    if "int" in hint_lower or "integer" in hint_lower:
        return "INTEGER"
    elif "float" in hint_lower or "decimal" in hint_lower or "numeric" in hint_lower:
        return "DECIMAL(10,2)"
    elif "date" in hint_lower:
        return "DATE"
    elif "time" in hint_lower:
        if "stamp" in hint_lower:
            return "TIMESTAMP"
        return "TIME"
    elif "bool" in hint_lower:
        return "BOOLEAN"
    elif "text" in hint_lower or "string" in hint_lower or "char" in hint_lower:
        return "VARCHAR(255)"
    else:
        return "VARCHAR(255)"  # Safe default

