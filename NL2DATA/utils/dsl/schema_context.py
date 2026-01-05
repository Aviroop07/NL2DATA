"""Schema context for DSL semantic validation.

User requirement:
- "full schema: tables, attributes, attribute type. That's it."

We model that minimal schema and provide helpers to:
- Resolve identifiers (col vs table.col)
- Map SQL-ish types to coarse categories for type checking
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Set


DSLType = str  # "number" | "string" | "boolean" | "datetime" | "date" | "time" | "unknown" | "null"


def _normalize_type_str(type_str: Optional[str]) -> str:
    t = (type_str or "").strip().lower()
    if not t:
        return ""
    # Drop length/precision: varchar(255) -> varchar, decimal(10,2) -> decimal
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    return t


def _map_sql_type_to_dsl_type(type_str: Optional[str]) -> DSLType:
    t = _normalize_type_str(type_str)
    if not t:
        return "unknown"

    if t in {"bool", "boolean"}:
        return "boolean"

    if t in {"date"}:
        return "date"
    if t in {"time"}:
        return "time"
    if t in {"timestamp", "datetime"}:
        return "datetime"

    if t in {
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "float",
        "double",
        "real",
        "decimal",
        "numeric",
    }:
        return "number"

    if t in {"varchar", "char", "text", "clob", "string"}:
        return "string"

    # JSON can be treated as unknown in DSL (we avoid complex typing here).
    return "unknown"


@dataclass(frozen=True)
class DSLTableSchema:
    columns: Dict[str, DSLType]  # column_name -> DSLType


@dataclass(frozen=True)
class DSLSchemaContext:
    """Minimal schema context for semantic validation."""

    tables: Dict[str, DSLTableSchema]  # table_name -> table schema

    def all_columns_index(self) -> Dict[str, Set[str]]:
        """Return col_name -> {tables that contain col_name}."""
        idx: Dict[str, Set[str]] = {}
        for tname, ts in self.tables.items():
            for col in ts.columns.keys():
                idx.setdefault(col, set()).add(tname)
        return idx

    def resolve_identifier(self, identifier: str, anchor_table: Optional[str] = None) -> tuple[Optional[str], Optional[str], Optional[DSLType], Optional[str]]:
        """Resolve identifier to (table, column, type, error).

        Rules:
        - If anchor_table is provided, bare identifiers resolve first to anchor_table columns (anchor-first resolution).
        - Allow bare column name if it exists in exactly one table (or in anchor_table if provided).
        - Allow table.column.
        - Reject deeper paths (schema.table.column) for now.
        
        Args:
            identifier: The identifier to resolve
            anchor_table: Optional anchor table name for anchor-first resolution
            
        Returns:
            Tuple of (table_name, column_name, type, error_message)
        """
        raw = (identifier or "").strip()
        if not raw:
            return None, None, None, "Empty identifier"

        parts = [p for p in raw.split(".") if p]
        if len(parts) == 1:
            # Bare identifier - use anchor-first resolution if anchor_table is provided
            col = parts[0]
            
            # Anchor-first resolution: check anchor_table first
            if anchor_table:
                anchor_schema = self.tables.get(anchor_table)
                if anchor_schema and col in anchor_schema.columns:
                    # Found in anchor table - resolve to anchor table (no ambiguity)
                    return anchor_table, col, anchor_schema.columns.get(col, "unknown"), None
            
            # Fall back to global resolution
            idx = self.all_columns_index().get(col, set())
            if not idx:
                return None, None, None, f"Unknown column '{col}'"
            
            # If anchor_table was provided but column not found there, check if ambiguous
            if anchor_table:
                # Column exists in other tables but not in anchor table
                # This is valid - it's not ambiguous because anchor-first already checked
                if len(idx) == 1:
                    tname = next(iter(idx))
                    ctype = self.tables.get(tname).columns.get(col, "unknown") if self.tables.get(tname) else "unknown"
                    return tname, col, ctype, None
                # Multiple tables have this column, but not anchor table
                # This is still valid - resolve to first match (or could be made stricter)
                tname = next(iter(idx))
                ctype = self.tables.get(tname).columns.get(col, "unknown") if self.tables.get(tname) else "unknown"
                return tname, col, ctype, None
            
            # No anchor table - use existing global resolution
            if len(idx) > 1:
                return None, None, None, f"Ambiguous column '{col}' found in tables {sorted(idx)}; use Table.{col}"
            tname = next(iter(idx))
            ctype = self.tables.get(tname).columns.get(col, "unknown") if self.tables.get(tname) else "unknown"
            return tname, col, ctype, None

        if len(parts) == 2:
            tname, col = parts
            ts = self.tables.get(tname)
            if ts is None:
                return None, None, None, f"Unknown table '{tname}' in identifier '{raw}'"
            if col not in ts.columns:
                return None, None, None, f"Unknown column '{col}' in table '{tname}'"
            return tname, col, ts.columns.get(col, "unknown"), None

        return None, None, None, f"Unsupported identifier '{raw}'. Use column or Table.column only."


def build_schema_context_from_relational_schema(relational_schema: Dict[str, object]) -> DSLSchemaContext:
    """Build a DSLSchemaContext from Step 3.5 relational schema dict.

    Expects:
      relational_schema = {"tables": [{"name": "...", "columns": [{"name": "...", "type": "..." or "type_hint": "..."}]}]}
    """
    tables_out: Dict[str, DSLTableSchema] = {}
    tables = relational_schema.get("tables", []) if isinstance(relational_schema, dict) else []
    if not isinstance(tables, list):
        tables = []
    for t in tables:
        if not isinstance(t, dict):
            continue
        tname = (t.get("name") or "").strip()
        if not tname:
            continue
        cols = t.get("columns", []) or []
        if not isinstance(cols, list):
            cols = []
        col_map: Dict[str, DSLType] = {}
        for c in cols:
            if not isinstance(c, dict):
                continue
            cname = (c.get("name") or "").strip()
            if not cname:
                continue
            # prefer explicit type, fallback to type_hint
            ctype_raw = c.get("type") or c.get("sql_type") or c.get("type_hint")
            col_map[cname] = _map_sql_type_to_dsl_type(str(ctype_raw) if ctype_raw is not None else None)
        tables_out[tname] = DSLTableSchema(columns=col_map)
    return DSLSchemaContext(tables=tables_out)

