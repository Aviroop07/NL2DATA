"""Shared utilities for SQL data type assignment.

This module centralizes:
- portable name/type_hint heuristics (deterministic fallback)
- Pydantic output schemas used by LLM-powered typing steps

It is intentionally phase-agnostic so multiple phases can reuse it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, ConfigDict
from pydantic.json_schema import JsonSchemaMode
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema

from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_type_hint,
)

_DATE_TOKENS = ("date",)
_TS_TOKENS = ("datetime", "timestamp", "time")


def _is_timestamp_like(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return False
    if n.endswith("_at"):
        return True
    return any(t in n for t in _TS_TOKENS)


def _is_date_like(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return False
    # Prefer TIMESTAMP if also time-like
    if _is_timestamp_like(n):
        return False
    return any(t in n for t in _DATE_TOKENS) or n.endswith("_date")


def _infer_type_from_name_and_hint(
    attr_name: str, type_hint: Optional[str]
) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
    """Return (sql_type, size, precision, scale) using portable defaults."""
    n = (attr_name or "").strip().lower()
    h = (type_hint or "").strip().lower()

    if h:
        # Honor explicit hints first
        if "bool" in h:
            return "BOOLEAN", None, None, None
        if "timestamp" in h or "datetime" in h:
            return "TIMESTAMP", None, None, None
        if h == "date" or "date only" in h:
            return "DATE", None, None, None
        if "int" in h or "integer" in h or "bigint" in h:
            return "BIGINT", None, None, None
        if "decimal" in h or "numeric" in h or "money" in h:
            return "DECIMAL", None, 12, 2
        if "float" in h or "double" in h or "real" in h:
            return "DOUBLE", None, None, None
        if "json" in h:
            return "JSON", None, None, None

    # Name-based heuristics
    if n.endswith("_id") or n in {"id"}:
        return "BIGINT", None, None, None
    if n.startswith("is_") or n.startswith("has_") or n.endswith("_flag") or n.endswith("_enabled") or n.endswith("_breached"):
        return "BOOLEAN", None, None, None
    if _is_timestamp_like(n):
        return "TIMESTAMP", None, None, None
    if _is_date_like(n):
        return "DATE", None, None, None
    if any(t in n for t in ("price", "amount", "subtotal", "total", "fee", "cost", "revenue")):
        return "DECIMAL", None, 12, 2
    if "percent" in n or n.endswith("_pct") or n.endswith("_percentage") or n.endswith("_rate"):
        return "DECIMAL", None, 6, 4
    if any(t in n for t in ("quantity", "count", "num_", "_num", "days", "minutes", "seconds", "hours", "km", "distance")):
        return "INT", None, None, None
    if "description" in n or "notes" in n or "comment" in n:
        return "TEXT", None, None, None

    return "VARCHAR", 255, None, None


def _deterministic_type_assignment(
    *,
    entity_name: str,
    attributes: List[Dict[str, Any]],
    primary_key: Optional[List[str]],
) -> Dict[str, Any]:
    """Deterministic fallback when the LLM fails to produce a valid schema."""
    pk_set = {a for a in (primary_key or []) if isinstance(a, str) and a}
    out: Dict[str, Any] = {}
    for attr in attributes or []:
        attr_name = extract_attribute_name(attr)
        if not attr_name:
            continue
        hint = extract_attribute_type_hint(attr)
        sql_type, size, precision, scale = _infer_type_from_name_and_hint(attr_name, hint)
        if attr_name in pk_set and sql_type not in {"INT", "BIGINT", "UUID", "VARCHAR"}:
            sql_type, size, precision, scale = "BIGINT", None, None, None
        reasoning = (
            f"Deterministic fallback: inferred {sql_type}"
            + (f"({size})" if sql_type == "VARCHAR" and size else "")
            + (f"({precision},{scale})" if sql_type == "DECIMAL" and precision is not None and scale is not None else "")
            + f" for '{attr_name}' based on name/type_hint."
        )
        out[attr_name] = {
            "type": sql_type,
            "size": size,
            "precision": precision,
            "scale": scale,
            "reasoning": reasoning,
        }
    return {"attribute_types": out}


class AttributeTypeInfo(BaseModel):
    """Information about a single attribute's data type for Step 5.2/5.4.
    
    This structure is focused on SQL data type assignment only.
    Constraints are handled in separate steps (Phase 8) and not included here.
    """
    model_config = ConfigDict(
        extra="forbid",
        schema_generator=OpenAICompatibleJsonSchema
    )
    
    type: str = Field(
        description="SQL data type (e.g., 'VARCHAR', 'INT', 'DECIMAL', 'DATE', 'TIMESTAMP', 'BOOLEAN')"
    )
    size: Optional[int] = Field(
        default=None,
        description="Size for VARCHAR/CHAR types (e.g., VARCHAR(255) -> size=255)"
    )
    precision: Optional[int] = Field(
        default=None,
        description="Precision for DECIMAL/NUMERIC types (total digits)"
    )
    scale: Optional[int] = Field(
        default=None,
        description="Scale for DECIMAL/NUMERIC types (digits after decimal point)"
    )
    reasoning: str = Field(
        description="REQUIRED - Explanation of why this data type was chosen"
    )


class DataTypeAssignmentOutput(BaseModel):
    """Output schema: attribute name -> type info.
    
    This structure uses a dictionary for attribute_types to allow direct lookup by attribute name.
    For single-attribute requests, the dictionary MUST contain exactly one entry with the 
    requested attribute name as the key.
    """
    model_config = ConfigDict(
        extra="forbid",
        schema_generator=OpenAICompatibleJsonSchema
    )
    
    attribute_types: Dict[str, AttributeTypeInfo] = Field(
        description=(
            "Dictionary mapping attribute names to their type information. "
            "CRITICAL: The keys in this dictionary MUST be the exact attribute names provided in the request. "
            "For a single attribute request, this dictionary MUST contain exactly one entry with the attribute name as the key. "
            "The dictionary MUST NOT be empty (empty dicts will trigger retry with error feedback)."
        )
    )

