"""Query validation tools for SQL queries against schema."""

import re
from typing import Dict, Any, List
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


def _validate_query_against_schema_impl(sql: str, schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Pure (non-LangChain-tool) implementation of validate_query_against_schema."""
    errors: List[str] = []
    warnings: List[str] = []

    sql_str = (sql or "").strip()
    if not sql_str:
        return {"valid": False, "errors": ["SQL is empty"], "warnings": warnings}

    # NOTE: Still intentionally lightweight; production should use a SQL parser.
    sql_upper = sql_str.upper()

    entities = schema_state.get("entities", [])
    entity_names = set()
    for e in entities:
        entity_name = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        if entity_name:
            entity_names.add(entity_name.upper())

    # Very rough extraction: look for FROM <word> / JOIN <word>
    tokens = re.split(r"\s+", sql_upper)
    for i, tok in enumerate(tokens[:-1]):
        if tok in ("FROM", "JOIN"):
            candidate = tokens[i + 1].strip().strip(",")
            candidate = candidate.strip('"').strip("'")
            if candidate and candidate not in entity_names:
                warnings.append(f"Table '{candidate}' not found in schema entities")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


@tool
def validate_query_against_schema(sql: str, schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that a SQL query references valid tables and columns from schema.
    
    Args:
        sql: SQL query to validate
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'valid' (bool), 'errors' (List[str]), and 'warnings' (List[str])
    """
    return _validate_query_against_schema_impl(sql, schema_state)

