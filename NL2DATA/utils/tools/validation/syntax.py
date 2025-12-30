"""Syntax validation tools for SQL, DSL, and formula expressions."""

from typing import Dict, Any, Optional
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


@tool
def validate_sql_syntax(sql: str) -> Dict[str, Any]:
    """Validate SQL syntax is correct and parseable.
    
    Args:
        sql: SQL statement to validate
        
    Returns:
        Dictionary with 'valid' (bool) and 'error' (str, if invalid) keys
        
    Purpose: Validates SQL syntax before including it in the output. Currently
    performs basic syntax checks. For production, should use a SQL parser.
    """
    sql = sql.strip()
    
    # Basic validation
    if not sql:
        raise ToolException("SQL statement is empty")
    
    # Check for balanced parentheses
    if sql.count("(") != sql.count(")"):
        return {"valid": False, "error": "Unbalanced parentheses"}
    
    # Check for balanced quotes
    single_quotes = sql.count("'") - sql.count("\\'")
    double_quotes = sql.count('"') - sql.count('\\"')
    if single_quotes % 2 != 0:
        return {"valid": False, "error": "Unbalanced single quotes"}
    if double_quotes % 2 != 0:
        return {"valid": False, "error": "Unbalanced double quotes"}
    
    # Check for basic SQL keywords (very basic check)
    sql_upper = sql.upper()
    if any(keyword in sql_upper for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"]):
        # Looks like valid SQL
        return {"valid": True, "error": None}
    
    return {"valid": True, "error": None}  # Assume valid if passes basic checks


@tool
def validate_sql_type(type_str: str) -> bool:
    """Validate that a SQL data type string is valid.
    
    Args:
        type_str: SQL type string (e.g., "VARCHAR(255)", "INT", "DECIMAL(10,2)")
        
    Returns:
        True if type is valid, False otherwise
    """
    type_str = type_str.strip().upper()
    
    # Valid SQL types (common ones)
    valid_types = [
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
        "VARCHAR", "CHAR", "TEXT", "CLOB",
        "DATE", "TIME", "TIMESTAMP", "DATETIME",
        "BOOLEAN", "BOOL",
        "BLOB", "BINARY", "VARBINARY",
        "JSON", "JSONB", "XML",
    ]
    
    # Extract base type (before parentheses)
    base_type = type_str.split("(")[0].strip()
    
    return base_type in valid_types


@tool
def validate_formula_syntax(formula: str) -> Dict[str, Any]:
    """Validate formula syntax (for derived attributes).
    
    Args:
        formula: Formula expression to validate
        
    Returns:
        Dictionary with 'valid' (bool) and 'error' (str, if invalid) keys
    """
    formula = formula.strip()
    
    if not formula:
        return {"valid": False, "error": "Formula is empty"}
    
    # Check for balanced parentheses
    if formula.count("(") != formula.count(")"):
        return {"valid": False, "error": "Unbalanced parentheses"}
    
    # Check for balanced brackets
    if formula.count("[") != formula.count("]"):
        return {"valid": False, "error": "Unbalanced brackets"}
    
    # Check for valid operators (basic check)
    valid_operators = ["+", "-", "*", "/", "=", ">", "<", ">=", "<=", "!=", "AND", "OR", "NOT"]
    # This is a very basic check - in production, use a proper formula parser
    
    return {"valid": True, "error": None}


def _validate_dsl_expression_impl(dsl: str, grammar: Optional[str] = None) -> Dict[str, Any]:
    """Pure implementation of DSL expression validation (no @tool decorator).
    
    Args:
        dsl: DSL expression to validate
        grammar: Optional grammar specification (for future use)
        
    Returns:
        Dictionary with 'valid' (bool) and 'error' (str, if invalid) keys
    """
    dsl = dsl.strip()
    
    if not dsl:
        return {"valid": False, "error": "DSL expression is empty"}
    
    # Basic validation - check for balanced parentheses and brackets
    if dsl.count("(") != dsl.count(")"):
        return {"valid": False, "error": "Unbalanced parentheses"}
    
    if dsl.count("[") != dsl.count("]"):
        return {"valid": False, "error": "Unbalanced brackets"}
    
    # In production, should parse against actual DSL grammar
    return {"valid": True, "error": None}


@tool
def validate_dsl_expression(dsl: str, grammar: Optional[str] = None) -> Dict[str, Any]:
    """Validate DSL expression follows provided grammar rules.
    
    Args:
        dsl: DSL expression to validate
        grammar: Optional grammar specification (for future use)
        
    Returns:
        Dictionary with 'valid' (bool) and 'error' (str, if invalid) keys
    """
    return _validate_dsl_expression_impl(dsl, grammar)

