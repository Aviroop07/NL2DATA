"""Naming validation tools for entity/attribute names and conventions."""

import re
from typing import Dict, Any
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


@tool
def check_entity_name_validity(name: str) -> Dict[str, Any]:
    """Check if an entity name is valid (SQL-safe, no reserved keywords).
    
    Args:
        name: Entity name to validate
        
    Returns:
        Dictionary with 'valid' (bool), 'error' (str), and 'suggestion' (str)
    """
    if not name or not name.strip():
        raise ToolException("Entity name is empty")
    
    name = name.strip()
    
    # Check for SQL reserved keywords
    sql_keywords = [
        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
        "ALTER", "TABLE", "INDEX", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
        "CONSTRAINT", "UNIQUE", "NOT", "NULL", "DEFAULT", "CHECK", "AND", "OR",
    ]
    
    if name.upper() in sql_keywords:
        suggestion = f"Use '{name}_entity' or '{name}_table' instead"
        raise ToolException(f"Entity name '{name}' is a SQL reserved keyword. Suggestion: {suggestion}")
    
    # Check for valid SQL identifier (alphanumeric + underscore, starts with letter)
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", name):
        suggestion = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        raise ToolException(f"Entity name '{name}' contains invalid characters. Must start with letter and contain only alphanumeric and underscore. Suggestion: {suggestion}")
    
    return {"valid": True, "error": None, "suggestion": None}


@tool
def check_attribute_name_validity(name: str) -> Dict[str, Any]:
    """Check if an attribute name is valid (SQL-safe, no reserved keywords).
    
    Args:
        name: Attribute name to validate
        
    Returns:
        Dictionary with 'valid' (bool), 'error' (str), and 'suggestion' (str)
    """
    # Same validation as entity name
    return check_entity_name_validity(name)


@tool
def check_naming_convention(name: str, convention: str = "snake_case") -> bool:
    """Check if a name follows the specified naming convention.
    
    Args:
        name: Name to check
        convention: Naming convention ("snake_case", "camelCase", "PascalCase", "lowercase")
        
    Returns:
        True if name follows convention, False otherwise
    """
    if convention == "snake_case":
        return bool(re.match(r"^[a-z][a-z0-9_]*$", name))
    elif convention == "camelCase":
        return bool(re.match(r"^[a-z][a-zA-Z0-9]*$", name))
    elif convention == "PascalCase":
        return bool(re.match(r"^[A-Z][a-zA-Z0-9]*$", name))
    elif convention == "lowercase":
        return name.islower() and name.replace("_", "").isalnum()
    else:
        return True  # Unknown convention, assume valid


@tool
def check_name_reserved(name: str) -> bool:
    """Check if a name is a reserved keyword (SQL or Python).
    
    Args:
        name: Name to check
        
    Returns:
        True if name is reserved, False otherwise
    """
    sql_keywords = [
        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
        "ALTER", "TABLE", "INDEX", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
        "CONSTRAINT", "UNIQUE", "NOT", "NULL", "DEFAULT", "CHECK", "AND", "OR",
        "AS", "ON", "IN", "LIKE", "BETWEEN", "IS", "EXISTS", "GROUP", "BY",
        "ORDER", "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT",
        "FULL", "OUTER", "UNION", "ALL", "DISTINCT", "COUNT", "SUM", "AVG", "MAX", "MIN",
    ]
    
    python_keywords = [
        "and", "as", "assert", "break", "class", "continue", "def", "del", "elif",
        "else", "except", "exec", "finally", "for", "from", "global", "if", "import",
        "in", "is", "lambda", "not", "or", "pass", "print", "raise", "return", "try",
        "while", "with", "yield", "True", "False", "None",
    ]
    
    name_upper = name.upper()
    name_lower = name.lower()
    
    return name_upper in sql_keywords or name_lower in python_keywords

