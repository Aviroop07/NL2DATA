"""Phase 6, Step 6.2: DDL Validation.

Validate DDL statements for syntax errors, naming conflicts, or database-specific issues.
Deterministic validation - uses SQL parser or local database execution.
"""

from typing import Dict, Any, List, Optional
import sqlite3
import io
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class SyntaxError(BaseModel):
    """A syntax error in a DDL statement."""
    statement: str = Field(description="The DDL statement with the error")
    error: str = Field(description="Error message")
    line: int = Field(description="Line number (if available)")

    model_config = ConfigDict(extra="forbid")


class DDLValidationOutput(BaseModel):
    """Output structure for DDL validation."""
    validation_passed: bool = Field(description="Whether validation passed")
    syntax_errors: List[SyntaxError] = Field(description="List of syntax errors")
    naming_conflicts: List[str] = Field(description="List of naming conflicts")
    warnings: List[str] = Field(description="List of warnings")
    parse_tree: Optional[Any] = Field(default=None, description="Parse tree (if available)")

    model_config = ConfigDict(extra="forbid")


def step_6_2_ddl_validation(
    ddl_statements: List[str],  # DDL statements from Step 6.1
    validate_with_db: bool = True,  # Whether to validate by executing in SQLite
) -> DDLValidationOutput:
    """
    Step 6.2 (deterministic): Validate DDL statements.
    
    This is a deterministic validation that checks DDL syntax and attempts to execute
    the statements in a local SQLite database to catch errors. No LLM call needed.
    
    Args:
        ddl_statements: List of CREATE TABLE statements from Step 6.1
        validate_with_db: Whether to validate by executing in SQLite (default: True)
        
    Returns:
        dict: Validation result with validation_passed, syntax_errors, naming_conflicts, warnings
        
    Example:
        >>> ddl = ["CREATE TABLE Customer (customer_id INTEGER PRIMARY KEY);"]
        >>> result = step_6_2_ddl_validation(ddl)
        >>> result["validation_passed"]
        True
    """
    logger.info("Starting Step 6.2: DDL Validation (deterministic)")
    
    syntax_errors = []
    naming_conflicts = []
    warnings = []
    
    # Track table names for conflict detection
    table_names = set()
    
    # Basic syntax checks
    for idx, ddl in enumerate(ddl_statements):
        if not ddl or not ddl.strip():
            syntax_errors.append(SyntaxError(
                statement=f"Statement {idx + 1}",
                error="Empty DDL statement",
                line=1
            ))
            continue
        
        # Check for CREATE TABLE
        if "CREATE TABLE" not in ddl.upper():
            syntax_errors.append(SyntaxError(
                statement=ddl,
                error="Missing CREATE TABLE keyword",
                line=1
            ))
            continue
        
        # Extract table name (simple regex-like extraction)
        try:
            # Find table name after CREATE TABLE
            upper_ddl = ddl.upper()
            create_idx = upper_ddl.find("CREATE TABLE")
            if create_idx != -1:
                start_idx = create_idx + len("CREATE TABLE")
                # Skip whitespace
                while start_idx < len(ddl) and ddl[start_idx] in " \t\n":
                    start_idx += 1
                # Find end of table name (whitespace, parenthesis, or quote)
                end_idx = start_idx
                if ddl[start_idx] == '"':
                    # Quoted identifier
                    end_idx = ddl.find('"', start_idx + 1) + 1
                else:
                    while end_idx < len(ddl) and ddl[end_idx] not in " \t\n(":
                        end_idx += 1
                
                table_name = ddl[start_idx:end_idx].strip('"')
                
                # Check for naming conflicts
                if table_name in table_names:
                    naming_conflicts.append(f"Duplicate table name: {table_name}")
                else:
                    table_names.add(table_name)
        except Exception as e:
            warnings.append(f"Could not extract table name from statement {idx + 1}: {e}")
    
    # Validate with SQLite if requested
    if validate_with_db:
        try:
            # Create in-memory database
            conn = sqlite3.connect(":memory:")
            cursor = conn.cursor()
            
            for idx, ddl in enumerate(ddl_statements):
                try:
                    cursor.execute(ddl)
                except sqlite3.Error as e:
                    syntax_errors.append(SyntaxError(
                        statement=ddl,
                        error=str(e),
                        line=1  # SQLite doesn't provide line numbers easily
                    ))
            
            conn.close()
        except Exception as e:
            warnings.append(f"Database validation failed: {e}. Syntax checks still performed.")
    
    validation_passed = len(syntax_errors) == 0 and len(naming_conflicts) == 0
    
    if validation_passed:
        logger.info(f"DDL validation passed: {len(ddl_statements)} statements validated")
    else:
        logger.warning(
            f"DDL validation failed: {len(syntax_errors)} syntax errors, "
            f"{len(naming_conflicts)} naming conflicts"
        )
    
    return DDLValidationOutput(
        validation_passed=validation_passed,
        syntax_errors=syntax_errors,
        naming_conflicts=naming_conflicts,
        warnings=warnings,
        parse_tree=None  # Could be enhanced with actual SQL parser
    )

