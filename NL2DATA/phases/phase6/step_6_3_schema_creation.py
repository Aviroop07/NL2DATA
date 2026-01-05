"""Phase 6, Step 6.3: Schema Creation.

Execute DDL statements to create actual database schema in a local database.
Deterministic execution - executes validated DDL statements in SQLite.
"""

from typing import Dict, Any, List, Optional
import sqlite3
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class SchemaCreationOutput(BaseModel):
    """Output structure for schema creation."""
    success: bool = Field(description="Whether schema creation succeeded")
    errors: List[str] = Field(description="List of error messages (if any)")
    tables_created: List[str] = Field(description="List of table names created")

    model_config = ConfigDict(extra="forbid")


def step_6_3_schema_creation(
    ddl_statements: List[str],  # Validated DDL statements from Step 6.2
    database_path: Optional[str] = None,  # Path to database file (None = in-memory)
) -> SchemaCreationOutput:
    """
    Step 6.3 (deterministic): Execute DDL statements to create database schema.
    
    This is a deterministic execution that creates the actual database schema
    by executing DDL statements in a local SQLite database.
    
    Args:
        ddl_statements: List of validated CREATE TABLE statements from Step 6.2
        database_path: Optional path to database file (None = in-memory for testing)
        
    Returns:
        SchemaCreationOutput: Schema creation result with success, errors, tables_created
        
    Example:
        >>> ddl = ["CREATE TABLE Customer (customer_id INTEGER PRIMARY KEY);"]
        >>> result = step_6_3_schema_creation(ddl)
        >>> result.success
        True
        >>> "Customer" in result.tables_created
        True
    """
    logger.info("Starting Step 6.3: Schema Creation (deterministic)")
    
    errors = []
    tables_created = []
    
    try:
        # Connect to database (in-memory for testing, or file path if provided)
        if database_path:
            conn = sqlite3.connect(database_path)
        else:
            conn = sqlite3.connect(":memory:")
        
        cursor = conn.cursor()
        
        # Execute each DDL statement
        for ddl in ddl_statements:
            try:
                cursor.execute(ddl)
                
                # Extract table name from DDL (simple extraction)
                # This is a best-effort extraction for logging
                if "CREATE TABLE" in ddl.upper():
                    # Try to find table name
                    parts = ddl.upper().split("CREATE TABLE")
                    if len(parts) > 1:
                        table_part = parts[1].strip()
                        # Find first identifier (table name)
                        table_name = table_part.split()[0].strip('"').strip("'")
                        if table_name:
                            tables_created.append(table_name)
                
            except sqlite3.Error as e:
                error_msg = f"Failed to execute DDL: {str(e)}"
                errors.append(error_msg)
                logger.error(f"DDL execution error: {error_msg}\nDDL: {ddl}")
        
        # Commit changes
        conn.commit()
        conn.close()
        
        success = len(errors) == 0
        
        if success:
            logger.info(f"Schema creation completed: {len(tables_created)} tables created")
        else:
            logger.warning(f"Schema creation completed with errors: {len(errors)} errors, {len(tables_created)} tables created")
        
        return SchemaCreationOutput(
            success=success,
            errors=errors,
            tables_created=tables_created
        )
        
    except Exception as e:
        error_msg = f"Schema creation failed: {str(e)}"
        logger.error(error_msg)
        return SchemaCreationOutput(
            success=False,
            errors=[error_msg],
            tables_created=tables_created
        )
