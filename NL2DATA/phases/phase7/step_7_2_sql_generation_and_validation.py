"""Phase 7, Step 7.2: SQL Generation and Validation.

For each information need, generate SQL statement and validate it's executable on the schema.
Max 5 retries per information need. Only include info needs with valid, executable SQL.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
import sqlite3
import tempfile
import os

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class SQLGenerationOutput(BaseModel):
    """Output structure for SQL generation."""
    sql_query: str = Field(
        description="SQL SELECT statement to retrieve the information"
    )
    reasoning: str = Field(
        description="Reasoning for the SQL query structure"
    )

    model_config = ConfigDict(extra="forbid")


class SQLGenerationAndValidationOutput(BaseModel):
    """Output structure for SQL generation and validation."""
    information_need: Dict[str, Any] = Field(
        description="The information need that was processed"
    )
    sql_query: str = Field(
        description="Generated SQL SELECT statement"
    )
    is_valid: bool = Field(
        description="Whether the SQL query is valid and executable"
    )
    validation_error: Optional[str] = Field(
        default=None,
        description="Error message if validation failed"
    )
    retry_count: int = Field(
        description="Number of retry attempts made"
    )
    reasoning: str = Field(
        description="Reasoning for the SQL query structure"
    )

    model_config = ConfigDict(extra="forbid")


def _validate_sql_on_schema(
    sql_query: str,
    relational_schema: Dict[str, Any],
) -> tuple[bool, Optional[str]]:
    """
    Validate that SQL query is executable on the schema (empty tables, just syntax/structure check).
    
    Args:
        sql_query: SQL SELECT statement
        relational_schema: Relational schema from Phase 4
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        # Create temporary in-memory SQLite database
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        
        # Create tables from schema (empty, no data)
        tables = relational_schema.get("tables", [])
        for table in tables:
            table_name = table.get("name", "")
            columns = table.get("columns", [])
            primary_key = table.get("primary_key", [])
            foreign_keys = table.get("foreign_keys", [])
            
            # Build CREATE TABLE statement
            col_defs = []
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "TEXT")
                nullable = col.get("nullable", True)
                
                # Build column definition
                col_def = f'"{col_name}" {col_type}'
                if not nullable:
                    col_def += " NOT NULL"
                col_defs.append(col_def)
            
            # Add PRIMARY KEY constraint
            if primary_key:
                pk_cols = ", ".join(f'"{pk}"' for pk in primary_key)
                col_defs.append(f"PRIMARY KEY ({pk_cols})")
            
            # Add FOREIGN KEY constraints (simplified - just check syntax)
            for fk in foreign_keys:
                fk_attrs = fk.get("attributes", [])
                ref_table = fk.get("references_table", "")
                ref_attrs = fk.get("referenced_attributes", [])
                if fk_attrs and ref_table and ref_attrs:
                    fk_cols = ", ".join(f'"{attr}"' for attr in fk_attrs)
                    ref_cols = ", ".join(f'"{attr}"' for attr in ref_attrs)
                    col_defs.append(f"FOREIGN KEY ({fk_cols}) REFERENCES \"{ref_table}\" ({ref_cols})")
            
            create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})'
            
            try:
                cursor.execute(create_table_sql)
            except sqlite3.Error as e:
                logger.warning(f"Error creating table {table_name}: {e}")
                # Continue with other tables
        
        conn.commit()
        
        # Try to prepare the query (validate syntax and structure)
        try:
            cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}")
            # If we get here, the query is syntactically valid
            conn.close()
            return True, None
        except sqlite3.Error as e:
            error_msg = str(e)
            conn.close()
            return False, error_msg
            
    except Exception as e:
        logger.error(f"Error validating SQL: {e}")
        return False, str(e)


@traceable_step("7.2", phase=7, tags=['phase_7_step_2'])
async def step_7_2_sql_generation_and_validation(
    information_need: Dict[str, Any],
    relational_schema: Dict[str, Any],
    nl_description: str,
    domain: Optional[str] = None,
    max_retries: int = 5,
) -> SQLGenerationAndValidationOutput:
    """
    Step 6.2 (per-information need, LLM with retries): Generate SQL and validate it's executable.
    
    IMPORTANT:
    - No schema modification - only validates SQL against existing schema
    - Max 5 retries per information need
    - Only returns info needs with valid, executable SQL
    
    Args:
        information_need: Information need with description, entities_involved
        relational_schema: Relational schema from Phase 4
        nl_description: Original natural language description
        domain: Optional domain context
        max_retries: Maximum number of retries (default 5)
        
    Returns:
        dict: SQL generation result with sql_query, is_valid, validation_error, retry_count
        If is_valid is False after max_retries, the info need should be rejected
    """
    logger.debug(f"Generating SQL for information need: {information_need.get('description', '')}")
    
    # Build schema summary for LLM
    schema_summary = []
    tables = relational_schema.get("tables", [])
    for table in tables:
        table_name = table.get("name", "")
        columns = table.get("columns", [])
        primary_key = table.get("primary_key", [])
        
        col_info = []
        for col in columns:
            col_name = col.get("name", "")
            col_type = col.get("type", "TEXT")
            nullable = col.get("nullable", True)
            col_info.append(f"{col_name} ({col_type}{' NOT NULL' if not nullable else ''})")
        
        schema_summary.append(f"Table: {table_name}")
        schema_summary.append(f"  Primary Key: {', '.join(primary_key) if primary_key else 'None'}")
        schema_summary.append(f"  Columns: {', '.join(col_info)}")
        
        # Add foreign keys
        foreign_keys = table.get("foreign_keys", [])
        if foreign_keys:
            fk_info = []
            for fk in foreign_keys:
                fk_attrs = fk.get("attributes", [])
                ref_table = fk.get("references_table", "")
                ref_attrs = fk.get("referenced_attributes", [])
                if fk_attrs and ref_table and ref_attrs:
                    fk_info.append(f"{', '.join(fk_attrs)} -> {ref_table}({', '.join(ref_attrs)})")
            if fk_info:
                schema_summary.append(f"  Foreign Keys: {', '.join(fk_info)}")
        schema_summary.append("")
    
    system_prompt = """You are a SQL query generator. Generate SQL SELECT statements to retrieve information from a database schema.

IMPORTANT:
- Generate valid SQL that can be executed on the given schema
- Use proper table and column names from the schema
- Include necessary JOINs for related tables
- Use appropriate WHERE clauses for filtering
- Return only SELECT statements (no INSERT, UPDATE, DELETE)

The schema is provided with table names, columns, primary keys, and foreign keys."""
    
    human_prompt = f"""Information Need:
Description: {information_need.get('description', '')}
Entities involved: {', '.join(information_need.get('entities_involved', []))}

Database Schema:
{chr(10).join(schema_summary)}

Natural Language Description:
{nl_description}

Domain: {domain or 'Not specified'}

Generate a SQL SELECT statement to retrieve this information from the schema.
The query must be syntactically valid and executable on the provided schema."""
    
    llm = get_model_for_step("7.2")
    trace_config = get_trace_config("7.2", phase=7, tags=["phase_7_step_2"])
    
    # Retry loop
    for retry_count in range(max_retries):
        try:
            result = await standardized_llm_call(
                llm=llm,
                output_schema=SQLGenerationOutput,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt,
                input_data={},
                config=trace_config,
            )
            
            sql_query = result.sql_query.strip()
            
            # Validate SQL on schema
            is_valid, error_msg = _validate_sql_on_schema(sql_query, relational_schema)
            
            if is_valid:
                logger.info(f"SQL validation successful for info need '{information_need.get('description', '')}' after {retry_count + 1} attempt(s)")
                return SQLGenerationAndValidationOutput(
                    information_need=information_need,
                    sql_query=sql_query,
                    is_valid=True,
                    validation_error=None,
                    retry_count=retry_count + 1,
                    reasoning=result.reasoning,
                )
            else:
                logger.warning(
                    f"SQL validation failed for info need '{information_need.get('description', '')}' "
                    f"(attempt {retry_count + 1}/{max_retries}): {error_msg}"
                )
                
                # If not last retry, add error feedback to prompt
                if retry_count < max_retries - 1:
                    human_prompt += f"\n\nPrevious attempt failed with error: {error_msg}\nPlease fix the SQL query."
                else:
                    # Last retry failed
                    return SQLGenerationAndValidationOutput(
                        information_need=information_need,
                        sql_query=sql_query,
                        is_valid=False,
                        validation_error=error_msg,
                        retry_count=retry_count + 1,
                        reasoning=result.reasoning,
                    )
                    
        except Exception as e:
            logger.error(f"Error generating SQL for info need '{information_need.get('description', '')}': {e}")
            if retry_count == max_retries - 1:
                return SQLGenerationAndValidationOutput(
                    information_need=information_need,
                    sql_query="",
                    is_valid=False,
                    validation_error=str(e),
                    retry_count=retry_count + 1,
                    reasoning=f"Error during SQL generation: {str(e)}",
                )
    
    # Should not reach here, but just in case
    return SQLGenerationAndValidationOutput(
        information_need=information_need,
        sql_query="",
        is_valid=False,
        validation_error="Max retries exceeded",
        retry_count=max_retries,
        reasoning="Failed to generate valid SQL after maximum retries",
    )


async def step_7_2_sql_generation_and_validation_batch(
    information_needs: List[Dict[str, Any]],
    relational_schema: Dict[str, Any],
    nl_description: str,
    domain: Optional[str] = None,
    max_retries: int = 5,
) -> Dict[str, Any]:
    """
    Step 6.2: Generate and validate SQL for all information needs (parallel execution).
    
    Args:
        information_needs: List of information needs from Step 6.1
        relational_schema: Relational schema from Phase 4
        nl_description: Original natural language description
        domain: Optional domain context
        max_retries: Maximum number of retries per info need
        
    Returns:
        dict: Batch results with valid_info_needs (with SQL) and invalid_info_needs
    """
    logger.info(f"Starting Step 6.2: SQL Generation and Validation for {len(information_needs)} information needs")
    
    import asyncio
    
    tasks = [
        step_7_2_sql_generation_and_validation(
            information_need=need,
            relational_schema=relational_schema,
            nl_description=nl_description,
            domain=domain,
            max_retries=max_retries,
        )
        for need in information_needs
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Separate valid and invalid info needs
    valid_info_needs = []
    invalid_info_needs = []
    
    for result in results:
        # Convert Pydantic model to dict if needed
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
        else:
            result_dict = result
        
        if result_dict.get("is_valid", False):
            valid_info_needs.append({
                "information_need": result_dict["information_need"],
                "sql_query": result_dict["sql_query"],
                "reasoning": result_dict.get("reasoning", ""),
            })
        else:
            invalid_info_needs.append({
                "information_need": result_dict["information_need"],
                "validation_error": result_dict.get("validation_error", ""),
                "retry_count": result_dict.get("retry_count", 0),
            })
    
    logger.info(f"SQL validation completed: {len(valid_info_needs)} valid, {len(invalid_info_needs)} invalid information needs")
    
    return {
        "valid_info_needs": valid_info_needs,
        "invalid_info_needs": invalid_info_needs,
        "total_info_needs": len(information_needs),
    }
