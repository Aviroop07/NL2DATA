"""Phase 6, Step 6.5: SQL Query Generation.

Generate SQL queries for identified information needs.
Converts natural language query descriptions into executable SQL.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase6.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools import validate_sql_syntax
from NL2DATA.utils.tools.validation_tools import _validate_query_against_schema_impl
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class SQLQueryGenerationOutput(BaseModel):
    """Output structure for SQL query generation."""
    sql: str = Field(description="Generated SQL query")
    validation_status: str = Field(description="Validation status: 'valid', 'invalid', 'needs_review'")
    corrected_sql: Optional[str] = Field(
        default=None,
        description="Corrected SQL if validation failed (null if validation passed)"
    )
    reasoning: str = Field(description="Reasoning for the SQL query structure and any corrections")

    model_config = ConfigDict(extra="forbid")


@traceable_step("6.5", phase=10, tags=['phase_10_step_5'])
async def step_6_5_sql_query_generation(
    information_need: Dict[str, Any],  # Information need
    normalized_schema: Dict[str, Any],  # Full normalized schema
    data_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # Data types
    related_entities: Optional[List[str]] = None,  # Related entities for this information need
    relations: Optional[List[Dict[str, Any]]] = None,  # Relations from Phase 1
) -> SQLQueryGenerationOutput:
    """
    Step 5.5 (per-information need, LLM): Generate SQL query for an information need.
    
    This step converts a natural language information need description into
    executable SQL that matches the generated schema.
    
    Args:
        information_need: Information need with description, entities_involved
        normalized_schema: Full normalized schema
        data_types: Optional data types
        related_entities: Optional list of related entity names
        relations: Optional list of relations from Phase 1
        
    Returns:
        dict: SQL query generation result with sql, validation_status, corrected_sql, reasoning
        
    Example:
        >>> info_need = {"description": "Find all customers", "entities_involved": ["Customer"]}
        >>> result = await step_6_5_sql_query_generation(info_need, {...})
        >>> "SELECT" in result["sql"]
        True
    """
    logger.debug(f"Generating SQL query for information need: {information_need.get('description', '')}")
    
    # Build comprehensive schema context with full column details
    schema_summary = []
    tables = normalized_schema.get("normalized_tables", [])
    
    for table in tables:
        table_name = table.get("name", "")
        columns = table.get("columns", [])
        pk = table.get("primary_key", [])
        fks = table.get("foreign_keys", [])
        
        # Build detailed schema summary with all columns
        col_names = [col.get("name", "") for col in columns]
        
        # Build detailed schema summary
        fk_details = []
        for fk in fks[:5]:  # Limit FK details to avoid too long context
            ref_table = fk.get("references_table", "")
            fk_attrs = fk.get("attributes", [])
            ref_attrs = fk.get("referenced_attributes", [])
            if ref_table:
                fk_details.append(f"{fk_attrs} -> {ref_table}({ref_attrs})")
        
        schema_summary.append(
            f"Table {table_name}:\n"
            f"  Columns: {col_names}\n"
            f"  Primary Key: {pk}\n"
            f"  Foreign Keys: {fk_details if fk_details else 'None'}"
        )
    
    # Get model
    model = get_model_for_step("5.5")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=SQLQueryGenerationOutput,
        additional_requirements=[
            "After using validation tools (if needed), you MUST return ONLY the JSON object",
            "DO NOT return markdown, explanations, or any text outside the JSON object",
            "The \"sql\" field MUST ALWAYS be a valid SQL query string - NEVER null, NEVER empty, NEVER missing",
            "Even if validation fails, you MUST still provide a valid SQL query in the \"sql\" field",
            "If validation fails, set \"validation_status\" to \"invalid\" and provide a corrected version in \"corrected_sql\" if possible",
        ]
    )
    
    # Create prompt
    system_prompt = f"""You are a SQL query generation expert. Your task is to convert natural language information needs into executable SQL queries.

SQL GENERATION RULES:
1. **Use correct table and column names** from the provided schema - verify columns exist in the table before using them
2. **Quote reserved keywords**: If a table or column name is a SQL reserved keyword (e.g., "Order", "Group", "User"), you MUST wrap it in double quotes: "Order", "Group", "User"
3. **Use proper JOIN syntax** for multi-table queries - match foreign key relationships exactly
4. **Use appropriate WHERE clauses** to filter data
5. **Use GROUP BY and aggregate functions** when needed
6. **Use ORDER BY** for sorting when relevant
7. **Ensure query matches the information need description**
8. **SQLite compatibility**: 
   - Use SQLite-compatible date functions:
     * DATE('now', '-3 months') instead of DATE_TRUNC('quarter', ...)
     * DATE('now') for current date
     * datetime('now') for current timestamp
     * Use date arithmetic: DATE('now', '-1 day'), DATE('now', '-1 month')
   - SQLite does NOT support DATE_TRUNC, use DATE() with modifiers instead
   - For date ranges: WHERE date_column >= DATE('now', '-3 months')
9. **Column existence**: Before using a column, verify it exists in the table's column list from the schema
10. **Junction tables**: If you need to join through a junction table (e.g., Book_Category), use both foreign keys in the JOIN condition

{output_structure_section}

VALIDATION:
- Check that all table and column names exist in the schema
- Check that JOIN conditions are correct (match foreign keys)
- Check SQL syntax is valid

You have access to validation tools:
- validate_sql_syntax: Validate SQL syntax is correct
- validate_query_against_schema: Validate that tables and columns exist in the schema

Use these tools to validate your SQL query before finalizing your response.

Return a valid SQL query that answers the information need."""
    
    human_prompt = f"""Information Need:
Description: {information_need.get('description', '')}
Entities involved: {', '.join(information_need.get('entities_involved', []))}

Schema Summary:
{chr(10).join(schema_summary)}

Generate a SQL query that answers this information need. Ensure the query:
- Uses correct table and column names from the schema
- Properly joins tables using foreign key relationships
- Filters and aggregates data as needed
- Returns the information described in the need"""
    
    # Create bound version of validate_query_against_schema with normalized_schema
    def validate_query_against_schema_bound(sql: str) -> Dict[str, Any]:
        """Bound version of validate_query_against_schema with normalized_schema.
        
        Args:
            sql: SQL query string to validate. Must be a valid SQL query string.
                 Example: "SELECT * FROM Customer;"
        
        Returns:
            Dictionary with validation results
        
        IMPORTANT: When calling this tool, provide arguments as a JSON object:
        {"sql": "SELECT * FROM Customer;"}
        NOT as a list: ["sql"] (WRONG)
        """
        schema_state = {
            "entities": [
                {"name": table.get("name", "")} 
                for table in normalized_schema.get("normalized_tables", [])
            ],
            "attributes": {
                table.get("name", ""): [
                    {"name": col.get("name", "")} 
                    for col in table.get("columns", [])
                ]
                for table in normalized_schema.get("normalized_tables", [])
            }
        }
        # NOTE: validate_query_against_schema is a LangChain @tool (StructuredTool) and is not callable.
        return _validate_query_against_schema_impl(sql, schema_state)
    
    # Invoke standardized LLM call with SQL validation tools
    try:
        config = get_trace_config("6.5", phase=10, tags=["sql_generation"])
        result: SQLQueryGenerationOutput = await standardized_llm_call(
            llm=model,
            output_schema=SQLQueryGenerationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
            tools=[validate_sql_syntax, validate_query_against_schema_bound],
            use_agent_executor=True,  # Use agent executor for tool calls
            decouple_tools=True,  # Decouple tool calling from JSON generation
            config=config,
        )
        
        # Work with Pydantic model directly
        # Validate that sql is not None or empty
        if not result.sql or result.sql.strip() == "":
            error_msg = (
                "SQL query generation returned empty or null SQL. "
                "The 'sql' field must always contain a valid SQL query string, even if validation fails. "
                "Please generate a SQL query that answers the information need."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.debug(f"SQL query generated: {result.sql[:100]}...")
        
        return result
    except Exception as e:
        logger.error(f"SQL query generation failed: {e}")
        raise


async def step_6_5_sql_query_generation_batch(
    information_needs: List[Dict[str, Any]],  # List of information needs
    normalized_schema: Dict[str, Any],  # Full normalized schema
    data_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    Generate SQL queries for multiple information needs in parallel.
    
    Args:
        information_needs: List of information needs from Step 3.1
        normalized_schema: Full normalized schema from Phase 4
        data_types: Optional data types from Step 4.3
        relations: Optional list of relations from Phase 1
        
    Returns:
        List of SQL query generation results
    """
    import asyncio
    
    tasks = [
        step_6_5_sql_query_generation(
            information_need=info_need,
            normalized_schema=normalized_schema,
            data_types=data_types,
            related_entities=info_need.get("entities_involved", []),
            relations=relations,
        )
        for info_need in information_needs
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle exceptions
    output = []
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"SQL generation failed for information need {idx}: {result}")
            output.append({
                "sql": "",
                "validation_status": "error",
                "corrected_sql": None,
                "reasoning": f"Generation failed: {str(result)}"
            })
        else:
            output.append(result)
    
    return output

