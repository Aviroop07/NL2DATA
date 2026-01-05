"""Phase 5, Step 5.5: Nullability Detection.

Determine which columns can be nullable (after datatype assignment).
Excludes PKs and FKs that are already set as NOT NULL due to total participation.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase5.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class NullabilityOutput(BaseModel):
    """Output structure for nullability detection."""
    nullable_columns: List[str] = Field(
        description="List of column names that can be nullable (NULL allowed)"
    )
    not_nullable_columns: List[str] = Field(
        description="List of column names that must be NOT NULL"
    )
    reasoning: str = Field(
        description="Reasoning for nullability decisions"
    )

    model_config = ConfigDict(extra="forbid")


class TableNullabilityResult(BaseModel):
    """Nullability result for a single table."""
    table_name: str = Field(description="Name of the table")
    nullable_columns: List[str] = Field(description="List of nullable column names")
    not_nullable_columns: List[str] = Field(description="List of NOT NULL column names")
    reasoning: str = Field(description="Reasoning for nullability decisions")

    model_config = ConfigDict(extra="forbid")


class NullabilityBatchOutput(BaseModel):
    """Batch output structure for nullability detection."""
    table_results: List[TableNullabilityResult] = Field(
        description="List of nullability results for each table"
    )
    total_tables: int = Field(description="Total number of tables processed")

    model_config = ConfigDict(extra="forbid")


@traceable_step("5.5", phase=5, tags=['phase_5_step_5'])
async def step_5_5_nullability_detection(
    table_name: str,
    columns: List[Dict[str, Any]],  # List of columns with name, type, etc.
    primary_key: List[str],  # Primary key columns (already NOT NULL)
    foreign_keys: List[Dict[str, Any]],  # Foreign keys with nullable info from Phase 4
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> NullabilityOutput:
    """
    Step 5.5 (per-table, LLM): Determine nullability for columns.
    
    IMPORTANT:
    - Primary key columns are always NOT NULL (excluded from LLM decision)
    - Foreign keys that are already set as NOT NULL due to total participation are excluded
    - Only asks LLM about other columns
    
    Args:
        table_name: Name of the table
        columns: List of columns with name, type, nullable (current state), etc.
        primary_key: List of primary key column names
        foreign_keys: List of foreign key definitions (with nullable info from Phase 4)
        nl_description: Optional original NL description
        domain: Optional domain context
        
    Returns:
        dict: Nullability result with nullable_columns, not_nullable_columns, reasoning
        
    Example:
        >>> result = await step_5_5_nullability_detection("Customer", [...], ["customer_id"], [...])
        >>> "nullable_columns" in result
        True
    """
    logger.debug(f"Determining nullability for table: {table_name}")
    
    # Identify columns that are already NOT NULL (PKs and FKs with total participation)
    already_not_null = set(primary_key)
    
    # Check FKs that are already NOT NULL due to total participation
    for fk in foreign_keys:
        fk_attrs = fk.get("from_attributes", [])
        fk_table = fk.get("from_entity", "")
        if fk_table == table_name:
            # Check if this FK is already set as NOT NULL (total participation)
            for col in columns:
                col_name = col.get("name", "")
                if col_name in fk_attrs and col.get("nullable") is False:
                    already_not_null.add(col_name)
    
    # Filter columns that need LLM decision (exclude PKs and FKs already set as NOT NULL)
    columns_to_decide = [
        col for col in columns
        if col.get("name", "") not in already_not_null
    ]
    
    if not columns_to_decide:
        logger.debug(f"No columns need nullability decision for {table_name} (all are PKs or FKs with total participation)")
        return NullabilityOutput(
            nullable_columns=[],
            not_nullable_columns=list(already_not_null),
            reasoning="All columns are primary keys or foreign keys with total participation (already NOT NULL)",
        )
    
    # Build column summary
    column_summary = []
    for col in columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "UNKNOWN")
        col_desc = col.get("description", "")
        is_pk = col_name in primary_key
        is_fk = any(col_name in fk.get("from_attributes", []) for fk in foreign_keys if fk.get("from_entity") == table_name)
        current_nullable = col.get("nullable", True)
        
        status = []
        if is_pk:
            status.append("PRIMARY KEY (always NOT NULL)")
        if is_fk:
            if not current_nullable:
                status.append("FK with total participation (already NOT NULL)")
            else:
                status.append("FK (excluded from decision - handled by participation)")
        
        column_summary.append(f"- {col_name}: {col_type} {col_desc} {' '.join(status) if status else ''}")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=NullabilityOutput,
        additional_requirements=[
            "Primary key columns are ALWAYS NOT NULL (excluded from this decision)",
            "Foreign keys with total participation are already NOT NULL (excluded)",
            "Do NOT include primary keys or FKs already set as NOT NULL in nullable_columns",
        ]
    )
    
    system_prompt = f"""You are a database schema analyst. Determine which columns can be nullable.

NULLABILITY RULES:
- Primary key columns are ALWAYS NOT NULL (excluded from this decision)
- Foreign keys with total participation are already NOT NULL (excluded)
- Other columns can be nullable if:
  * The attribute is optional (e.g., middle_name, email for non-required fields)
  * The attribute can have missing values
  * The attribute is not required for entity identification
- Columns should be NOT NULL if:
  * The attribute is required for the entity to be meaningful
  * The attribute is a required business field
  * The attribute is part of a composite key (if not PK)
  
Return a list of columns that can be nullable and columns that must be NOT NULL.

{output_structure_section}"""
    
    human_prompt = f"""Table: {table_name}

Columns:
{chr(10).join(column_summary)}

Primary Key: {', '.join(primary_key) if primary_key else 'None'}

Natural Language Description:
{nl_description or 'Not provided'}

Domain: {domain or 'Not specified'}

Determine which columns (excluding primary keys and FKs already set as NOT NULL) can be nullable.
Remember: Primary keys are always NOT NULL and should not be included in your decision."""
    
    llm = get_model_for_step("5.5")
    trace_config = get_trace_config("5.5", phase=5, tags=["phase_5_step_5"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=NullabilityOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    # Validate that no PKs or already-NOT-NULL FKs are in nullable_columns
    invalid_nullable = [col for col in result.nullable_columns if col in already_not_null]
    if invalid_nullable:
        logger.warning(
            f"Step 5.5 for {table_name}: LLM marked PKs/FKs as nullable: {invalid_nullable}. Removing them."
        )
        result.nullable_columns = [col for col in result.nullable_columns if col not in already_not_null]
    
    # Ensure already_not_null columns are in not_nullable_columns
    result.not_nullable_columns = list(set(result.not_nullable_columns + list(already_not_null)))
    
    return result


async def step_5_5_nullability_detection_batch(
    relational_schema: Dict[str, Any],  # Relational schema from Phase 4
    primary_keys: Dict[str, List[str]],  # entity -> PK list
    foreign_keys: List[Dict[str, Any]],  # FK definitions
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> NullabilityBatchOutput:
    """
    Step 5.5: Determine nullability for all tables (parallel execution).
    
    Args:
        relational_schema: Relational schema from Phase 4
        primary_keys: Dictionary mapping table names to primary key lists
        foreign_keys: List of foreign key definitions
        nl_description: Original natural language description
        domain: Optional domain context
        
    Returns:
        dict: Batch nullability results with table_results
    """
    logger.info(f"Starting Step 5.5: Nullability Detection for {len(relational_schema.get('tables', []))} tables")
    
    import asyncio
    
    tables = relational_schema.get("tables", [])
    tasks = []
    
    for table in tables:
        table_name = table.get("name", "")
        columns = table.get("columns", [])
        pk = primary_keys.get(table_name, [])
        
        # Filter FKs for this table
        table_fks = [
            fk for fk in foreign_keys
            if fk.get("from_entity") == table_name
        ]
        
        tasks.append(
            step_5_5_nullability_detection(
                table_name=table_name,
                columns=columns,
                primary_key=pk,
                foreign_keys=table_fks,
                nl_description=nl_description,
                domain=domain,
            )
        )
    
    results = await asyncio.gather(*tasks)
    
    # Convert results to list of TableNullabilityResult
    table_results_list = []
    for result in results:
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
        else:
            result_dict = result
        table_results_list.append(TableNullabilityResult(
            table_name=result_dict.get("table_name", ""),
            nullable_columns=result_dict.get("nullable_columns", []),
            not_nullable_columns=result_dict.get("not_nullable_columns", []),
            reasoning=result_dict.get("reasoning", "")
        ))
    
    return NullabilityBatchOutput(
        table_results=table_results_list,
        total_tables=len(tables),
    )
