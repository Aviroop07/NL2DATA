"""Phase 8, Step 8.5: Constraint Scope Analysis.

Analyze the scope of each constraint (which tables/columns it affects).
Uses functional dependencies to understand constraint propagation.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase8.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema
import asyncio

logger = get_logger(__name__)


class ConstraintScopeAnalysis(BaseModel):
    """Scope analysis for a single constraint."""
    affected_attributes: List[str] = Field(
        default_factory=list,
        description="List of affected attribute names (format: 'table.column')"
    )
    affected_tables: List[str] = Field(
        default_factory=list,
        description="List of affected table names"
    )
    scope_type: str = Field(
        description="Type of scope: 'column', 'table', 'cross_table', etc."
    )
    reasoning: str = Field(
        description="REQUIRED - Explanation of why these tables/columns are affected, including FD-based propagation"
    )

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


def _format_schema_for_llm(
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Format relational schema with FDs for LLM context."""
    schema_parts = []
    
    tables = normalized_schema.get("tables", []) or normalized_schema.get("normalized_tables", [])
    
    for table in tables:
        table_name = table.get("name", "")
        if not table_name:
            continue
        
        schema_parts.append(f"\nTable: {table_name}")
        
        # Columns
        columns = table.get("columns", [])
        if columns:
            schema_parts.append("  Columns:")
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("type", col.get("datatype", "UNKNOWN"))
                col_desc = col.get("description", "")
                is_pk = col.get("is_primary_key", False)
                is_fk = col.get("is_foreign_key", False)
                
                col_info = f"    - {col_name}: {col_type}"
                if is_pk:
                    col_info += " (PRIMARY KEY)"
                if is_fk:
                    col_info += " (FOREIGN KEY)"
                if col_desc:
                    col_info += f" - {col_desc}"
                schema_parts.append(col_info)
        
        # Primary Key
        pk = table.get("primary_key", [])
        if pk:
            schema_parts.append(f"  Primary Key: {', '.join(pk)}")
    
    # Functional Dependencies
    if functional_dependencies:
        schema_parts.append("\nFunctional Dependencies:")
        for fd in functional_dependencies:
            if isinstance(fd, dict):
                lhs = fd.get("lhs", [])
                rhs = fd.get("rhs", [])
                reasoning = fd.get("reasoning", "")
                if lhs and rhs:
                    fd_str = f"  {', '.join(lhs)} -> {', '.join(rhs)}"
                    if reasoning:
                        fd_str += f" ({reasoning})"
                    schema_parts.append(fd_str)
            elif hasattr(fd, 'lhs') and hasattr(fd, 'rhs'):
                lhs = fd.lhs if isinstance(fd.lhs, list) else [fd.lhs]
                rhs = fd.rhs if isinstance(fd.rhs, list) else [fd.rhs]
                reasoning = getattr(fd, 'reasoning', '')
                if lhs and rhs:
                    fd_str = f"  {', '.join(lhs)} -> {', '.join(rhs)}"
                    if reasoning:
                        fd_str += f" ({reasoning})"
                    schema_parts.append(fd_str)
    
    return "\n".join(schema_parts)


@traceable_step("8.5", phase=8, tags=['phase_8_step_5'])
async def step_8_5_constraint_scope_analysis_single(
    constraint: Dict[str, Any],
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> ConstraintScopeAnalysis:
    """
    Step 8.5 (single constraint): Analyze scope for one constraint.
    
    Args:
        constraint: Constraint dictionary with description, type, table, column
        normalized_schema: Relational schema
        functional_dependencies: Optional list of functional dependencies
        
    Returns:
        ConstraintScopeAnalysis with affected tables/columns and reasoning
    """
    # Format constraint for LLM
    constraint_type = constraint.get("constraint_type", constraint.get("constraint_category", "unknown"))
    table = constraint.get("table", "")
    column = constraint.get("column", "")
    description = constraint.get("description", "")
    
    constraint_text = f"""Constraint Type: {constraint_type}
Table: {table}
Column: {column if column else "N/A (table-level constraint)"}
Description: {description}"""
    
    # Format schema
    schema_text = _format_schema_for_llm(normalized_schema, functional_dependencies)
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=ConstraintScopeAnalysis,
        additional_requirements=[
            "The 'reasoning' field is REQUIRED and must explain why each table/column is affected.",
            "Consider functional dependencies when determining scope - if A -> B and constraint affects A, it may affect B.",
            "Use 'table.column' format for affected_attributes.",
            "Scope type should be: 'column' (single column), 'table' (entire table), or 'cross_table' (multiple tables).",
        ]
    )
    
    # Build prompt
    system_prompt = f"""You are a database constraint scope analyst. Your task is to determine which tables and columns are affected by a constraint.

CRITICAL REQUIREMENTS:
1. Analyze the constraint and determine its scope (which tables/columns it affects).
2. Consider functional dependencies: if constraint affects column A and A -> B (FD), then B may also be affected.
3. Provide clear reasoning for why each table/column is affected.
4. Do NOT analyze foreign key relationships - those are already handled.
5. Use 'table.column' format for affected_attributes.

{output_structure_section}"""

    human_prompt = f"""Analyze the scope of the following constraint:

{constraint_text}

Schema:
{schema_text}

Determine:
1. Which tables are affected by this constraint
2. Which columns in those tables are affected (use 'table.column' format)
3. The scope type (column/table/cross_table)
4. Clear reasoning explaining why each table/column is affected, including FD-based propagation if applicable"""

    # Get model
    llm = get_model_for_step("8.5")
    
    # Make LLM call
    trace_config = get_trace_config("8.5", phase=8, tags=["phase_8_step_5"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=ConstraintScopeAnalysis,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    return result


@traceable_step("8.5", phase=8, tags=['phase_8_step_5'])
async def step_8_5_constraint_scope_analysis_batch(
    constraints: List[Dict[str, Any]],
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> List[ConstraintScopeAnalysis]:
    """
    Step 8.5 (batch, LLM): Analyze scope for each constraint.
    
    Args:
        constraints: List of constraint dictionaries
        normalized_schema: Relational schema
        functional_dependencies: Optional list of functional dependencies from step 8.1
        
    Returns:
        list: List of scope analysis results, one per constraint
    """
    if not constraints:
        logger.warning("No constraints provided to step_8_5_constraint_scope_analysis_batch")
        return []
    
    # Process constraints in parallel (batch)
    tasks = [
        step_8_5_constraint_scope_analysis_single(
            constraint=constraint,
            normalized_schema=normalized_schema,
            functional_dependencies=functional_dependencies,
        )
        for constraint in constraints
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle exceptions
    scope_analyses = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error analyzing scope for constraint {i}: {result}", exc_info=result)
            # Return default scope analysis on error
            constraint = constraints[i]
            scope_analyses.append(
                ConstraintScopeAnalysis(
                    affected_attributes=[],
                    affected_tables=[],
                    scope_type="unknown",
                    reasoning=f"Error during scope analysis: {str(result)}",
                )
            )
        else:
            scope_analyses.append(result)
    
    logger.info(f"Completed scope analysis for {len(scope_analyses)} constraints")
    return scope_analyses
