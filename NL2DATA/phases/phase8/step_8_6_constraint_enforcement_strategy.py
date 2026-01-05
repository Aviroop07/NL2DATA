"""Phase 8, Step 8.6: Constraint Enforcement Strategy.

Determine how to enforce each constraint and generate column-wise DSL expressions.
Processes each constraint separately for granular control.
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


class ConstraintWithEnforcement(BaseModel):
    """Constraint with enforcement strategy and column-wise DSL."""
    constraint_data: Dict[str, Any] = Field(
        description="Original constraint data"
    )
    enforcement_strategy: str = Field(
        description="Strategy for enforcement: 'database', 'application', or 'hybrid'"
    )
    enforcement_level: str = Field(
        description="Level of enforcement: 'strict', 'warning', or 'soft'"
    )
    column_dsl_expressions: Dict[str, str] = Field(
        description="Dictionary mapping 'table.column' -> DSL expression string for that column"
    )
    reasoning: str = Field(
        description="REQUIRED - Explanation of why this enforcement approach was chosen, considering FD relationships"
    )

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


def _format_affected_tables_schema(
    constraint: Dict[str, Any],
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Format schema information for affected tables only, with relevant FDs."""
    # Get affected tables and columns from constraint scope
    affected_tables = constraint.get("affected_tables", [])
    affected_attributes = constraint.get("affected_attributes", [])
    
    # Extract table names from affected_attributes
    table_set = set(affected_tables)
    for attr in affected_attributes:
        if isinstance(attr, str) and "." in attr:
            table_name = attr.split(".")[0]
            table_set.add(table_name)
    
    if not table_set:
        # Fallback: use constraint's table
        table_name = constraint.get("table", "")
        if table_name:
            table_set.add(table_name)
    
    # Filter FDs to only those relevant to affected tables
    relevant_fds = []
    if functional_dependencies:
        for fd in functional_dependencies:
            if isinstance(fd, dict):
                lhs = fd.get("lhs", [])
                rhs = fd.get("rhs", [])
            elif hasattr(fd, 'lhs') and hasattr(fd, 'rhs'):
                lhs = fd.lhs if isinstance(fd.lhs, list) else [fd.lhs]
                rhs = fd.rhs if isinstance(fd.rhs, list) else [fd.rhs]
            else:
                continue
            
            # Check if FD involves columns from affected tables
            # For simplicity, check if any column name appears in affected attributes
            fd_involves_affected = False
            for col_list in [lhs, rhs]:
                for col in col_list:
                    if isinstance(col, str):
                        # Check if this column is in affected attributes
                        for attr in affected_attributes:
                            if isinstance(attr, str) and col in attr:
                                fd_involves_affected = True
                                break
                        if fd_involves_affected:
                            break
                if fd_involves_affected:
                    break
            
            if fd_involves_affected:
                relevant_fds.append(fd)
    
    # Format schema for affected tables
    schema_parts = []
    tables = normalized_schema.get("tables", []) or normalized_schema.get("normalized_tables", [])
    
    for table in tables:
        table_name = table.get("name", "")
        if table_name not in table_set:
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
    
    # Relevant Functional Dependencies
    if relevant_fds:
        schema_parts.append("\nRelevant Functional Dependencies:")
        for fd in relevant_fds:
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


@traceable_step("8.6", phase=8, tags=['phase_8_step_6'])
async def step_8_6_constraint_enforcement_strategy_single(
    constraint: Dict[str, Any],
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> ConstraintWithEnforcement:
    """
    Step 8.6 (single constraint): Determine enforcement strategy for one constraint.
    
    Args:
        constraint: Constraint dictionary with scope information (from step 8.5)
        normalized_schema: Relational schema
        functional_dependencies: Optional list of functional dependencies (filtered to relevant ones)
        
    Returns:
        ConstraintWithEnforcement with strategy, level, and column-wise DSL
    """
    # Format constraint for LLM
    constraint_type = constraint.get("constraint_type", constraint.get("constraint_category", "unknown"))
    table = constraint.get("table", "")
    column = constraint.get("column", "")
    description = constraint.get("description", "")
    affected_tables = constraint.get("affected_tables", [])
    affected_attributes = constraint.get("affected_attributes", [])
    
    constraint_text = f"""Constraint Type: {constraint_type}
Table: {table}
Column: {column if column else "N/A (table-level constraint)"}
Description: {description}
Affected Tables: {', '.join(affected_tables) if affected_tables else 'N/A'}
Affected Attributes: {', '.join(affected_attributes) if affected_attributes else 'N/A'}"""
    
    # Format schema for affected tables only
    schema_text = _format_affected_tables_schema(constraint, normalized_schema, functional_dependencies)
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=ConstraintWithEnforcement,
        additional_requirements=[
            "The 'column_dsl_expressions' field must contain a DSL expression for EACH affected column (format: 'table.column' -> DSL string).",
            "DSL expressions should be executable/interpretable, not natural language.",
            "Enforcement strategy: 'database' (CHECK constraints, triggers), 'application' (validation code), or 'hybrid'.",
            "Enforcement level: 'strict' (reject invalid), 'warning' (log but allow), or 'soft' (suggest only).",
            "Consider functional dependencies when generating DSL - if A -> B and constraint affects A, DSL for B may need to account for this.",
            "The 'reasoning' field is REQUIRED and must explain the enforcement approach.",
        ]
    )
    
    # Build prompt
    system_prompt = f"""You are a database constraint enforcement strategist. Your task is to determine how to enforce constraints and generate column-wise DSL expressions.

CRITICAL REQUIREMENTS:
1. Generate a DSL expression for EACH affected column (use 'table.column' format as key).
2. DSL expressions must be executable/interpretable, not natural language.
3. Consider functional dependencies when generating DSL.
4. Choose appropriate enforcement strategy and level based on constraint type and criticality.
5. Provide clear reasoning for your choices.

{output_structure_section}"""

    human_prompt = f"""Determine enforcement strategy and generate column-wise DSL for the following constraint:

{constraint_text}

Schema (Affected Tables Only):
{schema_text}

Generate:
1. Enforcement strategy (database/application/hybrid)
2. Enforcement level (strict/warning/soft)
3. Column-wise DSL expressions for each affected column
4. Reasoning for your choices"""

    # Get model
    llm = get_model_for_step("8.6")
    
    # Make LLM call
    trace_config = get_trace_config("8.6", phase=8, tags=["phase_8_step_6"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=ConstraintWithEnforcement,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    return result


@traceable_step("8.6", phase=8, tags=['phase_8_step_6'])
async def step_8_6_constraint_enforcement_strategy_batch(
    constraints_with_scope: List[Dict[str, Any]],
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> List[ConstraintWithEnforcement]:
    """
    Step 8.6 (per-constraint, LLM): Determine enforcement strategy for each constraint.
    
    Processes each constraint separately (not batch) for granular control.
    
    Args:
        constraints_with_scope: List of constraints with scope analysis (from step 8.5)
        normalized_schema: Relational schema
        functional_dependencies: Optional list of functional dependencies from step 8.1
        
    Returns:
        list: List of constraints with enforcement strategies and column-wise DSL
    """
    if not constraints_with_scope:
        logger.warning("No constraints provided to step_8_6_constraint_enforcement_strategy_batch")
        return []
    
    # Process constraints in parallel (but each is a separate LLM call)
    tasks = [
        step_8_6_constraint_enforcement_strategy_single(
            constraint=constraint,
            normalized_schema=normalized_schema,
            functional_dependencies=functional_dependencies,
        )
        for constraint in constraints_with_scope
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle exceptions
    enforcement_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error determining enforcement for constraint {i}: {result}", exc_info=result)
            # Return default enforcement on error
            constraint = constraints_with_scope[i]
            enforcement_results.append(
                ConstraintWithEnforcement(
                    constraint_data=constraint,
                    enforcement_strategy="unknown",
                    enforcement_level="unknown",
                    column_dsl_expressions={},
                    reasoning=f"Error during enforcement strategy determination: {str(result)}",
                )
            )
        else:
            enforcement_results.append(result)
    
    logger.info(f"Completed enforcement strategy determination for {len(enforcement_results)} constraints")
    return enforcement_results
