"""Phase 5: DDL & SQL Generation Graph."""

from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _ddl_validation_passed(state: IRGenerationState) -> Literal["passed", "failed"]:
    """Check if DDL validation passed."""
    metadata = state.get("metadata", {})
    validation_passed = metadata.get("ddl_validation_passed", False)
    
    if validation_passed:
        return "passed"
    return "failed"


def _wrap_step_5_1(step_func):
    """Wrap Step 5.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.1: DDL Compilation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            normalized_schema=metadata.get("normalized_schema", {}),
            data_types=state.get("data_types", {})
        )
        
        # Update DDL statements
        ddl_statements = result.get("ddl_statements", [])
        
        return {
            "ddl_statements": ddl_statements,
            "current_step": "5.1",
            "previous_answers": {**prev_answers, "5.1": result}
        }
    return node


def _wrap_step_5_2(step_func):
    """Wrap Step 5.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.2: DDL Validation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            ddl_statements=state.get("ddl_statements", [])
        )
        
        return {
            "current_step": "5.2",
            "previous_answers": {**prev_answers, "5.2": result},
            "metadata": {
                **state.get("metadata", {}),
                "ddl_validation_passed": result.get("validation_passed", False),
                "ddl_validation_errors": result.get("syntax_errors", [])
            }
        }
    return node


def _wrap_step_5_3(step_func):
    """Wrap Step 5.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.3: DDL Error Correction")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            validation_errors=metadata.get("ddl_validation_errors", []),
            ddl_statements=state.get("ddl_statements", []),
            normalized_schema=metadata.get("normalized_schema", {})
        )
        
        # Update normalized schema with patches
        if "ir_patches" in result:
            # Apply patches to normalized schema (simplified - actual implementation would apply patches)
            metadata = state.get("metadata", {}).copy()
            metadata["normalized_schema"] = result.get("patched_schema", metadata.get("normalized_schema", {}))
            
            return {
                "metadata": metadata,
                "current_step": "5.3",
                "previous_answers": {**prev_answers, "5.3": result}
            }
        
        return {
            "current_step": "5.3",
            "previous_answers": {**prev_answers, "5.3": result}
        }
    return node


def _wrap_step_5_4(step_func):
    """Wrap Step 5.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.4: Schema Creation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            ddl_statements=state.get("ddl_statements", [])
        )
        
        return {
            "current_step": "5.4",
            "previous_answers": {**prev_answers, "5.4": result},
            "metadata": {
                **state.get("metadata", {}),
                "schema_created": result.get("success", False),
                "tables_created": result.get("tables_created", [])
            }
        }
    return node


def _wrap_step_5_5(step_func):
    """Wrap Step 5.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.5: SQL Query Generation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            information_needs=state.get("information_needs", []),
            relational_schema=metadata.get("relational_schema", {}),
            normalized_schema=metadata.get("normalized_schema", {})
        )
        
        # Update SQL queries
        sql_queries = result.get("query_results", [])
        
        return {
            "sql_queries": sql_queries,
            "current_step": "5.5",
            "previous_answers": {**prev_answers, "5.5": result}
        }
    return node


def create_phase_5_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 5 (DDL & SQL Generation).
    
    This graph orchestrates all Phase 5 steps:
    1. DDL Compilation (5.1) - deterministic
    2. DDL Validation (5.2) - deterministic, conditional loop to 5.3 if fails
    3. DDL Error Correction (5.3) - conditional, loops back to 5.1
    4. Schema Creation (5.4) - deterministic
    5. SQL Query Generation (5.5) - parallel per information need
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase5 import (
        step_5_1_ddl_compilation,
        step_5_2_ddl_validation,
        step_5_3_ddl_error_correction,
        step_5_4_schema_creation,
        step_5_5_sql_query_generation_batch,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("ddl_compilation", _wrap_step_5_1(step_5_1_ddl_compilation))
    workflow.add_node("ddl_validation", _wrap_step_5_2(step_5_2_ddl_validation))
    workflow.add_node("ddl_error_correction", _wrap_step_5_3(step_5_3_ddl_error_correction))
    workflow.add_node("schema_creation", _wrap_step_5_4(step_5_4_schema_creation))
    workflow.add_node("sql_query_generation", _wrap_step_5_5(step_5_5_sql_query_generation_batch))
    
    # Set entry point
    workflow.set_entry_point("ddl_compilation")
    
    # Add edges
    workflow.add_edge("ddl_compilation", "ddl_validation")
    
    # Conditional: Loop back if validation fails
    workflow.add_conditional_edges(
        "ddl_validation",
        _ddl_validation_passed,
        {
            "passed": "schema_creation",
            "failed": "ddl_error_correction"
        }
    )
    
    # Error correction loops back to compilation
    workflow.add_edge("ddl_error_correction", "ddl_compilation")
    
    workflow.add_edge("schema_creation", "sql_query_generation")
    workflow.add_edge("sql_query_generation", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

