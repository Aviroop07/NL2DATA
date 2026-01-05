"""Phase 6: DDL Generation & Schema Creation Graph.

This phase handles DDL compilation, validation, error correction, schema creation, and SQL query generation.
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _wrap_step_6_1(step_func):
    """Wrap Step 6.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.1: DDL Compilation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        
        result = await invoke_step_checked(
            step_func,
            normalized_schema=metadata.get("relational_schema", {}),
            data_types=state.get("data_types", {}),
        )
        
        # Handle both Pydantic model and dict formats
        if hasattr(result, "ddl_statements"):
            ddl_statements = result.ddl_statements
        elif isinstance(result, dict):
            ddl_statements = result.get("ddl_statements", [])
        else:
            ddl_statements = []
        
        return {
            "current_step": "6.1",
            "previous_answers": {**prev_answers, "6.1": result},
            "metadata": {**metadata, "ddl_statements": ddl_statements},
        }
    return node


def _wrap_step_6_2(step_func):
    """Wrap Step 6.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.2: DDL Validation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        ddl_statements = metadata.get("ddl_statements", [])
        
        result = await invoke_step_checked(
            step_func,
            ddl_statements=ddl_statements,
        )
        
        return {
            "current_step": "6.2",
            "previous_answers": {**prev_answers, "6.2": result},
        }
    return node


def _wrap_step_6_3(step_func):
    """Wrap Step 6.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.3: DDL Error Correction")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        ddl_statements = metadata.get("ddl_statements", [])
        
        validation_result = prev_answers.get("6.2", {})
        result = await invoke_step_checked(
            step_func,
            validation_errors=validation_result if isinstance(validation_result, dict) else {},
            original_ddl=ddl_statements,
            normalized_schema=metadata.get("relational_schema", {}),
        )
        
        # Handle both Pydantic model and dict formats
        if hasattr(result, "corrected_ddl"):
            corrected_ddl = result.corrected_ddl
        elif isinstance(result, dict):
            corrected_ddl = result.get("corrected_ddl", ddl_statements)
        else:
            corrected_ddl = ddl_statements
        
        return {
            "current_step": "6.3",
            "previous_answers": {**prev_answers, "6.3": result},
            "metadata": {**metadata, "ddl_statements": corrected_ddl},
        }
    return node


def _wrap_step_6_4(step_func):
    """Wrap Step 6.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.4: Schema Creation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        ddl_statements = metadata.get("ddl_statements", [])
        
        result = await invoke_step_checked(
            step_func,
            ddl_statements=ddl_statements,
        )
        
        return {
            "current_step": "6.4",
            "previous_answers": {**prev_answers, "6.4": result},
        }
    return node


def _wrap_step_6_5(step_func):
    """Wrap Step 6.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.5: SQL Query Generation")
        prev_answers = state.get("previous_answers", {})
        information_needs = state.get("information_needs", [])
        metadata = state.get("metadata", {})
        relational_schema = metadata.get("relational_schema", {})
        
        # Process each information need
        results = []
        for info_need in information_needs:
            result = await invoke_step_checked(
                step_func,
                information_need=info_need,
                normalized_schema=relational_schema,
                data_types=state.get("data_types", {}),
            )
            results.append(result)
        
        return {
            "current_step": "6.5",
            "previous_answers": {**prev_answers, "6.5": results},
        }
    return node


def create_phase_6_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 6 (DDL Generation & Schema Creation)."""
    from NL2DATA.phases.phase6 import (
        step_6_1_ddl_compilation,
        step_6_2_ddl_validation,
        step_6_3_ddl_error_correction,
        step_6_4_schema_creation,
        step_6_5_sql_query_generation,
    )
    
    workflow = StateGraph(IRGenerationState)
    workflow.add_node("ddl_compilation", _wrap_step_6_1(step_6_1_ddl_compilation))
    workflow.add_node("ddl_validation", _wrap_step_6_2(step_6_2_ddl_validation))
    workflow.add_node("ddl_error_correction", _wrap_step_6_3(step_6_3_ddl_error_correction))
    workflow.add_node("schema_creation", _wrap_step_6_4(step_6_4_schema_creation))
    workflow.add_node("sql_generation", _wrap_step_6_5(step_6_5_sql_query_generation))
    
    workflow.set_entry_point("ddl_compilation")
    workflow.add_edge("ddl_compilation", "ddl_validation")
    workflow.add_edge("ddl_validation", "ddl_error_correction")
    workflow.add_edge("ddl_error_correction", "schema_creation")
    workflow.add_edge("schema_creation", "sql_generation")
    workflow.add_edge("sql_generation", END)
    
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
