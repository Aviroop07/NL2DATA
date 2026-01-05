"""Phase 6: DDL Generation & Schema Creation Graph.

This phase handles DDL compilation, validation, and schema creation.
All steps are deterministic - no LLM interaction required.
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
        logger.info("[LangGraph] Executing Step 6.3: Schema Creation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        ddl_statements = metadata.get("ddl_statements", [])
        
        # Generate database path if not already set
        database_path = metadata.get("database_path")
        if not database_path:
            # Generate a database path in the run directory if available
            import os
            from pathlib import Path
            run_dir = os.environ.get("NL2DATA_RUN_DIR")
            if run_dir:
                database_path = str(Path(run_dir) / "schema.db")
                # Ensure directory exists
                Path(run_dir).mkdir(parents=True, exist_ok=True)
            else:
                # Fallback: use a temp file
                import tempfile
                temp_dir = tempfile.gettempdir()
                database_path = str(Path(temp_dir) / "nl2data_schema.db")
        
        result = await invoke_step_checked(
            step_func,
            ddl_statements=ddl_statements,
            database_path=database_path,
        )
        
        # Store database path in metadata for later access
        updated_metadata = {**metadata, "database_path": database_path}
        
        return {
            "current_step": "6.3",
            "previous_answers": {**prev_answers, "6.3": result},
            "metadata": updated_metadata,
        }
    return node


def create_phase_6_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 6 (DDL Generation & Schema Creation)."""
    from NL2DATA.phases.phase6 import (
        step_6_1_ddl_compilation,
        step_6_2_ddl_validation,
        step_6_3_schema_creation,
    )
    
    workflow = StateGraph(IRGenerationState)
    workflow.add_node("ddl_compilation", _wrap_step_6_1(step_6_1_ddl_compilation))
    workflow.add_node("ddl_validation", _wrap_step_6_2(step_6_2_ddl_validation))
    workflow.add_node("schema_creation", _wrap_step_6_3(step_6_3_schema_creation))
    
    workflow.set_entry_point("ddl_compilation")
    workflow.add_edge("ddl_compilation", "ddl_validation")
    workflow.add_edge("ddl_validation", "schema_creation")
    workflow.add_edge("schema_creation", END)
    
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
