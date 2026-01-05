"""Phase 4: Relational Schema Compilation Graph.

Phase 4:
- Step 4.1: Relational Schema Compilation

This phase compiles the ER design into a canonical relational schema.
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _wrap_step_4_1(step_func):
    """Wrap Step 4.1 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.1: Relational Schema Compilation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})

        result = await invoke_step_checked(
            step_func,
            er_design=metadata.get("er_design", {}),
            foreign_keys=state.get("foreign_keys", []),
            primary_keys=state.get("primary_keys", {}),
            constraints=state.get("constraints", []),
            junction_table_names=metadata.get("junction_table_names", {}),
        )

        # Handle Pydantic model
        if hasattr(result, 'relational_schema'):
            relational_schema = result.relational_schema.model_dump() if hasattr(result.relational_schema, 'model_dump') else result.relational_schema
            result_dict = result.model_dump()
        elif isinstance(result, dict):
            relational_schema = result.get("relational_schema", {})
            result_dict = result
        else:
            # Fallback: try to access as attribute if it's a Pydantic model
            relational_schema = getattr(result, 'relational_schema', {})
            if hasattr(result, 'model_dump'):
                result_dict = result.model_dump()
            else:
                result_dict = {}
            relational_schema = {}
            result_dict = result

        return {
            "current_step": "4.1",
            "previous_answers": {**prev_answers, "4.1": result_dict},
            "metadata": {
                **metadata,
                "relational_schema": relational_schema,
                "frozen_schema": relational_schema,
            },
        }

    return node


def create_phase_4_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 4 (Relational Schema Compilation)."""

    from NL2DATA.phases.phase4 import (
        step_4_1_relational_schema_compilation,
    )

    workflow = StateGraph(IRGenerationState)
    workflow.add_node("relational_schema", _wrap_step_4_1(step_4_1_relational_schema_compilation))

    workflow.set_entry_point("relational_schema")
    workflow.add_edge("relational_schema", END)

    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
