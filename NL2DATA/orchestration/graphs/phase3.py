"""Phase 3: ER Design Compilation Graph.

Phase 3:
- Step 3.1: ER Design Compilation
- Step 3.2: Junction Table Naming

This phase compiles entities, relations, and attributes into an ER design.
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _wrap_step_3_1(step_func):
    """Wrap Step 3.1 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.1: ER Design Compilation")
        prev_answers = state.get("previous_answers", {})

        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            # Constraints are defined later in the plan; keep empty here.
            constraints=state.get("constraints", []),
        )

        # Handle Pydantic model
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
            # Convert entity_attributes list back to dict format for backward compatibility
            if "entity_attributes" in result_dict:
                attributes_dict = {entry["entity_name"]: entry["attributes"] for entry in result_dict["entity_attributes"]}
                result_dict["attributes"] = attributes_dict
        else:
            result_dict = result

        return {
            "current_step": "3.1",
            "previous_answers": {**prev_answers, "3.1": result_dict},
            "metadata": {**state.get("metadata", {}), "er_design": result_dict},
        }

    return node


def _wrap_step_3_2(step_func):
    """Wrap Step 3.2 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.2: Junction Table Naming")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})

        result = await invoke_step_checked(
            step_func,
            relations=state.get("relations", []),
            entities=state.get("entities", []),
            nl_description=state.get("nl_description", ""),
            domain=state.get("domain"),
        )

        # Handle Pydantic model
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
            # Convert junction_table_names list back to dict format for backward compatibility with step 4.1
            if "junction_table_names" in result_dict:
                junction_dict = {entry["relation_key"]: entry["table_name"] for entry in result_dict["junction_table_names"]}
                result_dict["junction_table_names_dict"] = junction_dict
        else:
            result_dict = result
            # If it's already a dict, ensure it has the right format
            if isinstance(result_dict, dict) and "junction_table_names" not in result_dict:
                # It's already in dict format (old format)
                junction_dict = result_dict
            else:
                junction_dict = result_dict.get("junction_table_names_dict", {})

        return {
            "current_step": "3.2",
            "previous_answers": {**prev_answers, "3.2": result_dict},
            "metadata": {**metadata, "junction_table_names": junction_dict},
        }

    return node


def create_phase_3_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 3 (ER Design Compilation)."""

    from NL2DATA.phases.phase3 import (
        step_3_1_er_design_compilation,
        step_3_2_junction_table_naming,
    )

    workflow = StateGraph(IRGenerationState)
    workflow.add_node("er_design", _wrap_step_3_1(step_3_1_er_design_compilation))
    workflow.add_node("junction_naming", _wrap_step_3_2(step_3_2_junction_table_naming))

    workflow.set_entry_point("er_design")
    workflow.add_edge("er_design", "junction_naming")
    workflow.add_edge("junction_naming", END)

    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

