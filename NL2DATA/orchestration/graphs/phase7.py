"""Phase 7: Information Mining Graph.

This phase handles information need identification and SQL validation.
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def create_phase_7_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 7 (Information Mining).
    
    IMPORTANT: No schema modification - only identifies information needs and validates SQL queries.
    """
    from NL2DATA.phases.phase7 import (
        step_7_1_information_need_identification_with_loop,
        step_7_2_sql_generation_and_validation_batch,
    )
    
    workflow = StateGraph(IRGenerationState)
    
    async def information_needs(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.1: Information Need Identification")
        metadata = state.get("metadata", {})
        relational_schema = metadata.get("relational_schema", {})
        
        result = await invoke_step_checked(
            step_7_1_information_need_identification_with_loop,
            nl_description=state.get("nl_description", ""),
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            domain=state.get("domain"),
            max_iterations=10,
            max_time_sec=300,
        )
        # Handle both Pydantic model and dict formats
        if hasattr(result, 'final_result'):
            final_result = result.final_result
            if hasattr(final_result, 'information_needs'):
                information_needs_list = final_result.information_needs
            elif isinstance(final_result, dict):
                information_needs_list = final_result.get("information_needs", [])
            else:
                information_needs_list = []
        elif isinstance(result, dict):
            final_result = result.get("final_result", {})
            information_needs_list = final_result.get("information_needs", []) if isinstance(final_result, dict) else []
        else:
            information_needs_list = []
        return {
            **state,
            "information_needs": information_needs_list,
            "previous_answers": {**state.get("previous_answers", {}), "7.1": result},
        }
    
    async def sql_validation(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.2: SQL Generation and Validation")
        information_needs = state.get("information_needs", [])
        metadata = state.get("metadata", {})
        relational_schema = metadata.get("relational_schema", {})
        nl_description = state.get("nl_description", "")
        domain = state.get("domain")
        
        if not information_needs:
            logger.info("No information needs found, skipping SQL validation")
            return {**state, "validated_information_needs": [], "previous_answers": {**state.get("previous_answers", {}), "7.2": {}}}
        
        result = await invoke_step_checked(
            step_7_2_sql_generation_and_validation_batch,
            information_needs=information_needs,
            relational_schema=relational_schema,
            nl_description=nl_description,
            domain=domain,
            max_retries=5,
        )
        
        # Handle both Pydantic model and dict formats
        if hasattr(result, 'validated_information_needs'):
            validated_needs = result.validated_information_needs
        elif isinstance(result, dict):
            validated_needs = result.get("validated_information_needs", [])
        else:
            validated_needs = []
        return {
            **state,
            "validated_information_needs": validated_needs,
            "previous_answers": {**state.get("previous_answers", {}), "7.2": result},
        }
    
    workflow.add_node("information_needs", information_needs)
    workflow.add_node("sql_validation", sql_validation)
    
    workflow.set_entry_point("information_needs")
    workflow.add_edge("information_needs", "sql_validation")
    workflow.add_edge("sql_validation", END)
    
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
