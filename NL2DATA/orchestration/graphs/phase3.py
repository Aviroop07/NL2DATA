"""Phase 3: Query Requirements & Schema Refinement Graph."""

from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _wrap_step_3_1(step_func):
    """Wrap Step 3.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.1: Information Need Identification")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            nl_description=state["nl_description"],
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            domain=state.get("domain")
        )
        
        # Update information needs
        information_needs = result.get("final_result", {}).get("information_needs", [])
        
        return {
            "information_needs": information_needs,
            "current_step": "3.1",
            "previous_answers": {**prev_answers, "3.1": result}
        }
    return node


def _wrap_step_3_2(step_func):
    """Wrap Step 3.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.2: Information Completeness Check")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            information_needs=state.get("information_needs", []),
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            constraints=state.get("constraints"),
            nl_description=state["nl_description"],
            domain=state.get("domain")
        )
        
        return {
            "current_step": "3.2",
            "previous_answers": {**prev_answers, "3.2": result},
            "metadata": {
                **state.get("metadata", {}),
                "completeness_results": result.get("completeness_results", {})
            }
        }
    return node


def _wrap_step_3_3(step_func):
    """Wrap Step 3.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.3: Phase 2 Re-execution")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            information_needs=state.get("information_needs", []),
            completeness_results=metadata.get("completeness_results", {}),
            nl_description=state["nl_description"],
            domain=state.get("domain")
        )
        
        # Update attributes if new ones were added
        if "new_attributes" in result:
            updated_attributes = state.get("attributes", {}).copy()
            for entity_name, new_attrs in result["new_attributes"].items():
                if entity_name not in updated_attributes:
                    updated_attributes[entity_name] = []
                updated_attributes[entity_name].extend(new_attrs)
            
            return {
                "attributes": updated_attributes,
                "current_step": "3.3",
                "previous_answers": {**prev_answers, "3.3": result}
            }
        
        return {
            "current_step": "3.3",
            "previous_answers": {**prev_answers, "3.3": result}
        }
    return node


def _wrap_step_3_4(step_func):
    """Wrap Step 3.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.4: ER Design Compilation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            constraints=state.get("constraints", [])
        )
        
        return {
            "current_step": "3.4",
            "previous_answers": {**prev_answers, "3.4": result},
            "metadata": {
                **state.get("metadata", {}),
                "er_design": result
            }
        }
    return node


def _wrap_step_3_45(step_func):
    """Wrap Step 3.45 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.45: Junction Table Naming")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        er_design = metadata.get("er_design", {})
        
        # Extract entities and relations from ER design
        entities = er_design.get("entities", [])
        relations = er_design.get("relations", [])
        
        result = await step_func(
            relations=relations,
            entities=entities,
            nl_description=state.get("nl_description"),
            domain=state.get("domain"),
        )
        
        return {
            "current_step": "3.45",
            "previous_answers": {**prev_answers, "3.45": result},
            "junction_table_names": result,
        }
    return node


def _wrap_step_3_5(step_func):
    """Wrap Step 3.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 3.5: Relational Schema Compilation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            er_design=metadata.get("er_design", {}),
            foreign_keys=state.get("foreign_keys", []),
            primary_keys=state.get("primary_keys", {}),
            constraints=state.get("constraints"),
            junction_table_names=state.get("junction_table_names", {}),
        )
        
        return {
            "current_step": "3.5",
            "previous_answers": {**prev_answers, "3.5": result},
            "metadata": {
                **state.get("metadata", {}),
                "relational_schema": result
            }
        }
    return node


def create_phase_3_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 3 (Query Requirements & Schema Refinement).
    
    This graph orchestrates all Phase 3 steps:
    1. Information Need Identification (3.1) - loop until no_more_changes
    2. Information Completeness Check (3.2) - parallel per information need (loop)
    3. Phase 2 Re-execution (3.3) - re-run Phase 2 steps with enhanced context
    4. ER Design Compilation (3.4) - deterministic
    5. Relational Schema Compilation (3.5) - deterministic
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase3 import (
        step_3_1_information_need_identification_with_loop,
        step_3_2_information_completeness_batch,
        step_3_3_phase2_reexecution,
        step_3_4_er_design_compilation,
        step_3_45_junction_table_naming,
        step_3_5_relational_schema_compilation,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("information_needs", _wrap_step_3_1(step_3_1_information_need_identification_with_loop))
    workflow.add_node("completeness_check", _wrap_step_3_2(step_3_2_information_completeness_batch))
    workflow.add_node("phase2_reexecution", _wrap_step_3_3(step_3_3_phase2_reexecution))
    workflow.add_node("er_compilation", _wrap_step_3_4(step_3_4_er_design_compilation))
    workflow.add_node("junction_naming", _wrap_step_3_45(step_3_45_junction_table_naming))
    workflow.add_node("relational_compilation", _wrap_step_3_5(step_3_5_relational_schema_compilation))
    
    # Set entry point
    workflow.set_entry_point("information_needs")
    
    # Add edges
    workflow.add_edge("information_needs", "completeness_check")
    workflow.add_edge("completeness_check", "phase2_reexecution")
    workflow.add_edge("phase2_reexecution", "er_compilation")
    workflow.add_edge("er_compilation", "junction_naming")
    workflow.add_edge("junction_naming", "relational_compilation")
    workflow.add_edge("relational_compilation", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

