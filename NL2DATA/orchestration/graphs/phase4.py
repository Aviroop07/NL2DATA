"""Phase 4: Functional Dependencies & Data Types Graph."""

from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _wrap_step_4_1(step_func):
    """Wrap Step 4.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.1: Functional Dependency Analysis")
        prev_answers = state.get("previous_answers", {})
        
        # Extract derived attributes from Phase 2
        derived_attrs = {}
        if "2.8" in prev_answers:
            step_2_8_result = prev_answers.get("2.8", {})
            entity_results = step_2_8_result.get("entity_results", {})
            for entity_name, entity_result in entity_results.items():
                derived_list = entity_result.get("derived", [])
                formulas = entity_result.get("derivation_rules", {})
                derived_attrs[entity_name] = {attr: formulas.get(attr, "") for attr in derived_list}
        
        result = await step_func(
            entities=state.get("entities", []),
            entity_attributes=state.get("attributes", {}),
            entity_primary_keys=state.get("primary_keys", {}),
            entity_derived_attributes=derived_attrs,
            nl_description=state["nl_description"],
            domain=state.get("domain")
        )
        
        # Update functional dependencies
        functional_dependencies = []
        entity_results = result.get("entity_results", {})
        for entity_name, entity_result in entity_results.items():
            final_result = entity_result.get("final_result", {})
            fds = final_result.get("functional_dependencies", [])
            for fd in fds:
                functional_dependencies.append({
                    "entity": entity_name,
                    **fd
                })
        
        return {
            "functional_dependencies": functional_dependencies,
            "current_step": "4.1",
            "previous_answers": {**prev_answers, "4.1": result}
        }
    return node


def _wrap_step_4_2(step_func):
    """Wrap Step 4.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.2: 3NF Normalization")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            relational_schema=metadata.get("relational_schema", {}),
            functional_dependencies=state.get("functional_dependencies", [])
        )
        
        return {
            "current_step": "4.2",
            "previous_answers": {**prev_answers, "4.2": result},
            "metadata": {
                **state.get("metadata", {}),
                "normalized_schema": result
            }
        }
    return node


def _wrap_step_4_3(step_func):
    """Wrap Step 4.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.3: Data Type Assignment")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            normalized_schema=metadata.get("normalized_schema", {})
        )
        
        # Update data types
        data_types = result.get("entity_results", {})
        
        return {
            "data_types": data_types,
            "current_step": "4.3",
            "previous_answers": {**prev_answers, "4.3": result}
        }
    return node


def _wrap_step_4_4(step_func):
    """Wrap Step 4.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.4: Categorical Detection")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            data_types=state.get("data_types", {})
        )
        
        # Update categorical attributes
        categorical_attributes = {}
        entity_results = result.get("entity_results", {})
        for entity_name, entity_result in entity_results.items():
            categorical_attributes[entity_name] = entity_result.get("categorical_attributes", [])
        
        return {
            "categorical_attributes": categorical_attributes,
            "current_step": "4.4",
            "previous_answers": {**prev_answers, "4.4": result}
        }
    return node


def _wrap_step_4_5(step_func):
    """Wrap Step 4.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.5: Check Constraint Detection")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            categorical_attributes=state.get("categorical_attributes", {}),
            attributes=state.get("attributes", {})
        )
        
        return {
            "current_step": "4.5",
            "previous_answers": {**prev_answers, "4.5": result}
        }
    return node


def _wrap_step_4_6(step_func):
    """Wrap Step 4.6 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.6: Categorical Value Extraction")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            categorical_attributes=state.get("categorical_attributes", {}),
            attributes=state.get("attributes", {}),
            nl_description=state["nl_description"]
        )
        
        return {
            "current_step": "4.6",
            "previous_answers": {**prev_answers, "4.6": result}
        }
    return node


def _wrap_step_4_7(step_func):
    """Wrap Step 4.7 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 4.7: Categorical Distribution")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            categorical_attributes=state.get("categorical_attributes", {}),
            categorical_values=prev_answers.get("4.6", {})
        )
        
        return {
            "current_step": "4.7",
            "previous_answers": {**prev_answers, "4.7": result}
        }
    return node


def create_phase_4_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 4 (Functional Dependencies & Data Types).
    
    This graph orchestrates all Phase 4 steps:
    1. Functional Dependency Analysis (4.1) - parallel per entity (loop)
    2. 3NF Normalization (4.2) - deterministic
    3. Data Type Assignment (4.3) - parallel per entity
    4. Categorical Detection (4.4) - parallel per entity
    5. Check Constraint Detection (4.5) - parallel per categorical attribute
    6. Categorical Value Extraction (4.6) - parallel per categorical attribute
    7. Categorical Distribution (4.7) - parallel per categorical attribute
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase4 import (
        step_4_1_functional_dependency_analysis_batch,
        step_4_2_3nf_normalization,
        step_4_3_data_type_assignment_batch,
        step_4_4_categorical_detection_batch,
        step_4_5_check_constraint_detection_batch,
        step_4_6_categorical_value_extraction_batch,
        step_4_7_categorical_distribution_batch,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("functional_dependencies", _wrap_step_4_1(step_4_1_functional_dependency_analysis_batch))
    workflow.add_node("normalization", _wrap_step_4_2(step_4_2_3nf_normalization))
    workflow.add_node("data_types", _wrap_step_4_3(step_4_3_data_type_assignment_batch))
    workflow.add_node("categorical_detection", _wrap_step_4_4(step_4_4_categorical_detection_batch))
    workflow.add_node("check_constraints", _wrap_step_4_5(step_4_5_check_constraint_detection_batch))
    workflow.add_node("categorical_values", _wrap_step_4_6(step_4_6_categorical_value_extraction_batch))
    workflow.add_node("categorical_distribution", _wrap_step_4_7(step_4_7_categorical_distribution_batch))
    
    # Set entry point
    workflow.set_entry_point("functional_dependencies")
    
    # Add edges
    workflow.add_edge("functional_dependencies", "normalization")
    workflow.add_edge("normalization", "data_types")
    workflow.add_edge("data_types", "categorical_detection")
    workflow.add_edge("categorical_detection", "check_constraints")
    workflow.add_edge("check_constraints", "categorical_values")
    workflow.add_edge("categorical_values", "categorical_distribution")
    workflow.add_edge("categorical_distribution", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

