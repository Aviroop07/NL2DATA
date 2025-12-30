"""Phase 7: Generation Strategies Graph."""

from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _wrap_step_7_1(step_func):
    """Wrap Step 7.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.1: Numerical Range Definition")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            data_types=state.get("data_types", {}),
            constraint_specs=state.get("constraint_specs", []),
            nl_description=state["nl_description"]
        )
        
        # Update generation strategies
        generation_strategies = state.get("generation_strategies", {})
        entity_results = result.get("entity_results", {})
        for entity_name, entity_result in entity_results.items():
            if entity_name not in generation_strategies:
                generation_strategies[entity_name] = {}
            for attr_name, strategy in entity_result.get("attribute_strategies", {}).items():
                if attr_name not in generation_strategies[entity_name]:
                    generation_strategies[entity_name][attr_name] = {}
                generation_strategies[entity_name][attr_name].update(strategy)
        
        return {
            "generation_strategies": generation_strategies,
            "current_step": "7.1",
            "previous_answers": {**prev_answers, "7.1": result}
        }
    return node


def _wrap_step_7_2(step_func):
    """Wrap Step 7.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.2: Text Generation Strategy")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            data_types=state.get("data_types", {}),
            domain=state.get("domain"),
            nl_description=state["nl_description"]
        )
        
        # Update generation strategies
        generation_strategies = state.get("generation_strategies", {})
        entity_results = result.get("entity_results", {})
        for entity_name, entity_result in entity_results.items():
            if entity_name not in generation_strategies:
                generation_strategies[entity_name] = {}
            for attr_name, strategy in entity_result.get("attribute_strategies", {}).items():
                if attr_name not in generation_strategies[entity_name]:
                    generation_strategies[entity_name][attr_name] = {}
                generation_strategies[entity_name][attr_name].update(strategy)
        
        return {
            "generation_strategies": generation_strategies,
            "current_step": "7.2",
            "previous_answers": {**prev_answers, "7.2": result}
        }
    return node


def _wrap_step_7_3(step_func):
    """Wrap Step 7.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.3: Boolean Dependency Analysis")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            data_types=state.get("data_types", {}),
            relations=state.get("relations", [])
        )
        
        # Update generation strategies
        generation_strategies = state.get("generation_strategies", {})
        entity_results = result.get("entity_results", {})
        for entity_name, entity_result in entity_results.items():
            if entity_name not in generation_strategies:
                generation_strategies[entity_name] = {}
            for attr_name, strategy in entity_result.get("attribute_strategies", {}).items():
                if attr_name not in generation_strategies[entity_name]:
                    generation_strategies[entity_name][attr_name] = {}
                generation_strategies[entity_name][attr_name].update(strategy)
        
        return {
            "generation_strategies": generation_strategies,
            "current_step": "7.3",
            "previous_answers": {**prev_answers, "7.3": result}
        }
    return node


def _wrap_step_7_4(step_func):
    """Wrap Step 7.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.4: Data Volume Specifications")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            entity_cardinalities=state.get("entity_cardinalities", {}),
            nl_description=state["nl_description"]
        )
        
        return {
            "current_step": "7.4",
            "previous_answers": {**prev_answers, "7.4": result},
            "metadata": {
                **state.get("metadata", {}),
                "data_volumes": result.get("entity_volumes", {})
            }
        }
    return node


def _wrap_step_7_5(step_func):
    """Wrap Step 7.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.5: Partitioning Strategy")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            entities=state.get("entities", []),
            entity_volumes=metadata.get("data_volumes", {}),
            nl_description=state["nl_description"]
        )
        
        return {
            "current_step": "7.5",
            "previous_answers": {**prev_answers, "7.5": result}
        }
    return node


def _wrap_step_7_6(step_func):
    """Wrap Step 7.6 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 7.6: Distribution Compilation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            generation_strategies=state.get("generation_strategies", {}),
            entities=state.get("entities", []),
            attributes=state.get("attributes", {})
        )
        
        return {
            "current_step": "7.6",
            "previous_answers": {**prev_answers, "7.6": result},
            "metadata": {
                **state.get("metadata", {}),
                "generation_ir": result
            }
        }
    return node


def create_phase_7_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 7 (Generation Strategies).
    
    This graph orchestrates all Phase 7 steps:
    1. Numerical Range Definition (7.1) - parallel per numerical attribute
    2. Text Generation Strategy (7.2) - parallel per text attribute
    3. Boolean Dependency Analysis (7.3) - parallel per boolean attribute
    4. Data Volume Specifications (7.4) - singular
    5. Partitioning Strategy (7.5) - parallel per entity (conditional)
    6. Distribution Compilation (7.6) - deterministic
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase7 import (
        step_7_1_numerical_range_definition_batch,
        step_7_2_text_generation_strategy_batch,
        step_7_3_boolean_dependency_analysis_batch,
        step_7_4_data_volume_specifications,
        step_7_5_partitioning_strategy_batch,
        step_7_6_distribution_compilation,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("numerical_ranges", _wrap_step_7_1(step_7_1_numerical_range_definition_batch))
    workflow.add_node("text_strategies", _wrap_step_7_2(step_7_2_text_generation_strategy_batch))
    workflow.add_node("boolean_dependencies", _wrap_step_7_3(step_7_3_boolean_dependency_analysis_batch))
    workflow.add_node("data_volumes", _wrap_step_7_4(step_7_4_data_volume_specifications))
    workflow.add_node("partitioning", _wrap_step_7_5(step_7_5_partitioning_strategy_batch))
    workflow.add_node("distribution_compilation", _wrap_step_7_6(step_7_6_distribution_compilation))
    
    # Set entry point
    workflow.set_entry_point("numerical_ranges")
    
    # Add edges (parallel execution for 7.1, 7.2, 7.3)
    workflow.add_edge("numerical_ranges", "text_strategies")
    workflow.add_edge("text_strategies", "boolean_dependencies")
    workflow.add_edge("boolean_dependencies", "data_volumes")
    workflow.add_edge("data_volumes", "partitioning")
    workflow.add_edge("partitioning", "distribution_compilation")
    workflow.add_edge("distribution_compilation", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

