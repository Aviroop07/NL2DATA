"""Phase 6: Constraints & Distributions Graph."""

from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _constraint_conflicts_resolved(state: IRGenerationState) -> Literal["resolved", "has_conflicts"]:
    """Check if constraint conflicts are resolved."""
    metadata = state.get("metadata", {})
    conflicts_resolved = metadata.get("conflicts_resolved", False)
    
    if conflicts_resolved:
        return "resolved"
    return "has_conflicts"


def _wrap_step_6_1(step_func):
    """Wrap Step 6.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.1: Constraint Detection")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            nl_description=state["nl_description"],
            previous_constraints=prev_answers.get("6.1", {}).get("constraints", []) if "6.1" in prev_answers else None
        )
        
        # Extract all constraints
        final_result = result.get("final_result", {})
        all_constraints = final_result.get("statistical_constraints", []) + \
                         final_result.get("structural_constraints", []) + \
                         final_result.get("distribution_constraints", []) + \
                         final_result.get("other_constraints", [])
        
        return {
            "current_step": "6.1",
            "previous_answers": {**prev_answers, "6.1": result},
            "metadata": {
                **state.get("metadata", {}),
                "detected_constraints": all_constraints
            }
        }
    return node


def _wrap_step_6_2(step_func):
    """Wrap Step 6.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.2: Constraint Scope Analysis")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            constraints=metadata.get("detected_constraints", []),
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            relations=state.get("relations", [])
        )
        
        return {
            "current_step": "6.2",
            "previous_answers": {**prev_answers, "6.2": result},
            "metadata": {
                **state.get("metadata", {}),
                "constraint_scopes": result.get("constraint_results", {})
            }
        }
    return node


def _wrap_step_6_3(step_func):
    """Wrap Step 6.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.3: Constraint Enforcement Strategy")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            constraints=metadata.get("detected_constraints", []),
            constraint_scopes=metadata.get("constraint_scopes", {}),
            schema=metadata.get("normalized_schema", {})
        )
        
        return {
            "current_step": "6.3",
            "previous_answers": {**prev_answers, "6.3": result},
            "metadata": {
                **state.get("metadata", {}),
                "constraint_enforcement": result.get("constraint_results", {})
            }
        }
    return node


def _wrap_step_6_4(step_func):
    """Wrap Step 6.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.4: Constraint Conflict Detection")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            constraints=metadata.get("detected_constraints", []),
            constraint_enforcement=metadata.get("constraint_enforcement", {})
        )
        
        return {
            "current_step": "6.4",
            "previous_answers": {**prev_answers, "6.4": result},
            "metadata": {
                **state.get("metadata", {}),
                "constraint_conflicts": result.get("conflicts", []),
                "conflicts_resolved": result.get("validation_passed", False)
            }
        }
    return node


def _wrap_step_6_5(step_func):
    """Wrap Step 6.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 6.5: Constraint Compilation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        result = await step_func(
            constraints=metadata.get("detected_constraints", []),
            constraint_scopes=metadata.get("constraint_scopes", {}),
            constraint_enforcement=metadata.get("constraint_enforcement", {})
        )
        
        # Update constraint specs
        constraint_specs = result.get("constraint_specs", [])
        
        return {
            "constraint_specs": constraint_specs,
            "current_step": "6.5",
            "previous_answers": {**prev_answers, "6.5": result}
        }
    return node


def create_phase_6_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 6 (Constraints & Distributions).
    
    This graph orchestrates all Phase 6 steps:
    1. Constraint Detection (6.1) - loop until no_more_changes
    2. Constraint Scope Analysis (6.2) - parallel per constraint
    3. Constraint Enforcement Strategy (6.3) - parallel per constraint
    4. Constraint Conflict Detection (6.4) - deterministic, conditional loop to 6.3 if conflicts
    5. Constraint Compilation (6.5) - deterministic
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase6 import (
        step_6_1_constraint_detection_with_loop,
        step_6_2_constraint_scope_analysis_batch,
        step_6_3_constraint_enforcement_strategy_batch,
        step_6_4_constraint_conflict_detection,
        step_6_5_constraint_compilation,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("constraint_detection", _wrap_step_6_1(step_6_1_constraint_detection_with_loop))
    workflow.add_node("constraint_scope", _wrap_step_6_2(step_6_2_constraint_scope_analysis_batch))
    workflow.add_node("constraint_enforcement", _wrap_step_6_3(step_6_3_constraint_enforcement_strategy_batch))
    workflow.add_node("constraint_conflicts", _wrap_step_6_4(step_6_4_constraint_conflict_detection))
    workflow.add_node("constraint_compilation", _wrap_step_6_5(step_6_5_constraint_compilation))
    
    # Set entry point
    workflow.set_entry_point("constraint_detection")
    
    # Add edges
    workflow.add_edge("constraint_detection", "constraint_scope")
    workflow.add_edge("constraint_scope", "constraint_enforcement")
    workflow.add_edge("constraint_enforcement", "constraint_conflicts")
    
    # Conditional: Loop back if conflicts found
    workflow.add_conditional_edges(
        "constraint_conflicts",
        _constraint_conflicts_resolved,
        {
            "resolved": "constraint_compilation",
            "has_conflicts": "constraint_enforcement"  # Loop back to fix
        }
    )
    
    workflow.add_edge("constraint_compilation", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

