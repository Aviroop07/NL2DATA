"""Master graph: Complete workflow connecting all phases."""

from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger
from .phase1 import create_phase_1_graph
from .phase2 import create_phase_2_graph
from .phase3 import create_phase_3_graph
from .phase4 import create_phase_4_graph
from .phase5 import create_phase_5_graph
from .phase6 import create_phase_6_graph
from .phase7 import create_phase_7_graph


def create_complete_workflow_graph() -> StateGraph:
    """Create LangGraph StateGraph for complete workflow (all phases).
    
    This master graph connects all phase graphs sequentially:
    Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7
    
    Each phase graph is executed as a subgraph, with state passed between phases.
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.orchestration.phase_gates import check_phase_gate
    
    # Create master graph
    workflow = StateGraph(IRGenerationState)
    
    # Create individual phase graphs
    phase_1_graph = create_phase_1_graph()
    phase_2_graph = create_phase_2_graph()
    phase_3_graph = create_phase_3_graph()
    phase_4_graph = create_phase_4_graph()
    phase_5_graph = create_phase_5_graph()
    phase_6_graph = create_phase_6_graph()
    phase_7_graph = create_phase_7_graph()
    
    # Add phase execution nodes (each runs the compiled phase graph)
    async def execute_phase_1(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 1 graph."""
        logger.info("[LangGraph] Executing Phase 1: Domain & Entity Discovery")
        result = await phase_1_graph.ainvoke(state)
        # Check phase gate
        try:
            check_phase_gate(1, result)
        except Exception as e:
            logger.error(f"Phase 1 gate failed: {e}")
            raise
        return {**result, "phase": 1}
    
    async def execute_phase_2(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 2 graph."""
        logger.info("[LangGraph] Executing Phase 2: Attribute Discovery & Schema Design")
        result = await phase_2_graph.ainvoke(state)
        try:
            check_phase_gate(2, result)
        except Exception as e:
            logger.error(f"Phase 2 gate failed: {e}")
            raise
        return {**result, "phase": 2}
    
    async def execute_phase_3(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 3 graph."""
        logger.info("[LangGraph] Executing Phase 3: Query Requirements & Schema Refinement")
        result = await phase_3_graph.ainvoke(state)
        try:
            check_phase_gate(3, result)
        except Exception as e:
            logger.error(f"Phase 3 gate failed: {e}")
            raise
        return {**result, "phase": 3}
    
    async def execute_phase_4(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 4 graph."""
        logger.info("[LangGraph] Executing Phase 4: Functional Dependencies & Data Types")
        result = await phase_4_graph.ainvoke(state)
        try:
            check_phase_gate(4, result)
        except Exception as e:
            logger.error(f"Phase 4 gate failed: {e}")
            raise
        return {**result, "phase": 4}
    
    async def execute_phase_5(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 5 graph."""
        logger.info("[LangGraph] Executing Phase 5: DDL & SQL Generation")
        result = await phase_5_graph.ainvoke(state)
        try:
            check_phase_gate(5, result)
        except Exception as e:
            logger.error(f"Phase 5 gate failed: {e}")
            raise
        return {**result, "phase": 5}
    
    async def execute_phase_6(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 6 graph."""
        logger.info("[LangGraph] Executing Phase 6: Constraints & Distributions")
        result = await phase_6_graph.ainvoke(state)
        try:
            check_phase_gate(6, result)
        except Exception as e:
            logger.error(f"Phase 6 gate failed: {e}")
            raise
        return {**result, "phase": 6}
    
    async def execute_phase_7(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 7 graph."""
        logger.info("[LangGraph] Executing Phase 7: Generation Strategies")
        result = await phase_7_graph.ainvoke(state)
        try:
            check_phase_gate(7, result)
        except Exception as e:
            logger.error(f"Phase 7 gate failed: {e}")
            raise
        return {**result, "phase": 7}
    
    # Add nodes
    workflow.add_node("phase_1", execute_phase_1)
    workflow.add_node("phase_2", execute_phase_2)
    workflow.add_node("phase_3", execute_phase_3)
    workflow.add_node("phase_4", execute_phase_4)
    workflow.add_node("phase_5", execute_phase_5)
    workflow.add_node("phase_6", execute_phase_6)
    workflow.add_node("phase_7", execute_phase_7)
    
    # Set entry point
    workflow.set_entry_point("phase_1")
    
    # Add sequential edges
    workflow.add_edge("phase_1", "phase_2")
    workflow.add_edge("phase_2", "phase_3")
    workflow.add_edge("phase_3", "phase_4")
    workflow.add_edge("phase_4", "phase_5")
    workflow.add_edge("phase_5", "phase_6")
    workflow.add_edge("phase_6", "phase_7")
    workflow.add_edge("phase_7", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)


def get_phase_graph(phase: int) -> StateGraph:
    """
    Get LangGraph StateGraph for a specific phase.
    
    Args:
        phase: Phase number (1-7)
        
    Returns:
        Compiled StateGraph for the specified phase
        
    Raises:
        ValueError: If phase number is invalid
    """
    from .phase1 import create_phase_1_graph
    from .phase2 import create_phase_2_graph
    from .phase3 import create_phase_3_graph
    from .phase4 import create_phase_4_graph
    from .phase5 import create_phase_5_graph
    from .phase6 import create_phase_6_graph
    from .phase7 import create_phase_7_graph
    
    phase_graphs = {
        1: create_phase_1_graph,
        2: create_phase_2_graph,
        3: create_phase_3_graph,
        4: create_phase_4_graph,
        5: create_phase_5_graph,
        6: create_phase_6_graph,
        7: create_phase_7_graph,
    }
    
    graph_func = phase_graphs.get(phase)
    if not graph_func:
        raise ValueError(f"Invalid phase number: {phase}. Must be 1-7.")
    
    return graph_func()

