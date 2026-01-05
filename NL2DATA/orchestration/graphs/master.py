"""Master graph: Complete workflow connecting all phases."""

from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger
from .phase1 import create_phase_1_graph
from .phase2 import create_phase_2_graph
from .phase3 import create_phase_3_graph  # ER Design
from .phase4 import create_phase_4_graph  # Relational Schema
from .phase5 import create_phase_5_graph  # Data Type Assignment
from .phase6 import create_phase_6_graph  # DDL Generation & Schema Creation
from .phase7 import create_phase_7_graph  # Information Mining
from .phase8 import create_phase_8_graph  # Functional Dependencies & Constraints
from .phase9 import create_phase_9_graph  # Generation Strategies


def create_complete_workflow_graph() -> StateGraph:
    """Create LangGraph StateGraph for complete workflow (all phases).
    
    This master graph connects all phase graphs sequentially:
    Phase 1 → Phase 2 (includes multivalued/derived detection) → Phase 3 (ER Design) → Phase 4 (Relational Schema) → 
    Phase 5 (Data Types, includes nullability) → Phase 6 (DDL Generation & Schema Creation, old Phase 10) → 
    Phase 7 (Information Mining, SQL validation only, old Phase 6) → Phase 8 (Functional Dependencies, old Phase 7) → 
    Phase 9 (Constraints & Generation Strategies, excludes derived and constrained columns, old Phase 8)
    
    Pipeline ends after Phase 9 with complete metadata (relational schema, constraints, generation strategies).
    
    Each phase graph is executed as a subgraph, with state passed between phases.
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.orchestration.phase_gates import check_phase_gate
    
    # Create master graph
    workflow = StateGraph(IRGenerationState)
    
    # Create individual phase graphs (new pipeline)
    phase_1_graph = create_phase_1_graph()
    phase_2_graph = create_phase_2_graph()
    phase_3_graph = create_phase_3_graph()  # ER Design Compilation
    phase_4_graph = create_phase_4_graph()  # Relational Schema Compilation
    phase_5_graph = create_phase_5_graph()  # Data Type Assignment (includes nullability)
    phase_6_graph = create_phase_6_graph()  # DDL Generation & Schema Creation (old Phase 10)
    phase_7_graph = create_phase_7_graph()  # Information Mining (SQL validation only, old Phase 6)
    phase_8_graph = create_phase_8_graph()  # Functional Dependencies (old Phase 7)
    phase_9_graph = create_phase_9_graph()  # Constraints & Generation Strategies (excludes derived and constrained columns, old Phase 8)
    
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
        
        # Validate phase transition from Phase 1 to Phase 2
        from NL2DATA.utils.validation.schema_anchored import validate_phase_transition
        transition_result = validate_phase_transition(1, 2, state)
        if not transition_result["valid"]:
            logger.warning(
                f"Phase 1→2 transition validation issues: {transition_result['errors']}. "
                f"Warnings: {transition_result['warnings']}"
            )
        
        result = await phase_2_graph.ainvoke(state)
        try:
            check_phase_gate(2, result)
        except Exception as e:
            logger.error(f"Phase 2 gate failed: {e}")
            raise
        
        return {**result, "phase": 2}
    
    async def execute_phase_3(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 3 graph (ER Design Compilation)."""
        logger.info("[LangGraph] Executing Phase 3: ER Design Compilation")
        
        # Validate phase transition from Phase 2 to Phase 3
        from NL2DATA.utils.validation.schema_anchored import validate_phase_transition
        transition_result = validate_phase_transition(2, 3, state)
        if not transition_result["valid"]:
            logger.warning(
                f"Phase 2→3 transition validation issues: {transition_result['errors']}. "
                f"Warnings: {transition_result['warnings']}"
            )
        
        result = await phase_3_graph.ainvoke(state)
        try:
            check_phase_gate(3, result)
        except Exception as e:
            logger.error(f"Phase 3 gate failed: {e}")
            raise
        return {**result, "phase": 3}
    
    async def execute_phase_4(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 4 graph (NEW: Relational Schema Compilation)."""
        logger.info("[LangGraph] Executing Phase 4: Relational Schema Compilation")
        result = await phase_4_graph.ainvoke(state)
        try:
            check_phase_gate(4, result)
        except Exception as e:
            logger.error(f"Phase 4 gate failed: {e}")
            raise
        return {**result, "phase": 4}
    
    async def execute_phase_5(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 5 graph (Data Type Assignment)."""
        logger.info("[LangGraph] Executing Phase 5: Data Type Assignment")
        
        # Validate frozen schema immutability (Phase 5 is first phase after freeze)
        from NL2DATA.utils.validation.schema_freeze import validate_frozen_schema_immutability
        freeze_validation = validate_frozen_schema_immutability(5, state)
        if not freeze_validation["valid"]:
            logger.error(f"Phase 5: Frozen schema validation failed: {freeze_validation['errors']}")
        if freeze_validation["warnings"]:
            logger.warning(f"Phase 5: Frozen schema validation warnings: {freeze_validation['warnings']}")
        
        result = await phase_5_graph.ainvoke(state)
        try:
            check_phase_gate(5, result)
        except Exception as e:
            logger.error(f"Phase 5 gate failed: {e}")
            raise
        
        return {**result, "phase": 5}
    
    async def execute_phase_6(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 6 graph (DDL Generation & Schema Creation - old Phase 10)."""
        logger.info("[LangGraph] Executing Phase 6: DDL Generation & Schema Creation")
        
        result = await phase_6_graph.ainvoke(state)
        try:
            check_phase_gate(6, result)
        except Exception as e:
            logger.error(f"Phase 6 gate failed: {e}")
            raise
        return {**result, "phase": 6}
    
    async def execute_phase_7(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 7 graph (Information Mining - old Phase 6)."""
        logger.info("[LangGraph] Executing Phase 7: Information Mining")
        result = await phase_7_graph.ainvoke(state)
        try:
            check_phase_gate(7, result)
        except Exception as e:
            logger.error(f"Phase 7 gate failed: {e}")
            raise
        return {**result, "phase": 7}
    
    async def execute_phase_8(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 8 graph (Functional Dependencies - old Phase 7)."""
        logger.info("[LangGraph] Executing Phase 8: Functional Dependencies")
        result = await phase_8_graph.ainvoke(state)
        try:
            check_phase_gate(8, result)
        except Exception as e:
            logger.error(f"Phase 8 gate failed: {e}")
            raise
        return {**result, "phase": 8}
    
    async def execute_phase_9(state: IRGenerationState) -> Dict[str, Any]:
        """Execute Phase 9 graph (Constraints & Generation Strategies - old Phase 8)."""
        logger.info("[LangGraph] Executing Phase 9: Constraints & Generation Strategies")
        result = await phase_9_graph.ainvoke(state)
        try:
            check_phase_gate(9, result)
        except Exception as e:
            logger.error(f"Phase 9 gate failed: {e}")
            raise
        return {**result, "phase": 9}
    
    # Add nodes
    workflow.add_node("phase_1", execute_phase_1)
    workflow.add_node("phase_2", execute_phase_2)
    workflow.add_node("phase_3", execute_phase_3)
    workflow.add_node("phase_4", execute_phase_4)
    workflow.add_node("phase_5", execute_phase_5)
    workflow.add_node("phase_6", execute_phase_6)
    workflow.add_node("phase_7", execute_phase_7)
    workflow.add_node("phase_8", execute_phase_8)
    workflow.add_node("phase_9", execute_phase_9)
    
    # Set entry point
    workflow.set_entry_point("phase_1")
    
    # Add sequential edges for complete pipeline flow
    # Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7 → Phase 8 → Phase 9 → END
    workflow.add_edge("phase_1", "phase_2")
    workflow.add_edge("phase_2", "phase_3")
    workflow.add_edge("phase_3", "phase_4")
    workflow.add_edge("phase_4", "phase_5")
    workflow.add_edge("phase_5", "phase_6")
    workflow.add_edge("phase_6", "phase_7")
    workflow.add_edge("phase_7", "phase_8")
    workflow.add_edge("phase_8", "phase_9")
    workflow.add_edge("phase_9", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)


def create_workflow_up_to_phase(max_phase: int) -> StateGraph:
    """
    Create LangGraph StateGraph for workflow up to a specific phase.
    
    Args:
        max_phase: Maximum phase to execute (1-9)
        
    Returns:
        Compiled StateGraph ready for execution
        
    Raises:
        ValueError: If max_phase is invalid
    """
    if max_phase < 1 or max_phase > 9:
        raise ValueError(f"Invalid max_phase: {max_phase}. Must be between 1 and 9.")
    
    if max_phase == 9:
        return create_complete_workflow_graph()
    
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
    phase_8_graph = create_phase_8_graph()
    phase_9_graph = create_phase_9_graph()
    
    # Phase execution functions (same as in create_complete_workflow_graph)
    async def execute_phase_1(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 1: Domain & Entity Discovery")
        result = await phase_1_graph.ainvoke(state)
        try:
            check_phase_gate(1, result)
        except Exception as e:
            logger.error(f"Phase 1 gate failed: {e}")
            raise
        return {**result, "phase": 1}
    
    async def execute_phase_2(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 2: Attribute Discovery & Schema Design")
        from NL2DATA.utils.validation.schema_anchored import validate_phase_transition
        transition_result = validate_phase_transition(1, 2, state)
        if not transition_result["valid"]:
            logger.warning(
                f"Phase 1→2 transition validation issues: {transition_result['errors']}. "
                f"Warnings: {transition_result['warnings']}"
            )
        result = await phase_2_graph.ainvoke(state)
        try:
            check_phase_gate(2, result)
        except Exception as e:
            logger.error(f"Phase 2 gate failed: {e}")
            raise
        return {**result, "phase": 2}
    
    async def execute_phase_3(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 3: ER Design Compilation")
        from NL2DATA.utils.validation.schema_anchored import validate_phase_transition
        transition_result = validate_phase_transition(2, 3, state)
        if not transition_result["valid"]:
            logger.warning(
                f"Phase 2→3 transition validation issues: {transition_result['errors']}. "
                f"Warnings: {transition_result['warnings']}"
            )
        result = await phase_3_graph.ainvoke(state)
        try:
            check_phase_gate(3, result)
        except Exception as e:
            logger.error(f"Phase 3 gate failed: {e}")
            raise
        return {**result, "phase": 3}
    
    async def execute_phase_4(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 4: Relational Schema Compilation")
        result = await phase_4_graph.ainvoke(state)
        try:
            check_phase_gate(4, result)
        except Exception as e:
            logger.error(f"Phase 4 gate failed: {e}")
            raise
        return {**result, "phase": 4}
    
    async def execute_phase_5(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 5: Data Type Assignment")
        from NL2DATA.utils.validation.schema_freeze import validate_frozen_schema_immutability
        freeze_validation = validate_frozen_schema_immutability(5, state)
        if not freeze_validation["valid"]:
            logger.error(f"Phase 5: Frozen schema validation failed: {freeze_validation['errors']}")
        if freeze_validation["warnings"]:
            logger.warning(f"Phase 5: Frozen schema validation warnings: {freeze_validation['warnings']}")
        result = await phase_5_graph.ainvoke(state)
        try:
            check_phase_gate(5, result)
        except Exception as e:
            logger.error(f"Phase 5 gate failed: {e}")
            raise
        return {**result, "phase": 5}
    
    async def execute_phase_6(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 6: DDL Generation & Schema Creation")
        result = await phase_6_graph.ainvoke(state)
        try:
            check_phase_gate(6, result)
        except Exception as e:
            logger.error(f"Phase 6 gate failed: {e}")
            raise
        return {**result, "phase": 6}
    
    async def execute_phase_7(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 7: Information Mining")
        result = await phase_7_graph.ainvoke(state)
        try:
            check_phase_gate(7, result)
        except Exception as e:
            logger.error(f"Phase 7 gate failed: {e}")
            raise
        return {**result, "phase": 7}
    
    async def execute_phase_8(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 8: Functional Dependencies")
        result = await phase_8_graph.ainvoke(state)
        try:
            check_phase_gate(8, result)
        except Exception as e:
            logger.error(f"Phase 8 gate failed: {e}")
            raise
        return {**result, "phase": 8}
    
    async def execute_phase_9(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Phase 9: Constraints & Generation Strategies")
        result = await phase_9_graph.ainvoke(state)
        try:
            check_phase_gate(9, result)
        except Exception as e:
            logger.error(f"Phase 9 gate failed: {e}")
            raise
        return {**result, "phase": 9}
    
    # Phase execution functions mapping
    phase_executors = {
        1: execute_phase_1,
        2: execute_phase_2,
        3: execute_phase_3,
        4: execute_phase_4,
        5: execute_phase_5,
        6: execute_phase_6,
        7: execute_phase_7,
        8: execute_phase_8,
        9: execute_phase_9,
    }
    
    # Add nodes for phases up to max_phase
    for phase_num in range(1, max_phase + 1):
        workflow.add_node(f"phase_{phase_num}", phase_executors[phase_num])
    
    # Set entry point
    workflow.set_entry_point("phase_1")
    
    # Add sequential edges up to max_phase
    for phase_num in range(1, max_phase):
        workflow.add_edge(f"phase_{phase_num}", f"phase_{phase_num + 1}")
    
    # Add edge from last phase to END
    workflow.add_edge(f"phase_{max_phase}", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)


def get_phase_graph(phase: int) -> StateGraph:
    """
    Get LangGraph StateGraph for a specific phase.
    
    Args:
        phase: Phase number (1, 2, 3, 4, 5, 6, 7, 8)
        
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
    from .phase8 import create_phase_8_graph
    from .phase9 import create_phase_9_graph
    
    phase_graphs = {
        1: create_phase_1_graph,
        2: create_phase_2_graph,
        3: create_phase_3_graph,
        4: create_phase_4_graph,
        5: create_phase_5_graph,
        6: create_phase_6_graph,
        7: create_phase_7_graph,
        8: create_phase_8_graph,
        9: create_phase_9_graph,
    }
    
    graph_func = phase_graphs.get(phase)
    if not graph_func:
        raise ValueError(f"Invalid phase number: {phase}. Valid phases: 1, 2, 3, 4, 5, 6, 7, 8, 9")
    
    return graph_func()

