"""Example usage of LangGraph Phase 1 workflow.

This demonstrates how to use the LangGraph StateGraph for Phase 1 execution.
"""

import asyncio
from .state import create_initial_state
from .graphs import create_phase_1_graph
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


async def run_phase_1_with_langgraph(nl_description: str):
    """Run Phase 1 using LangGraph StateGraph.
    
    Args:
        nl_description: Natural language description of database requirements
        
    Returns:
        Final state after Phase 1 completion
    """
    # Create initial state
    initial_state = create_initial_state(nl_description)
    
    # Create and compile graph
    app = create_phase_1_graph()
    
    # Execute workflow
    logger.info("Starting Phase 1 workflow with LangGraph")
    
    # Option 1: Execute synchronously (wait for completion)
    final_state = await app.ainvoke(initial_state)
    
    logger.info(f"Phase 1 completed. Found {len(final_state.get('entities', []))} entities and {len(final_state.get('relations', []))} relations")
    
    return final_state


async def run_phase_1_with_streaming(nl_description: str):
    """Run Phase 1 with streaming updates.
    
    Args:
        nl_description: Natural language description of database requirements
        
    Yields:
        Progress updates as workflow executes
    """
    # Create initial state
    initial_state = create_initial_state(nl_description)
    
    # Create and compile graph
    app = create_phase_1_graph()
    
    logger.info("Starting Phase 1 workflow with streaming")
    
    # Stream execution with real-time updates
    async for event in app.astream(initial_state, stream_mode="updates"):
        node_name = list(event.keys())[0]
        node_output = event[node_name]
        
        # Emit progress update
        yield {
            "step": node_name,
            "status": "completed",
            "output": node_output
        }
        
        logger.info(f"[Stream] Step {node_name} completed")


# Example usage
if __name__ == "__main__":
    description = """
    I need a database for an IoT sensor monitoring system. 
    The system tracks sensors installed in various plants. 
    Each sensor has a type (temperature, pressure, humidity) and belongs to a plant.
    Sensors generate readings over time. 
    The system also tracks maintenance events and incidents.
    """
    
    # Run with LangGraph
    final_state = asyncio.run(run_phase_1_with_langgraph(description))
    
    print(f"\n=== Phase 1 Results ===")
    print(f"Domain: {final_state.get('domain')}")
    print(f"Entities: {len(final_state.get('entities', []))}")
    print(f"Relations: {len(final_state.get('relations', []))}")

