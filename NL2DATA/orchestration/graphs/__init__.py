"""LangGraph StateGraph definitions for IR generation workflows - modular structure.

This package contains per-phase graph modules for better organization.
All functions are re-exported here for backward compatibility.
"""

# Import all phase graph creation functions
from .phase1 import create_phase_1_graph
from .phase2 import create_phase_2_graph
from .phase3 import create_phase_3_graph
from .phase4 import create_phase_4_graph
from .phase5 import create_phase_5_graph
from .phase6 import create_phase_6_graph
from .phase7 import create_phase_7_graph
from .master import (
    create_complete_workflow_graph,
    get_phase_graph,
)

# Re-export for backward compatibility
__all__ = [
    "create_phase_1_graph",
    "create_phase_2_graph",
    "create_phase_3_graph",
    "create_phase_4_graph",
    "create_phase_5_graph",
    "create_phase_6_graph",
    "create_phase_7_graph",
    "create_complete_workflow_graph",
    "get_phase_graph",
]

