"""LangGraph StateGraph definitions for IR generation workflows - modular structure.

This package contains per-phase graph modules for better organization.
All functions are re-exported here for backward compatibility.
"""

# Import all phase graph creation functions
from .phase1 import create_phase_1_graph
from .phase2 import create_phase_2_graph
from .phase3 import create_phase_3_graph  # Phase 3: ER Design
from .phase4 import create_phase_4_graph  # Phase 4: Relational Schema
from .phase5 import create_phase_5_graph  # Phase 5: Data Type Assignment
from .phase10 import create_phase_6_graph  # Phase 6: DDL Generation & Schema Creation (old Phase 10)
from .phase7 import create_phase_7_graph  # Phase 7: Information Mining (old Phase 6)
from .phase8 import create_phase_8_graph  # Phase 8: Functional Dependencies (old Phase 7)
from .phase9 import create_phase_9_graph  # Phase 9: Generation Strategies
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
    "create_phase_8_graph",
    "create_phase_9_graph",
    "create_complete_workflow_graph",
    "get_phase_graph",
]

