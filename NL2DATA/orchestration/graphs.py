"""LangGraph StateGraph definitions for IR generation workflows.

This module now imports from the refactored graphs/ submodules for better organization.
All functions are re-exported here for backward compatibility.
"""

# Import from modular submodules
from .graphs.phase1 import create_phase_1_graph
from .graphs.phase2 import create_phase_2_graph
from .graphs.phase3 import create_phase_3_graph
from .graphs.phase4 import create_phase_4_graph
from .graphs.phase5 import create_phase_5_graph
from .graphs.phase6 import create_phase_6_graph
from .graphs.phase7 import create_phase_7_graph
from .graphs.master import (
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
