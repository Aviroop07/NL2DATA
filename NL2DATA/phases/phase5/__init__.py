"""Phase 5: Attribute Dependency Graph and Data Type Assignment.

This phase:
1. Builds the attribute dependency graph (Step 5.1)
2. Assigns types to independent attributes (Step 5.2) - excludes FKs
3. Derives FK types from PK types deterministically (Step 5.3)
4. Assigns types to dependent attributes (Step 5.4) - derived types determined from DSL formulas
5. Determines nullability for columns (Step 5.5) - excludes PKs and FKs with total participation
"""

from .step_5_1_attribute_dependency_graph import step_5_1_attribute_dependency_graph
from .step_5_2_independent_attribute_data_types import (
    step_5_2_independent_attribute_data_types,
    step_5_2_independent_attribute_data_types_batch,
)
from .step_5_3_deterministic_fk_data_types import step_5_3_deterministic_fk_data_types
from .step_5_4_dependent_attribute_data_types import (
    step_5_4_dependent_attribute_data_types,
    step_5_4_dependent_attribute_data_types_batch,
)
from .step_5_5_nullability_detection import (
    step_5_5_nullability_detection,
    step_5_5_nullability_detection_batch,
)

__all__ = [
    "step_5_1_attribute_dependency_graph",
    "step_5_2_independent_attribute_data_types",
    "step_5_2_independent_attribute_data_types_batch",
    "step_5_3_deterministic_fk_data_types",
    "step_5_4_dependent_attribute_data_types",
    "step_5_4_dependent_attribute_data_types_batch",
    "step_5_5_nullability_detection",
    "step_5_5_nullability_detection_batch",
]

