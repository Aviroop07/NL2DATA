"""Phase 2: Attribute Discovery & Schema Design."""

from .step_2_1_attribute_count_detection import (
    step_2_1_attribute_count_detection,
    step_2_1_attribute_count_detection_batch,
)
from .step_2_2_intrinsic_attributes import (
    step_2_2_intrinsic_attributes,
    step_2_2_intrinsic_attributes_batch,
)
from .step_2_3_attribute_synonym_detection import (
    step_2_3_attribute_synonym_detection,
    step_2_3_attribute_synonym_detection_batch,
)
from .step_2_4_composite_attribute_handling import (
    step_2_4_composite_attribute_handling,
    step_2_4_composite_attribute_handling_batch,
)
from .step_2_5_temporal_attributes_detection import (
    step_2_5_temporal_attributes_detection,
    step_2_5_temporal_attributes_detection_batch,
)
from .step_2_6_naming_convention_validation import step_2_6_naming_convention_validation
from .step_2_7_primary_key_identification import (
    step_2_7_primary_key_identification,
    step_2_7_primary_key_identification_batch,
)
from .step_2_8_multivalued_derived_detection import (
    step_2_8_multivalued_derived_detection,
    step_2_8_multivalued_derived_detection_batch,
)
from .step_2_9_derived_attribute_formulas import (
    step_2_9_derived_attribute_formulas,
    step_2_9_derived_attribute_formulas_batch,
)
from .step_2_15_relation_intrinsic_attributes import (
    step_2_15_relation_intrinsic_attributes,
    step_2_15_relation_intrinsic_attributes_batch,
)
from .step_2_16_cross_entity_attribute_reconciliation import (
    step_2_16_cross_entity_attribute_reconciliation_single as step_2_16_cross_entity_attribute_reconciliation,
    step_2_16_cross_entity_attribute_reconciliation_batch,
)

__all__ = [
    "step_2_1_attribute_count_detection",
    "step_2_1_attribute_count_detection_batch",
    "step_2_2_intrinsic_attributes",
    "step_2_2_intrinsic_attributes_batch",
    "step_2_3_attribute_synonym_detection",
    "step_2_3_attribute_synonym_detection_batch",
    "step_2_4_composite_attribute_handling",
    "step_2_4_composite_attribute_handling_batch",
    "step_2_5_temporal_attributes_detection",
    "step_2_5_temporal_attributes_detection_batch",
    "step_2_6_naming_convention_validation",
    "step_2_7_primary_key_identification",
    "step_2_7_primary_key_identification_batch",
    "step_2_8_multivalued_derived_detection",
    "step_2_8_multivalued_derived_detection_batch",
    "step_2_9_derived_attribute_formulas",
    "step_2_9_derived_attribute_formulas_batch",
    "step_2_15_relation_intrinsic_attributes",
    "step_2_15_relation_intrinsic_attributes_batch",
    "step_2_16_cross_entity_attribute_reconciliation",
    "step_2_16_cross_entity_attribute_reconciliation_batch",
]

