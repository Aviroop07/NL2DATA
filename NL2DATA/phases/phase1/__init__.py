"""Phase 1: Domain & Entity Discovery.

Note: Step 1.3 (domain inference) was merged into Step 1.1. The legacy
implementation remains available for compatibility in tests/debugging but is
deprecated — prefer `step_1_1_domain_detection` for domain detection/inference.
"""

from .model_router import get_model_for_step as get_model_for_phase1_step
import warnings

# Emit a deprecation warning when this module is imported to surface the change
# to developers and test/debug scripts that still reference step 1.3.
warnings.warn(
    "Phase1: step_1_3_domain_inference is deprecated; "
    "use step_1_1_domain_detection (merged explicit detection + inference).",
    DeprecationWarning,
)
from .step_1_1_domain_detection import step_1_1_domain_detection
from .step_1_2_entity_mention_detection import step_1_2_entity_mention_detection
from .step_1_4_key_entity_extraction import step_1_4_key_entity_extraction
from .step_1_5_relation_mention_detection import step_1_5_relation_mention_detection
from .step_1_6_auxiliary_entity_suggestion import step_1_6_auxiliary_entity_suggestion
from .step_1_7_entity_consolidation import step_1_7_entity_consolidation
from .step_1_75_entity_relation_reclassification import step_1_75_entity_relation_reclassification
from .step_1_76_entity_attribute_guardrail import step_1_76_entity_attribute_guardrail
from .step_1_8_entity_cardinality import step_1_8_entity_cardinality, step_1_8_entity_cardinality_single
from .step_1_9_key_relations_extraction import step_1_9_key_relations_extraction
from .step_1_10_schema_connectivity import (
    step_1_10_schema_connectivity,
    step_1_10_schema_connectivity_with_loop,
)
from .step_1_11_relation_cardinality import step_1_11_relation_cardinality, step_1_11_relation_cardinality_single
from .step_1_12_relation_validation import (
    step_1_12_relation_validation,
    step_1_12_relation_validation_with_loop,
)

__all__ = [
    "get_model_for_phase1_step",
    "step_1_1_domain_detection",
    "step_1_2_entity_mention_detection",
    "step_1_4_key_entity_extraction",
    "step_1_5_relation_mention_detection",
    "step_1_6_auxiliary_entity_suggestion",
    "step_1_7_entity_consolidation",
    "step_1_75_entity_relation_reclassification",
    "step_1_76_entity_attribute_guardrail",
    "step_1_8_entity_cardinality",
    "step_1_8_entity_cardinality_single",
    "step_1_9_key_relations_extraction",
    "step_1_10_schema_connectivity",
    "step_1_10_schema_connectivity_with_loop",
    "step_1_11_relation_cardinality",
    "step_1_11_relation_cardinality_single",
    "step_1_12_relation_validation",
    "step_1_12_relation_validation_with_loop",
]

