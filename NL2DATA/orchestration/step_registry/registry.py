"""Step registry - Single source of truth for all step definitions.

This module contains the complete registry of all steps in the IR generation
framework. All step metadata is defined here to eliminate inconsistencies.
"""

from typing import Dict, List, Optional
from .types import StepDefinition, CallType, StepType


# ============================================================================
# Phase 1: Domain & Entity Discovery
# ============================================================================

PHASE_1_STEPS: Dict[str, StepDefinition] = {
    "P1_S1_DOMAIN_DETECTION": StepDefinition(
        step_id="P1_S1_DOMAIN_DETECTION",
        phase=1,
        step_number="1.1",
        name="Domain Detection & Inference",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=[],
        avg_tokens_per_call=3000,  # Increased since it now handles both detection and inference
    ),
    "P1_S2_ENTITY_MENTION": StepDefinition(
        step_id="P1_S2_ENTITY_MENTION",
        phase=1,
        step_number="1.2",
        name="Entity Mention Detection",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S1_DOMAIN_DETECTION"],
        avg_tokens_per_call=1500,
    ),
    "P1_S4_KEY_ENTITY_EXTRACTION": StepDefinition(
        step_id="P1_S4_KEY_ENTITY_EXTRACTION",
        phase=1,
        step_number="1.4",
        name="Key Entity Extraction",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S1_DOMAIN_DETECTION", "P1_S2_ENTITY_MENTION"],
        avg_tokens_per_call=3000,
    ),
    "P1_S5_RELATION_MENTION": StepDefinition(
        step_id="P1_S5_RELATION_MENTION",
        phase=1,
        step_number="1.5",
        name="Relation Mention Detection",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=True,
        dependencies=["P1_S4_KEY_ENTITY_EXTRACTION"],
        avg_tokens_per_call=1500,
    ),
    "P1_S6_AUXILIARY_ENTITIES": StepDefinition(
        step_id="P1_S6_AUXILIARY_ENTITIES",
        phase=1,
        step_number="1.6",
        name="Auxiliary Entity Suggestion",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=True,
        dependencies=["P1_S4_KEY_ENTITY_EXTRACTION"],
        avg_tokens_per_call=2000,
    ),
    "P1_S7_ENTITY_CONSOLIDATION": StepDefinition(
        step_id="P1_S7_ENTITY_CONSOLIDATION",
        phase=1,
        step_number="1.7",
        name="Entity Consolidation",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S4_KEY_ENTITY_EXTRACTION", "P1_S5_RELATION_MENTION", "P1_S6_AUXILIARY_ENTITIES"],
        avg_tokens_per_call=2500,
    ),
    "P1_S7B_ENTITY_RECLASSIFICATION": StepDefinition(
        step_id="P1_S7B_ENTITY_RECLASSIFICATION",
        phase=1,
        step_number="1.75",
        name="Entity vs Relation Reclassification",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S7_ENTITY_CONSOLIDATION"],
        avg_tokens_per_call=2000,
    ),
    "P1_S8_ENTITY_CARDINALITY": StepDefinition(
        step_id="P1_S8_ENTITY_CARDINALITY",
        phase=1,
        step_number="1.8",
        name="Entity Cardinality & Table Type",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P1_S7B_ENTITY_RECLASSIFICATION"],
        avg_tokens_per_call=1500,
    ),
    "P1_S9_KEY_RELATIONS": StepDefinition(
        step_id="P1_S9_KEY_RELATIONS",
        phase=1,
        step_number="1.9",
        name="Key Relations Extraction",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S7_ENTITY_CONSOLIDATION", "P1_S8_ENTITY_CARDINALITY"],
        avg_tokens_per_call=3000,
    ),
    "P1_S10_SCHEMA_CONNECTIVITY": StepDefinition(
        step_id="P1_S10_SCHEMA_CONNECTIVITY",
        phase=1,
        step_number="1.10",
        name="Schema Connectivity Validation",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S7_ENTITY_CONSOLIDATION", "P1_S9_KEY_RELATIONS"],
        avg_tokens_per_call=2000,
    ),
    "P1_S11_RELATION_CARDINALITY": StepDefinition(
        step_id="P1_S11_RELATION_CARDINALITY",
        phase=1,
        step_number="1.11",
        name="Relation Cardinality & Participation",
        step_type=StepType.LLM,
        call_type=CallType.PER_RELATION,
        fanout_unit="relation",
        can_parallelize=True,
        dependencies=["P1_S9_KEY_RELATIONS"],
        avg_tokens_per_call=2000,
    ),
    "P1_S12_RELATION_VALIDATION": StepDefinition(
        step_id="P1_S12_RELATION_VALIDATION",
        phase=1,
        step_number="1.12",
        name="Relation Validation",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S9_KEY_RELATIONS", "P1_S11_RELATION_CARDINALITY"],
        avg_tokens_per_call=2500,
    ),
}


# ============================================================================
# Phase 2: Attribute Discovery & Schema Design
# ============================================================================

PHASE_2_STEPS: Dict[str, StepDefinition] = {
    "P2_S1_ATTRIBUTE_COUNT": StepDefinition(
        step_id="P2_S1_ATTRIBUTE_COUNT",
        phase=2,
        step_number="2.1",
        name="Attribute Count Detection",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P1_S7_ENTITY_CONSOLIDATION"],
        avg_tokens_per_call=1200,
    ),
    "P2_S2_INTRINSIC_ATTRIBUTES": StepDefinition(
        step_id="P2_S2_INTRINSIC_ATTRIBUTES",
        phase=2,
        step_number="2.2",
        name="Intrinsic Attributes",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S1_ATTRIBUTE_COUNT"],
        avg_tokens_per_call=2500,
    ),
    "P2_S3_ATTRIBUTE_SYNONYM": StepDefinition(
        step_id="P2_S3_ATTRIBUTE_SYNONYM",
        phase=2,
        step_number="2.3",
        name="Attribute Synonym Detection",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S2_INTRINSIC_ATTRIBUTES"],
        avg_tokens_per_call=1500,
    ),
    "P2_S4_COMPOSITE_ATTRIBUTES": StepDefinition(
        step_id="P2_S4_COMPOSITE_ATTRIBUTES",
        phase=2,
        step_number="2.4",
        name="Composite Attribute Handling",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S3_ATTRIBUTE_SYNONYM"],
        avg_tokens_per_call=1500,
    ),
    "P2_S5_TEMPORAL_ATTRIBUTES": StepDefinition(
        step_id="P2_S5_TEMPORAL_ATTRIBUTES",
        phase=2,
        step_number="2.5",
        name="Temporal Attributes Detection",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S4_COMPOSITE_ATTRIBUTES"],
        avg_tokens_per_call=1200,
    ),
    "P2_S6_NAMING_VALIDATION": StepDefinition(
        step_id="P2_S6_NAMING_VALIDATION",
        phase=2,
        step_number="2.6",
        name="Naming Convention Validation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P2_S5_TEMPORAL_ATTRIBUTES"],
        avg_tokens_per_call=0,
    ),
    "P2_S7_PRIMARY_KEY": StepDefinition(
        step_id="P2_S7_PRIMARY_KEY",
        phase=2,
        step_number="2.7",
        name="Primary Key Identification",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S3_ATTRIBUTE_SYNONYM"],
        avg_tokens_per_call=2000,
    ),
    "P2_S8_MULTIVALUED_DERIVED": StepDefinition(
        step_id="P2_S8_MULTIVALUED_DERIVED",
        phase=2,
        step_number="2.8",
        name="Multivalued/Derived Detection",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S3_ATTRIBUTE_SYNONYM"],
        avg_tokens_per_call=2000,
    ),
    "P2_S9_DERIVED_FORMULAS": StepDefinition(
        step_id="P2_S9_DERIVED_FORMULAS",
        phase=2,
        step_number="2.9",
        name="Derived Attribute Formulas",
        step_type=StepType.LLM,
        call_type=CallType.PER_DERIVED_ATTRIBUTE,
        fanout_unit="derived_attribute",
        can_parallelize=True,
        dependencies=["P2_S8_MULTIVALUED_DERIVED"],
        avg_tokens_per_call=1500,
    ),
    "P2_S10_UNIQUE_CONSTRAINTS": StepDefinition(
        step_id="P2_S10_UNIQUE_CONSTRAINTS",
        phase=2,
        step_number="2.10",
        name="Unique Constraints",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S7_PRIMARY_KEY"],
        avg_tokens_per_call=1500,
    ),
    "P2_S11_NULLABILITY": StepDefinition(
        step_id="P2_S11_NULLABILITY",
        phase=2,
        step_number="2.11",
        name="Nullability Constraints",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S3_ATTRIBUTE_SYNONYM"],
        avg_tokens_per_call=1500,
    ),
    "P2_S12_DEFAULT_VALUES": StepDefinition(
        step_id="P2_S12_DEFAULT_VALUES",
        phase=2,
        step_number="2.12",
        name="Default Values",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S11_NULLABILITY"],
        avg_tokens_per_call=1500,
    ),
    "P2_S13_CHECK_CONSTRAINTS": StepDefinition(
        step_id="P2_S13_CHECK_CONSTRAINTS",
        phase=2,
        step_number="2.13",
        name="Check Constraints (Value Ranges)",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P2_S3_ATTRIBUTE_SYNONYM"],
        avg_tokens_per_call=2000,
    ),
    "P2_S14_RELATION_REALIZATION": StepDefinition(
        step_id="P2_S14_RELATION_REALIZATION",
        phase=2,
        step_number="2.14",
        name="Relation Realization",
        step_type=StepType.LLM,
        call_type=CallType.PER_RELATION,
        fanout_unit="relation",
        can_parallelize=True,
        dependencies=["P2_S7_PRIMARY_KEY", "P1_S11_RELATION_CARDINALITY"],
        avg_tokens_per_call=2500,
    ),
}


# ============================================================================
# Phase 3: ER Design Compilation
# ============================================================================

PHASE_3_STEPS: Dict[str, StepDefinition] = {
    "P3_S1_ER_COMPILATION": StepDefinition(
        step_id="P3_S1_ER_COMPILATION",
        phase=3,
        step_number="3.1",
        name="ER Design Compilation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P2_S14_RELATION_REALIZATION"],
        avg_tokens_per_call=0,
    ),
    "P3_S2_JUNCTION_NAMING": StepDefinition(
        step_id="P3_S2_JUNCTION_NAMING",
        phase=3,
        step_number="3.2",
        name="Junction Table Naming",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P3_S1_ER_COMPILATION"],
        avg_tokens_per_call=1500,
    ),
}


# ============================================================================
# Phase 4: Relational Schema Compilation
# ============================================================================

PHASE_4_STEPS: Dict[str, StepDefinition] = {
    "P4_S1_RELATIONAL_SCHEMA": StepDefinition(
        step_id="P4_S1_RELATIONAL_SCHEMA",
        phase=4,
        step_number="4.1",
        name="Relational Schema Compilation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P3_S2_JUNCTION_NAMING"],
        avg_tokens_per_call=0,
    ),
}


# ============================================================================
# Phase 5: Data Type Assignment
# ============================================================================

PHASE_5_STEPS: Dict[str, StepDefinition] = {
    "P5_S1_DEPENDENCY_GRAPH": StepDefinition(
        step_id="P5_S1_DEPENDENCY_GRAPH",
        phase=5,
        step_number="5.1",
        name="Attribute Dependency Graph",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P4_S1_RELATIONAL_SCHEMA"],
        avg_tokens_per_call=0,
    ),
    "P5_S2_INDEPENDENT_TYPES": StepDefinition(
        step_id="P5_S2_INDEPENDENT_TYPES",
        phase=5,
        step_number="5.2",
        name="Independent Attribute Data Types",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P5_S1_DEPENDENCY_GRAPH"],
        avg_tokens_per_call=2500,
    ),
    "P5_S3_FK_TYPES": StepDefinition(
        step_id="P5_S3_FK_TYPES",
        phase=5,
        step_number="5.3",
        name="Deterministic FK Data Types",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P5_S2_INDEPENDENT_TYPES"],
        avg_tokens_per_call=0,
    ),
    "P5_S4_DEPENDENT_TYPES": StepDefinition(
        step_id="P5_S4_DEPENDENT_TYPES",
        phase=5,
        step_number="5.4",
        name="Dependent Attribute Data Types",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P5_S3_FK_TYPES"],
        avg_tokens_per_call=2500,
    ),
    "P5_S5_NULLABILITY": StepDefinition(
        step_id="P5_S5_NULLABILITY",
        phase=5,
        step_number="5.5",
        name="Nullability Detection",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P5_S4_DEPENDENT_TYPES"],
        avg_tokens_per_call=1500,
    ),
}


# ============================================================================
# Phase 6: DDL Generation & Schema Creation
# ============================================================================

PHASE_6_STEPS: Dict[str, StepDefinition] = {
    "P6_S1_DDL_COMPILATION": StepDefinition(
        step_id="P6_S1_DDL_COMPILATION",
        phase=6,
        step_number="6.1",
        name="DDL Compilation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P5_S5_NULLABILITY"],
        avg_tokens_per_call=0,
    ),
    "P6_S2_DDL_VALIDATION": StepDefinition(
        step_id="P6_S2_DDL_VALIDATION",
        phase=6,
        step_number="6.2",
        name="DDL Validation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P6_S1_DDL_COMPILATION"],
        avg_tokens_per_call=0,
    ),
    "P6_S3_DDL_ERROR_CORRECTION": StepDefinition(
        step_id="P6_S3_DDL_ERROR_CORRECTION",
        phase=6,
        step_number="6.3",
        name="DDL Error Correction",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P6_S2_DDL_VALIDATION"],
        avg_tokens_per_call=3000,
    ),
    "P6_S4_SCHEMA_CREATION": StepDefinition(
        step_id="P6_S4_SCHEMA_CREATION",
        phase=6,
        step_number="6.4",
        name="Schema Creation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P6_S2_DDL_VALIDATION", "P6_S3_DDL_ERROR_CORRECTION"],
        avg_tokens_per_call=0,
    ),
    "P6_S5_SQL_GENERATION": StepDefinition(
        step_id="P6_S5_SQL_GENERATION",
        phase=6,
        step_number="6.5",
        name="SQL Query Generation",
        step_type=StepType.LLM,
        call_type=CallType.PER_INFORMATION_NEED,
        fanout_unit="information_need",
        can_parallelize=True,
        dependencies=["P6_S4_SCHEMA_CREATION"],
        avg_tokens_per_call=4000,
    ),
}




# ============================================================================
# Phase 7: Information Mining
# ============================================================================

PHASE_7_STEPS: Dict[str, StepDefinition] = {
    "P7_S1_INFORMATION_NEEDS": StepDefinition(
        step_id="P7_S1_INFORMATION_NEEDS",
        phase=7,
        step_number="7.1",
        name="Information Need Identification",
        step_type=StepType.LLM,
        call_type=CallType.LOOP,
        fanout_unit="",
        can_parallelize=False,
        is_loop=True,
        max_iters=10,
        dependencies=["P6_S4_SCHEMA_CREATION"],
        avg_tokens_per_call=3000,
    ),
    "P7_S2_SQL_VALIDATION": StepDefinition(
        step_id="P7_S2_SQL_VALIDATION",
        phase=7,
        step_number="7.2",
        name="SQL Generation and Validation",
        step_type=StepType.LLM,
        call_type=CallType.PER_INFORMATION_NEED,
        fanout_unit="information_need",
        can_parallelize=True,
        dependencies=["P7_S1_INFORMATION_NEEDS"],
        avg_tokens_per_call=4000,
    ),
}


# ============================================================================
# Phase 8: Functional Dependencies & Constraints
# ============================================================================

PHASE_8_STEPS: Dict[str, StepDefinition] = {
    "P8_S1_FUNCTIONAL_DEPENDENCIES": StepDefinition(
        step_id="P8_S1_FUNCTIONAL_DEPENDENCIES",
        phase=8,
        step_number="8.1",
        name="Functional Dependency Analysis",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P6_S4_SCHEMA_CREATION"],
        avg_tokens_per_call=3000,
    ),
    "P8_S2_CATEGORICAL_IDENTIFICATION": StepDefinition(
        step_id="P8_S2_CATEGORICAL_IDENTIFICATION",
        phase=8,
        step_number="8.2",
        name="Categorical Column Identification",
        step_type=StepType.LLM,
        call_type=CallType.PER_ENTITY,
        fanout_unit="entity",
        can_parallelize=True,
        dependencies=["P8_S1_FUNCTIONAL_DEPENDENCIES"],
        avg_tokens_per_call=1500,
    ),
    "P8_S3_CATEGORICAL_VALUES": StepDefinition(
        step_id="P8_S3_CATEGORICAL_VALUES",
        phase=8,
        step_number="8.3",
        name="Categorical Value Identification",
        step_type=StepType.LLM,
        call_type=CallType.PER_CATEGORICAL_ATTRIBUTE,
        fanout_unit="categorical_attribute",
        can_parallelize=True,
        dependencies=["P8_S2_CATEGORICAL_IDENTIFICATION"],
        avg_tokens_per_call=1500,
    ),
    "P8_S4_CONSTRAINT_DETECTION": StepDefinition(
        step_id="P8_S4_CONSTRAINT_DETECTION",
        phase=8,
        step_number="8.4",
        name="Constraint Detection",
        step_type=StepType.LLM,
        call_type=CallType.LOOP,
        fanout_unit="",
        can_parallelize=False,
        is_loop=True,
        max_iters=10,
        dependencies=["P8_S3_CATEGORICAL_VALUES"],
        avg_tokens_per_call=3000,
    ),
    "P8_S5_CONSTRAINT_SCOPE": StepDefinition(
        step_id="P8_S5_CONSTRAINT_SCOPE",
        phase=8,
        step_number="8.5",
        name="Constraint Scope Analysis",
        step_type=StepType.LLM,
        call_type=CallType.PER_CONSTRAINT,
        fanout_unit="constraint",
        can_parallelize=True,
        dependencies=["P8_S4_CONSTRAINT_DETECTION"],
        avg_tokens_per_call=2500,
    ),
    "P8_S6_CONSTRAINT_ENFORCEMENT": StepDefinition(
        step_id="P8_S6_CONSTRAINT_ENFORCEMENT",
        phase=8,
        step_number="8.6",
        name="Constraint Enforcement Strategy",
        step_type=StepType.LLM,
        call_type=CallType.PER_CONSTRAINT,
        fanout_unit="constraint",
        can_parallelize=True,
        dependencies=["P8_S5_CONSTRAINT_SCOPE"],
        avg_tokens_per_call=2500,
    ),
    "P8_S7_CONSTRAINT_CONFLICT": StepDefinition(
        step_id="P8_S7_CONSTRAINT_CONFLICT",
        phase=8,
        step_number="8.7",
        name="Constraint Conflict Detection",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P8_S6_CONSTRAINT_ENFORCEMENT"],
        avg_tokens_per_call=0,
    ),
    "P8_S8_CONSTRAINT_COMPILATION": StepDefinition(
        step_id="P8_S8_CONSTRAINT_COMPILATION",
        phase=8,
        step_number="8.8",
        name="Constraint Compilation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P8_S7_CONSTRAINT_CONFLICT"],
        avg_tokens_per_call=0,
    ),
}


# ============================================================================
# Phase 9: Generation Strategies
# ============================================================================

PHASE_9_STEPS: Dict[str, StepDefinition] = {
    "P9_S1_NUMERICAL_RANGE": StepDefinition(
        step_id="P9_S1_NUMERICAL_RANGE",
        phase=9,
        step_number="9.1",
        name="Numerical Range Definition",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P8_S8_CONSTRAINT_COMPILATION"],
        avg_tokens_per_call=2000,
    ),
    "P9_S2_TEXT_GEN_STRATEGY": StepDefinition(
        step_id="P9_S2_TEXT_GEN_STRATEGY",
        phase=9,
        step_number="9.2",
        name="Text Generation Strategy",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P9_S1_NUMERICAL_RANGE"],
        avg_tokens_per_call=1500,
    ),
    "P9_S3_BOOLEAN_DEPENDENCY": StepDefinition(
        step_id="P9_S3_BOOLEAN_DEPENDENCY",
        phase=9,
        step_number="9.3",
        name="Boolean Dependency Analysis",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P9_S2_TEXT_GEN_STRATEGY"],
        avg_tokens_per_call=1500,
    ),
    "P9_S4_DATA_VOLUME": StepDefinition(
        step_id="P9_S4_DATA_VOLUME",
        phase=9,
        step_number="9.4",
        name="Data Volume Specifications",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P1_S8_ENTITY_CARDINALITY"],
        avg_tokens_per_call=2000,
    ),
    "P9_S5_PARTITIONING": StepDefinition(
        step_id="P9_S5_PARTITIONING",
        phase=9,
        step_number="9.5",
        name="Partitioning Strategy",
        step_type=StepType.LLM,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P9_S4_DATA_VOLUME"],
        avg_tokens_per_call=2000,
    ),
    "P9_S6_DISTRIBUTION_COMPILATION": StepDefinition(
        step_id="P9_S6_DISTRIBUTION_COMPILATION",
        phase=9,
        step_number="9.6",
        name="Distribution Compilation",
        step_type=StepType.DETERMINISTIC,
        call_type=CallType.SINGULAR,
        fanout_unit="",
        can_parallelize=False,
        dependencies=["P9_S1_NUMERICAL_RANGE", "P9_S2_TEXT_GEN_STRATEGY", "P9_S3_BOOLEAN_DEPENDENCY", "P9_S5_PARTITIONING"],
        avg_tokens_per_call=0,
    ),
}


# ============================================================================
# Complete Registry
# ============================================================================

STEP_REGISTRY: Dict[str, StepDefinition] = {
    **PHASE_1_STEPS,
    **PHASE_2_STEPS,
    **PHASE_3_STEPS,
    **PHASE_4_STEPS,
    **PHASE_5_STEPS,
    **PHASE_6_STEPS,
    **PHASE_7_STEPS,
    **PHASE_8_STEPS,
    **PHASE_9_STEPS,
}


# ============================================================================
# Query Functions
# ============================================================================

def get_steps_by_phase(phase: int) -> List[StepDefinition]:
    """Get all steps for a phase, sorted by step_number."""
    return sorted(
        [s for s in STEP_REGISTRY.values() if s.phase == phase],
        key=lambda s: float(s.step_number)
    )


def get_llm_steps() -> List[StepDefinition]:
    """Get all LLM-powered steps."""
    return [s for s in STEP_REGISTRY.values() if s.step_type == StepType.LLM]


def get_deterministic_steps() -> List[StepDefinition]:
    """Get all deterministic steps."""
    return [s for s in STEP_REGISTRY.values() if s.step_type == StepType.DETERMINISTIC]


def get_step_by_id(step_id: str) -> Optional[StepDefinition]:
    """Get step definition by stable step_id."""
    return STEP_REGISTRY.get(step_id)


def get_step_by_number(phase: int, step_number: str) -> Optional[StepDefinition]:
    """Get step definition by phase and step_number."""
    for step in STEP_REGISTRY.values():
        if step.phase == phase and step.step_number == step_number:
            return step
    return None

