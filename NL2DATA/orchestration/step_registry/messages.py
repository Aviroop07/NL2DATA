"""Status message templates for all steps.

This module contains human-readable status messages for each step in the pipeline.
Messages are keyed by step_id and can include placeholders like {entity} for
dynamic content based on the step's scope.
"""

# Message templates per step - comprehensive mapping for all phases
# Maps step_id from registry to human-readable messages
STEP_MESSAGE_TEMPLATES = {
    # Phase 1: Domain & Entity Discovery
    "P1_S1_DOMAIN_DETECTION": "Detecting the primary domain from the description",
    "P1_S2_ENTITY_MENTION": "Finding explicitly mentioned entities",
    "P1_S3_DOMAIN_INFERENCE": "Inferring domain context and nuances",
    "P1_S4_KEY_ENTITY_EXTRACTION": "Extracting key entities for the schema",
    "P1_S5_RELATION_MENTION": "Identifying explicitly stated relations",
    "P1_S6_AUXILIARY_ENTITIES": "Suggesting auxiliary/supporting entities",
    "P1_S7_ENTITY_CONSOLIDATION": "Consolidating and deduplicating entities",
    "P1_S7B_ENTITY_RECLASSIFICATION": "Reclassifying entities vs relations",
    "P1_S8_ENTITY_CARDINALITY": "Determining cardinality and table type for {entity}",
    "P1_S9_KEY_RELATIONS": "Extracting key relations among entities",
    "P1_S10_SCHEMA_CONNECTIVITY": "Validating overall schema connectivity",
    "P1_S11_RELATION_CARDINALITY": "Determining relation cardinality for {relation}",
    "P1_S12_RELATION_VALIDATION": "Validating extracted relations",
    
    # Phase 2: Attribute Discovery & Schema Design
    "P2_S1_ATTRIBUTE_COUNT": "Counting candidate attributes for {entity}",
    "P2_S2_INTRINSIC_ATTRIBUTES": "Identifying intrinsic attributes for {entity}",
    "P2_S3_ATTRIBUTE_SYNONYM": "Detecting attribute synonyms for {entity}",
    "P2_S4_COMPOSITE_ATTRIBUTES": "Handling composite attributes for {entity}",
    "P2_S5_TEMPORAL_ATTRIBUTES": "Identifying temporal attributes for {entity}",
    "P2_S6_NAMING_VALIDATION": "Validating attribute naming for {entity}",
    "P2_S7_PRIMARY_KEY": "Identifying primary keys for {entity}",
    "P2_S8_MULTIVALUED_DERIVED": "Handling multivalued/derived attributes for {entity}",
    "P2_S9_DERIVED_FORMULAS": "Defining formulas for derived attribute {attribute} in {entity}",
    "P2_S10_UNIQUE_CONSTRAINTS": "Identifying unique constraints for {entity}",
    "P2_S11_NULLABILITY": "Determining nullability for {entity}",
    "P2_S12_DEFAULT_VALUES": "Defining default values for {entity}",
    "P2_S13_CHECK_CONSTRAINTS": "Defining check constraints for {entity}",
    "P2_S14_RELATION_REALIZATION": "Realizing relation {relation} as foreign keys",
    
    # Phase 3: Query Requirements & Schema Refinement
    "P3_S1_INFORMATION_NEEDS": "Identifying information needs",
    "P3_S2_INFORMATION_COMPLETENESS": "Checking completeness for {information_need}",
    "P3_S3_PHASE2_REEXECUTION": "Re-executing Phase 2 refinements for {entity}",
    "P3_S4_ER_COMPILATION": "Compiling the ER diagram",
    "P3_S5_RELATIONAL_SCHEMA": "Generating the relational schema",
    
    # Phase 4: Functional Dependencies & Data Types
    "P4_S1_FUNCTIONAL_DEPENDENCIES": "Identifying functional dependencies for {entity}",
    "P4_S2_3NF_NORMALIZATION": "Applying 3NF normalization",
    "P4_S3_DATA_TYPE_ASSIGNMENT": "Assigning data types for {entity}",
    "P4_S4_CATEGORICAL_DETECTION": "Detecting categorical attributes for {entity}",
    "P4_S5_CHECK_CONSTRAINT_DETECTION": "Detecting check constraints for {attribute}",
    "P4_S6_CATEGORICAL_VALUES": "Identifying categorical values for {attribute}",
    "P4_S7_CATEGORICAL_DISTRIBUTION": "Defining categorical distributions for {attribute}",
    
    # Phase 5: DDL & SQL Generation
    "P5_S1_DDL_COMPILATION": "Compiling DDL statements",
    "P5_S2_DDL_VALIDATION": "Validating DDL syntax",
    "P5_S3_DDL_ERROR_CORRECTION": "Correcting DDL errors",
    "P5_S4_SCHEMA_CREATION": "Creating database schema",
    "P5_S5_SQL_GENERATION": "Generating SQL query for {information_need}",
    
    # Phase 6: Constraints & Distributions
    "P6_S1_CONSTRAINT_DETECTION": "Detecting constraints",
    "P6_S2_CONSTRAINT_SCOPE": "Determining scope for constraint {constraint}",
    "P6_S3_CONSTRAINT_ENFORCEMENT": "Enforcing constraint {constraint}",
    "P6_S4_CONSTRAINT_CONFLICT": "Resolving conflicts for constraint {constraint}",
    "P6_S5_CONSTRAINT_COMPILATION": "Compiling constraint specifications",
    
    # Phase 7: Generation Strategies
    "P7_S1_NUMERICAL_RANGE": "Defining numerical range for {attribute} in {entity}",
    "P7_S2_TEXT_GEN_STRATEGY": "Defining text generation strategy for {attribute} in {entity}",
    "P7_S3_BOOLEAN_DEPENDENCY": "Defining boolean dependencies for {attribute} in {entity}",
    "P7_S4_DATA_VOLUME": "Estimating data volume",
    "P7_S5_PARTITIONING": "Defining partitioning strategy for {entity}",
    "P7_S6_DISTRIBUTION_COMPILATION": "Compiling distribution strategies",
}


def get_step_message(step_id: str, scope: dict = None) -> str:
    """Get human-readable message for a step.
    
    Args:
        step_id: Step identifier (e.g., "P1_S1_DOMAIN_DETECTION")
        scope: Optional scope dictionary with entity/attribute/relation/information_need/constraint info
        
    Returns:
        Human-readable status message for the step with placeholders filled in
    """
    template = STEP_MESSAGE_TEMPLATES.get(step_id, "Processing")
    
    if not scope:
        return template
    
    # Build format kwargs from scope, only including keys that exist in the template
    format_kwargs = {}
    
    # Check which placeholders exist in template and fill them from scope
    if "{entity}" in template and scope.get("entity"):
        format_kwargs["entity"] = scope["entity"]
    
    if "{relation}" in template and scope.get("relation"):
        format_kwargs["relation"] = scope["relation"]
    
    if "{attribute}" in template and scope.get("attribute"):
        format_kwargs["attribute"] = scope["attribute"]
    
    if "{information_need}" in template and scope.get("information_need"):
        format_kwargs["information_need"] = scope["information_need"]
    
    if "{constraint}" in template and scope.get("constraint"):
        format_kwargs["constraint"] = scope["constraint"]
    
    # Only format if we have at least one matching placeholder
    if format_kwargs:
        try:
            return template.format(**format_kwargs)
        except KeyError:
            # If formatting fails (missing placeholder), return template as-is
            return template
    
    return template

