"""Phase 2, Step 2.8: Multivalued/Derived Detection.

Classify existing attributes (from Step 2.2) as multivalued or derived.
No new attribute mining - only classification of existing attributes.
Derived attributes can only depend on attributes within the same entity (no cross-entity dependencies).
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class MultivaluedHandlingInfo(BaseModel):
    """Information about how a multivalued attribute should be handled."""
    attribute_name: str = Field(description="Name of the multivalued attribute")
    strategy: str = Field(
        description="Handling strategy: 'separate_table' (create junction table), 'array' (use array type), or 'json' (store as JSON)"
    )


class MultivaluedDerivedOutput(BaseModel):
    """Output structure for Step 2.8: Multivalued/Derived Detection.
    
    This step only classifies existing attributes - it does not handle implementation details.
    """
    multivalued: List[str] = Field(
        default_factory=list,
        description="List of attribute names that are multivalued (can have multiple values)"
    )
    derived: List[str] = Field(
        default_factory=list,
        description="List of attribute names that are derived (calculated from other attributes in the SAME entity only)"
    )
    multivalued_handling: List[MultivaluedHandlingInfo] = Field(
        default_factory=list,
        description=(
            "List of multivalued attribute handling strategies. "
            "Only include entries for attributes listed in 'multivalued'."
        )
    )
    reasoning: str = Field(
        description="Reasoning for the multivalued/derived classification"
    )


class EntityMultivaluedDerivedResult(BaseModel):
    """Result for a single entity in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    multivalued: List[str] = Field(
        default_factory=list,
        description="List of attribute names that are multivalued"
    )
    derived: List[str] = Field(
        default_factory=list,
        description="List of attribute names that are derived"
    )
    multivalued_handling: List[MultivaluedHandlingInfo] = Field(
        default_factory=list,
        description="List of multivalued attribute handling strategies"
    )
    reasoning: str = Field(description="Reasoning for the classification")


class MultivaluedDerivedBatchOutput(BaseModel):
    """Output structure for Step 2.8 batch processing."""
    entity_results: List[EntityMultivaluedDerivedResult] = Field(
        description="List of multivalued/derived detection results, one per entity"
    )
    total_entities: int = Field(
        description="Total number of entities processed"
    )


@traceable_step("2.8", phase=2, tags=['phase_2_step_8'])
async def step_2_8_multivalued_derived_detection(
    entity_name: str,
    entity_description: Optional[str],
    entity_attributes: List[str],  # List of attribute names from Step 2.2
    primary_key: List[str],  # Primary key from Step 2.7
    nl_description: str,
    domain: Optional[str] = None,
) -> MultivaluedDerivedOutput:
    """
    Step 2.8 (per-entity, LLM): Classify existing attributes as multivalued or derived.
    
    IMPORTANT:
    - Only classifies attributes that already exist (from Step 2.2)
    - Does NOT mine for new attributes
    - Derived attributes can ONLY depend on attributes within the same entity
    - No cross-entity dependencies allowed for derived attributes
    
    Args:
        entity_name: Name of the entity
        entity_description: Description of the entity
        entity_attributes: List of attribute names for this entity (from Step 2.2)
        primary_key: Primary key attributes (from Step 2.7)
        nl_description: Original natural language description
        domain: Optional domain context
        
    Returns:
        dict: Multivalued/derived classification result with multivalued, derived, multivalued_handling, reasoning
        
    Example:
        >>> result = await step_2_8_multivalued_derived_detection("Customer", {...}, ["phone_numbers"], ["customer_id"], "...")
        >>> "multivalued" in result
        True
    """
    logger.debug(f"Classifying multivalued/derived attributes for entity: {entity_name}")
    
    # Exclude primary key attributes from being classified as multivalued or derived
    non_pk_attributes = [attr for attr in entity_attributes if attr not in primary_key]
    
    if not non_pk_attributes:
        logger.debug(f"No non-PK attributes to classify for {entity_name}")
        return MultivaluedDerivedOutput(
            multivalued=[],
            derived=[],
            multivalued_handling=[],
            reasoning="No non-primary-key attributes to classify",
        )
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=MultivaluedDerivedOutput,
        additional_requirements=[
            "Only classify attributes that are already in the provided list - DO NOT suggest new attributes",
            "Derived attributes can ONLY depend on attributes within the SAME entity",
            "NO cross-entity dependencies are allowed for derived attributes",
            "Primary key attributes are excluded from classification (already provided separately)",
        ]
    )
    
    system_prompt = f"""You are a database schema analyst. Classify existing attributes as multivalued or derived.

IMPORTANT CONSTRAINTS:
1. Only classify attributes that are already in the provided list - DO NOT suggest new attributes
2. Derived attributes can ONLY depend on attributes within the SAME entity
3. NO cross-entity dependencies are allowed for derived attributes
4. Primary key attributes are excluded from classification (already provided separately)

Multivalued attributes:
- Can have multiple values for a single entity instance
- Examples: phone_numbers, addresses, tags, categories
- Need special handling (separate table, array, JSON)

Derived attributes:
- Calculated from other attributes in the SAME entity only
- Examples: total_price (quantity * unit_price), age (current_date - birth_date)
- Must only reference attributes from the same entity
- Cannot reference attributes from other entities

{output_structure_section}"""
    
    human_prompt = f"""Entity: {entity_name}
Description: {entity_description or "Not provided"}

Primary Key: {', '.join(primary_key) if primary_key else "None"}

Existing Attributes (from Step 2.2): {', '.join(non_pk_attributes)}

Natural Language Description:
{nl_description}

Domain: {domain or "Not specified"}

Classify which of the EXISTING attributes are:
1. Multivalued (can have multiple values)
2. Derived (calculated from other attributes in the SAME entity only)

Remember:
- Only classify attributes from the provided list
- Derived attributes can only depend on same-entity attributes
- Do not suggest new attributes"""
    
    llm = get_model_for_step("2.8")
    trace_config = get_trace_config("2.8", phase=2, tags=["phase_2_step_8"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=MultivaluedDerivedOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    # Validate that all classified attributes are in the original list
    classified_attrs = set(result.multivalued + result.derived)
    invalid_attrs = classified_attrs - set(non_pk_attributes)
    if invalid_attrs:
        logger.warning(
            f"Step 2.8 for {entity_name}: LLM classified attributes not in original list: {invalid_attrs}. "
            f"Removing them from classification."
        )
        result.multivalued = [a for a in result.multivalued if a in non_pk_attributes]
        result.derived = [a for a in result.derived if a in non_pk_attributes]
        # Filter multivalued_handling to only include valid attributes
        result.multivalued_handling = [
            h for h in result.multivalued_handling 
            if h.attribute_name in non_pk_attributes
        ]
    
    return result


async def step_2_8_multivalued_derived_detection_batch(
    entities: List,  # List of entities (can be dicts or objects with name/description)
    entity_attributes: dict,  # entity_name -> list of attribute names
    primary_keys: dict,  # entity_name -> list of PK attribute names
    nl_description: str,
    domain: Optional[str] = None,
) -> MultivaluedDerivedBatchOutput:
    """
    Step 2.8: Classify multivalued/derived attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities
        entity_attributes: Dictionary mapping entity names to their attribute name lists (from Step 2.2)
        primary_keys: Dictionary mapping entity names to their primary key lists (from Step 2.7)
        nl_description: Original natural language description
        domain: Optional domain context
        
    Returns:
        MultivaluedDerivedBatchOutput: Batch classification results
    """
    logger.info(f"Starting Step 2.8: Multivalued/Derived Detection for {len(entities)} entities")
    
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else None
        attrs = entity_attributes.get(entity_name, [])
        pk = primary_keys.get(entity_name, [])
        
        tasks.append(
            step_2_8_multivalued_derived_detection(
                entity_name=entity_name,
                entity_description=entity_desc,
                entity_attributes=attrs,
                primary_key=pk,
                nl_description=nl_description,
                domain=domain,
            )
        )
    
    results = await asyncio.gather(*tasks)
    
    # Convert results to batch output format with entity names
    entity_results_list = []
    for i, entity in enumerate(entities):
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        result = results[i]
        entity_results_list.append(
            EntityMultivaluedDerivedResult(
                entity_name=entity_name,
                multivalued=result.multivalued,
                derived=result.derived,
                multivalued_handling=result.multivalued_handling,
                reasoning=result.reasoning,
            )
        )
    
    return MultivaluedDerivedBatchOutput(
        entity_results=entity_results_list,
        total_entities=len(entities),
    )
