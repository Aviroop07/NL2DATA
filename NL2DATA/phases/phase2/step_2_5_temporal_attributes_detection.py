"""Phase 2, Step 2.5: Temporal Attributes Detection.

Determines if entities need temporal attributes (created_at, updated_at timestamps).
Common pattern in modern applications for audit trails.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class TemporalAttributesOutput(BaseModel):
    """Output structure for temporal attribute detection."""
    needs_temporal: bool = Field(description="Whether the entity needs temporal attributes")
    temporal_attributes: List[str] = Field(
        default_factory=list,
        description="List of temporal attribute names to add (e.g., ['created_at', 'updated_at'])"
    )
    reasoning: str = Field(description="Reasoning for temporal attribute decision")


@traceable_step("2.5", phase=2, tags=['phase_2_step_5'])
async def step_2_5_temporal_attributes_detection(
    entity_name: str,
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    existing_attributes: Optional[List[str]] = None,
) -> TemporalAttributesOutput:
    """
    Step 2.5 (per-entity): Determine if entity needs temporal attributes.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        existing_attributes: Optional list of existing attribute names to check for conflicts
        
    Returns:
        dict: Temporal attribute detection result with needs_temporal, temporal_attributes, and reasoning
        
    Example:
        >>> result = await step_2_5_temporal_attributes_detection(
        ...     "Customer"
        ... )
        >>> result["needs_temporal"]
        True
        >>> "created_at" in result["temporal_attributes"]
        True
    """
    logger.debug(f"Detecting temporal attributes for entity: {entity_name}")
    
    # Build context
    context_parts = []
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    if existing_attributes:
        context_parts.append(f"Existing attributes: {', '.join(existing_attributes)}")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=TemporalAttributesOutput,
        additional_requirements=[
            "The \"reasoning\" field is REQUIRED and cannot be omitted or empty",
            "Check if temporal attributes already exist in the attribute list - don't suggest duplicates"
        ]
    )
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to determine if an entity needs temporal attributes (timestamps) for audit trails and tracking.

Temporal attributes are timestamps that track when records are created or modified. Common patterns:
- **created_at**: Timestamp when the record was first created
- **updated_at**: Timestamp when the record was last modified
- **deleted_at**: Timestamp when the record was soft-deleted (optional, for soft deletes)

When to add temporal attributes:
- **Most entities benefit** from created_at and updated_at for audit trails
- **Transactional entities** (orders, transactions, events) almost always need temporal attributes
- **Reference data** (categories, configurations) may not need them
- **If already present**: Check if temporal attributes are already in the attribute list
 
Common temporal attribute names:
- created_at, created_date, date_created, created_timestamp
- updated_at, updated_date, date_updated, updated_timestamp, modified_at
- deleted_at, deleted_date (for soft deletes)

Important:
- Check if temporal attributes already exist in the attribute list
- If they exist, don't suggest duplicates
- Consider the entity type: transactional entities typically need temporal attributes
- Consider domain patterns: modern applications often include audit trails

""" + output_structure_section
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}{context_msg}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model and create chain
    llm = get_model_for_step("2.5")  # Step 2.5 maps to "high_fanout" task type
    try:
        config = get_trace_config("2.5", phase=2, tags=["phase_2_step_5"])
        result: TemporalAttributesOutput = await standardized_llm_call(
            llm=llm,
            output_schema=TemporalAttributesOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description or ""},
            config=config,
        )
        
        # Work with Pydantic model directly
        needs_temporal = result.needs_temporal
        temporal_count = len(result.temporal_attributes)
        
        logger.debug(
            f"Entity {entity_name}: needs_temporal={needs_temporal}, "
            f"temporal_attributes={result.temporal_attributes}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error detecting temporal attributes for entity {entity_name}: {e}", exc_info=True)
        raise


class EntityTemporalAttributesResult(BaseModel):
    """Result for a single entity in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    needs_temporal: bool = Field(description="Whether the entity needs temporal attributes")
    temporal_attributes: List[str] = Field(
        default_factory=list,
        description="List of temporal attribute names to add"
    )
    reasoning: str = Field(description="Reasoning for temporal attribute decision")


class TemporalAttributesBatchOutput(BaseModel):
    """Output structure for Step 2.5 batch processing."""
    entity_results: List[EntityTemporalAttributesResult] = Field(
        description="List of temporal attribute detection results, one per entity"
    )
    total_entities: int = Field(description="Total number of entities processed")


async def step_2_5_temporal_attributes_detection_batch(
    entities: List,
    entity_attributes: Optional[dict] = None,  # entity_name -> attribute list
    nl_description: Optional[str] = None,
) -> TemporalAttributesBatchOutput:
    """
    Step 2.5: Detect temporal attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities
        entity_attributes: Optional dictionary mapping entity names to their attribute lists
        nl_description: Optional original NL description
        
    Returns:
        TemporalAttributesBatchOutput: Temporal attribute detection results for all entities
        
    Example:
        >>> result = await step_2_5_temporal_attributes_detection_batch(
        ...     entities=[{"name": "Customer"}]
        ... )
        >>> len(result.entity_results)
        1
    """
    logger.info(f"Starting Step 2.5: Temporal Attributes Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for temporal attribute detection")
        return TemporalAttributesBatchOutput(entity_results=[], total_entities=0)
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    task_metadata = []  # Store entity_name for each task
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        existing_attrs = entity_attributes.get(entity_name, []) if entity_attributes else None
        
        task = step_2_5_temporal_attributes_detection(
            entity_name=entity_name,
            nl_description=nl_description,
            entity_description=entity_desc,
            existing_attributes=existing_attrs,
        )
        tasks.append(task)
        task_metadata.append(entity_name)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )
    
    # Process results
    entity_results_list = []
    for entity_name, result in zip(task_metadata, results):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results_list.append(
                EntityTemporalAttributesResult(
                    entity_name=entity_name,
                    needs_temporal=False,
                    temporal_attributes=[],
                    reasoning=f"Error during analysis: {str(result)}"
                )
            )
        else:
            entity_results_list.append(
                EntityTemporalAttributesResult(
                    entity_name=entity_name,
                    needs_temporal=result.needs_temporal,
                    temporal_attributes=result.temporal_attributes,
                    reasoning=result.reasoning,
                )
            )
    
    total_temporal = sum(len(r.temporal_attributes) for r in entity_results_list)
    logger.info(f"Temporal attribute detection completed: {total_temporal} temporal attributes added across {len(entity_results_list)} entities")
    
    return TemporalAttributesBatchOutput(
        entity_results=entity_results_list,
        total_entities=len(entities),
    )

