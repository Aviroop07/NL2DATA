"""Phase 2, Step 2.1: Attribute Count Detection.

Checks if the description specifies attribute counts or column names.
Helps validate completeness and identify explicitly mentioned attributes.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class AttributeCountOutput(BaseModel):
    """Output structure for attribute count detection."""
    has_explicit_count: bool = Field(description="Whether the description explicitly mentions an attribute count")
    count: Optional[int] = Field(default=None, description="Explicit count if mentioned, null otherwise")
    explicit_attributes: List[str] = Field(
        default_factory=list,
        description="List of attribute names explicitly mentioned in the description"
    )
    explicit_column_names: List[str] = Field(
        default_factory=list,
        description="List of column names explicitly mentioned (may differ from attribute names)"
    )


@traceable_step("2.1", phase=2, tags=['phase_2_step_1'])
@traceable_step("2.1", phase=2, tags=["attribute_count_detection"])
async def step_2_1_attribute_count_detection(
    entity_name: str,
    nl_description: str,
    entity_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.1 (per-entity): Check if description specifies attribute counts or column names.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        nl_description: Natural language description of the database requirements
        entity_description: Optional description of the entity
        
    Returns:
        dict: Attribute count detection result with has_explicit_count, count, explicit_attributes, and explicit_column_names
        
    Example:
        >>> result = await step_2_1_attribute_count_detection(
        ...     "Customer",
        ...     "Customers have name, email, and address"
        ... )
        >>> result["has_explicit_count"]
        False
        >>> len(result["explicit_attributes"])
        3
    """
    logger.debug(f"Detecting attribute count for entity: {entity_name}")
    
    # Build context
    context_msg = ""
    if entity_description:
        context_msg = f"\n\nEntity description: {entity_description}"
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to identify if a natural language description explicitly mentions attribute counts or specific attribute/column names for an entity.

Look for:
1. **Explicit counts**: Numbers that specify how many attributes an entity should have
   - Examples: "Customer has 5 attributes", "Order table with 10 columns", "Product entity with 3 fields"
   
2. **Explicit attribute names**: Specific attribute names mentioned in the description
   - Examples: "Customer has name, email, address", "Order contains order_id, order_date, total_amount"
   - These may be mentioned as attributes, columns, fields, or properties
   
3. **Explicit column names**: SQL column names (may differ from attribute names)
   - Examples: "customer_name", "order_date", "product_id"
   - May use different naming conventions (snake_case, camelCase, etc.)

Important:
- Distinguish between explicit mentions and inferred attributes
- If an attribute is explicitly named, include it in explicit_attributes
- If a column name is explicitly mentioned (different from attribute name), include it in explicit_column_names
- If a count is explicitly stated (e.g., "5 attributes"), set has_explicit_count=True and provide the count

Provide:
- has_explicit_count: Whether a count is explicitly mentioned
- count: The explicit count if mentioned, null otherwise
- explicit_attributes: List of attribute names explicitly mentioned
- explicit_column_names: List of column names explicitly mentioned (if different from attribute names)"""
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}{context_msg}

Natural language description:
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("2.1")  # Step 2.1 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("2.1", phase=2, tags=["attribute_count_detection"])
        result: AttributeCountOutput = await standardized_llm_call(
            llm=llm,
            output_schema=AttributeCountOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            config=config,
        )
        
        # Work with Pydantic model directly
        logger.debug(
            f"Entity {entity_name}: has_explicit_count={result.has_explicit_count}, "
            f"count={result.count}, "
            f"explicit_attributes={len(result.explicit_attributes)}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error detecting attribute count for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_1_attribute_count_detection_batch(
    entities: List[Dict[str, Any]],
    nl_description: str,
) -> Dict[str, Any]:
    """
    Step 2.1: Detect attribute counts for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        nl_description: Natural language description
        
    Returns:
        dict: Attribute count detection results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_1_attribute_count_detection_batch(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     nl_description="Customers have name and email. Orders have order_id and date."
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.1: Attribute Count Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for attribute count detection")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        
        task = step_2_1_attribute_count_detection(
            entity_name=entity_name,
            nl_description=nl_description,
            entity_description=entity_desc,
        )
        tasks.append((entity_name, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    entity_results = {}
    for i, ((entity_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results[entity_name] = {
                "has_explicit_count": False,
                "count": None,
                "explicit_attributes": [],
                "explicit_column_names": [],
            }
        else:
            entity_results[entity_name] = result
    
    logger.info(f"Attribute count detection completed for {len(entity_results)} entities")
    
    return {"entity_results": entity_results}

