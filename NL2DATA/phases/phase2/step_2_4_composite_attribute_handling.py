"""Phase 2, Step 2.4: Composite Attribute Handling.

For composite attributes like "address", determines if they should be one field
or decomposed (street, city, zip). Affects normalization and queryability.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class CompositeAttributeInfo(BaseModel):
    """Information about a composite attribute."""
    name: str = Field(description="Name of the composite attribute")
    should_decompose: bool = Field(description="Whether this attribute should be decomposed into sub-attributes")
    decomposition: Optional[List[str]] = Field(
        default=None,
        description="List of sub-attribute names if should_decompose is True (e.g., ['street', 'city', 'zip'] for 'address')"
    )
    reasoning: str = Field(description="Reasoning for decomposition decision")


class CompositeAttributeOutput(BaseModel):
    """Output structure for composite attribute handling."""
    composite_attributes: List[CompositeAttributeInfo] = Field(
        default_factory=list,
        description="List of composite attributes with decomposition decisions"
    )


@traceable_step("2.4", phase=2, tags=['phase_2_step_4'])
async def step_2_4_composite_attribute_handling(
    entity_name: str,
    attributes: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.4 (per-entity): Determine if composite attributes should be decomposed.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of attributes from Step 2.3 (final attribute list)
        nl_description: Optional original NL description for context
        
    Returns:
        dict: Composite attribute handling result with composite_attributes list
        
    Example:
        >>> result = await step_2_4_composite_attribute_handling(
        ...     "Customer",
        ...     [{"name": "address"}]
        ... )
        >>> result["composite_attributes"][0]["should_decompose"]
        True
        >>> result["composite_attributes"][0]["decomposition"]
        ["street", "city", "zip"]
    """
    logger.debug(f"Handling composite attributes for entity: {entity_name}")
    
    if not attributes:
        logger.debug(f"No attributes provided for entity {entity_name}")
        return {"composite_attributes": []}
    
    # Build attribute list for prompt
    attribute_list_str = ""
    for i, attr in enumerate(attributes, 1):
        attr_name = attr if isinstance(attr, str) else (attr.get("name", "Unknown") if isinstance(attr, dict) else getattr(attr, "name", "Unknown"))
        attribute_list_str += f"{i}. {attr_name}\n"
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to identify composite attributes and determine if they should be decomposed into sub-attributes.

A composite attribute is an attribute that can be broken down into multiple simpler attributes. Examples:
- **address** → street, city, state, zip_code, country
- **name** → first_name, last_name (or full_name kept as one)
- **phone** → area_code, phone_number (or phone kept as one)
- **date** → year, month, day (or date kept as one)

Decomposition decision factors:
1. **Queryability**: Will users need to query by sub-components? (e.g., "find customers in California" → decompose address)
2. **Normalization**: Does decomposition help with normalization? (e.g., zip_code → city dependency)
3. **Simplicity**: Is the composite simple enough to keep as one? (e.g., "full_name" might stay as one field)
4. **Domain patterns**: What's standard in this domain? (e.g., addresses are usually decomposed)

For each composite attribute, provide:
- name: The composite attribute name
- should_decompose: Whether to decompose (true) or keep as one field (false)
- decomposition: List of sub-attribute names if should_decompose is True (e.g., ["street", "city", "zip"] for address)
- reasoning: REQUIRED - Clear explanation of the decision (cannot be omitted)

Important:
- Only identify attributes that are truly composite (can be broken down)
- Consider query needs: if users will filter/sort by sub-components, decompose
- Consider normalization: if decomposition helps avoid redundancy, decompose
- Be practical: don't over-decompose simple attributes"""
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}

Attributes to check:
{{attribute_list}}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("2.4")  # Step 2.4 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("2.4", phase=2, tags=["phase_2_step_4"])
        result: CompositeAttributeOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CompositeAttributeOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "attribute_list": attribute_list_str,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        composite_count = len(result.composite_attributes)
        
        logger.debug(
            f"Entity {entity_name}: {composite_count} composite attributes identified"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error handling composite attributes for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_4_composite_attribute_handling_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> final attribute list (strings)
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.4: Handle composite attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities
        entity_attributes: Dictionary mapping entity names to their final attribute lists from Step 2.3
        nl_description: Optional original NL description
        
    Returns:
        dict: Composite attribute handling results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_4_composite_attribute_handling_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": ["name", "address", "email"]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.4: Composite Attribute Handling for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for composite attribute handling")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        attributes = entity_attributes.get(entity_name, [])
        
        # Convert string list to dict list for compatibility
        attr_dicts = [{"name": attr} if isinstance(attr, str) else attr for attr in attributes]
        
        task = step_2_4_composite_attribute_handling(
            entity_name=entity_name,
            attributes=attr_dicts,
            nl_description=nl_description,
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
            entity_results[entity_name] = {"composite_attributes": []}
        else:
            entity_results[entity_name] = result
    
    total_composite = sum(len(r.get("composite_attributes", [])) for r in entity_results.values())
    logger.info(f"Composite attribute handling completed: {total_composite} composite attributes identified across {len(entity_results)} entities")
    
    return {"entity_results": entity_results}

