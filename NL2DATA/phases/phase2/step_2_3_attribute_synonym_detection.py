"""Phase 2, Step 2.3: Attribute Synonym Detection.

Checks for duplicate or synonymous attributes (e.g., "email" and "email_address").
Prevents redundant columns and schema bloat.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class AttributeSynonymInfo(BaseModel):
    """Information about a potential synonym pair."""
    attr1: str = Field(description="Name of first attribute")
    attr2: str = Field(description="Name of second attribute")
    should_merge: bool = Field(description="Whether these attributes should be merged")
    preferred_name: str = Field(description="Preferred name if merging (canonical name)")
    reasoning: str = Field(description="Reasoning for merge decision")


class AttributeSynonymOutput(BaseModel):
    """Output structure for attribute synonym detection."""
    synonyms: List[AttributeSynonymInfo] = Field(
        default_factory=list,
        description="List of synonym pairs with merge decisions"
    )
    merged_attributes: List[str] = Field(
        default_factory=list,
        description="List of attribute names that should be removed after merging (kept attributes are in final_attribute_list)"
    )
    final_attribute_list: List[str] = Field(
        default_factory=list,
        description="Final consolidated list of unique attribute names after merging"
    )


@traceable_step("2.3", phase=2, tags=['phase_2_step_3'])
async def step_2_3_attribute_synonym_detection(
    entity_name: str,
    attributes: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.3 (per-entity): Check for duplicate or synonymous attributes.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of attributes from Step 2.2
        nl_description: Optional original NL description for context
        
    Returns:
        dict: Synonym detection result with synonyms, merged_attributes, and final_attribute_list
        
    Example:
        >>> result = await step_2_3_attribute_synonym_detection(
        ...     "Customer",
        ...     [{"name": "email"}, {"name": "email_address"}]
        ... )
        >>> result["synonyms"][0]["should_merge"]
        True
    """
    logger.debug(f"Detecting attribute synonyms for entity: {entity_name}")
    
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}")
        return {
            "synonyms": [],
            "merged_attributes": [],
            "final_attribute_list": []
        }
    
    # Build attribute list for prompt
    attribute_list_str = ""
    for i, attr in enumerate(attributes, 1):
        attr_name = attr.get("name", "Unknown") if isinstance(attr, dict) else getattr(attr, "name", "Unknown")
        attr_desc = attr.get("description", "") if isinstance(attr, dict) else getattr(attr, "description", "")
        attribute_list_str += f"{i}. {attr_name}"
        if attr_desc:
            attribute_list_str += f": {attr_desc}"
        attribute_list_str += "\n"
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to identify duplicate attributes, synonyms, or attributes that should be merged to prevent schema bloat and confusion.

Look for:
1. **Exact duplicates**: Same attribute name appearing multiple times
2. **Synonyms**: Different names referring to the same concept
   - Examples: "email" vs "email_address", "name" vs "full_name", "phone" vs "phone_number"
3. **Overlapping attributes**: Attributes that represent the same or very similar concepts
   - Examples: "address" and "mailing_address", "created" and "created_at"
4. **Attributes that should be merged**: Attributes that are better represented as a single attribute

For each potential duplicate or merge candidate, provide:
- attr1: Name of first attribute
- attr2: Name of second attribute
- should_merge: Whether these should be merged (true) or kept separate (false)
- preferred_name: The canonical name to use if merging (usually the more standard/clear name)
- reasoning: REQUIRED - Clear explanation of why merge or keep separate (cannot be omitted)

After identifying duplicates, provide:
- merged_attributes: List of attribute names that should be removed (the non-preferred names)
- final_attribute_list: Complete list of unique attribute names after consolidation (preferred names only)

Important:
- If two attributes are synonyms, merge them and keep the more standard/canonical name
- If attributes are related but distinct, keep them separate
- Consider domain context when making decisions
- Be conservative: only merge if truly necessary
- Prefer shorter, clearer names when merging"""
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}

Attributes to check:
{{attribute_list}}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model and create chain
    llm = get_model_for_step("2.3")  # Step 2.3 maps to "high_fanout" task type
    try:
        config = get_trace_config("2.3", phase=2, tags=["phase_2_step_3"])
        result: AttributeSynonymOutput = await standardized_llm_call(
            llm=llm,
            output_schema=AttributeSynonymOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "attribute_list": attribute_list_str,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        synonym_count = len(result.synonyms)
        merged_count = len(result.merged_attributes)
        final_count = len(result.final_attribute_list)
        
        logger.debug(
            f"Entity {entity_name}: {synonym_count} synonyms found, {merged_count} attributes merged, "
            f"{final_count} final attributes"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error detecting attribute synonyms for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_3_attribute_synonym_detection_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> attributes
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.3: Detect attribute synonyms for all entities (parallel execution).
    
    Args:
        entities: List of entities
        entity_attributes: Dictionary mapping entity names to their attributes from Step 2.2
        nl_description: Optional original NL description
        
    Returns:
        dict: Synonym detection results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_3_attribute_synonym_detection_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": [{"name": "email"}, {"name": "email_address"}]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.3: Attribute Synonym Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for attribute synonym detection")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        attributes = entity_attributes.get(entity_name, [])
        
        task = step_2_3_attribute_synonym_detection(
            entity_name=entity_name,
            attributes=attributes,
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
            entity_results[entity_name] = {
                "synonyms": [],
                "merged_attributes": [],
                "final_attribute_list": entity_attributes.get(entity_name, []),
            }
        else:
            entity_results[entity_name] = result
    
    total_merged = sum(len(r.get("merged_attributes", [])) for r in entity_results.values())
    logger.info(f"Attribute synonym detection completed: {total_merged} attributes merged across {len(entity_results)} entities")
    
    return {"entity_results": entity_results}

