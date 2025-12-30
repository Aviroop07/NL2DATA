"""Phase 2, Step 2.7: Primary Key Identification.

Determines which attribute(s) uniquely identify each entity.
Critical for table design - every table needs a primary key.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class PrimaryKeyOutput(BaseModel):
    """Output structure for primary key identification."""
    primary_key: List[str] = Field(
        description="List of attribute names that form the primary key (usually 1, can be composite)"
    )
    reasoning: str = Field(description="Explanation of why these attributes form the primary key")
    alternative_keys: List[List[str]] = Field(
        default_factory=list,
        description="Alternative candidate keys (if any) that could also uniquely identify the entity"
    )


@traceable_step("2.7", phase=2, tags=['phase_2_step_7'])
async def step_2_7_primary_key_identification(
    entity_name: str,
    attributes: List[str],  # Final attribute list from Step 2.3
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.7 (per-entity): Determine which attribute(s) uniquely identify the entity.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of attribute names (final list from Step 2.3)
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Primary key identification result with primary_key, reasoning, and alternative_keys
        
    Example:
        >>> result = await step_2_7_primary_key_identification(
        ...     "Customer",
        ...     ["customer_id", "name", "email", "phone"]
        ... )
        >>> result["primary_key"]
        ["customer_id"]
    """
    logger.debug(f"Identifying primary key for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot identify primary key")
        return {
            "primary_key": [],
            "reasoning": "No attributes available to form primary key",
            "alternative_keys": []
        }
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"Available attributes: {', '.join(attributes)}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to identify the primary key for an entity.

A primary key is a set of attributes that uniquely identifies each instance of the entity. Key considerations:
1. The primary key must be unique for every instance
2. It should be stable (not change over time)
3. It should be minimal (no unnecessary attributes)
4. Common patterns: single ID attribute (e.g., customer_id), composite keys (e.g., order_id + item_id), or natural keys (e.g., email, ISBN)

**CRITICAL CONSTRAINT**: You MUST only use attribute names from the provided attribute list. DO NOT invent or suggest attribute names that are not in the list. If you suggest an attribute name that doesn't exist, it will be rejected and the entity will have no primary key.

**AVAILABLE ATTRIBUTES**: The context will include a list of available attributes. You can ONLY use attributes from this list. If no suitable primary key exists in the available attributes, you may suggest creating an ID attribute, but you must still only reference attributes that exist or clearly indicate that a new attribute needs to be created.

Return a JSON object with:
- primary_key: List of attribute names that form the primary key (MUST be from the provided attribute list)
- reasoning: REQUIRED - Clear explanation of why these attributes form the primary key (cannot be omitted)
- alternative_keys: Any alternative candidate keys (optional, also must be from the provided attribute list)"""
    
    # Human prompt
    human_prompt_template = """Identify the primary key for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object with the primary key attributes, reasoning, and any alternative candidate keys."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.7")
        
        # Create chain
        # Invoke standardized LLM call
        config = get_trace_config("2.7", phase=2, tags=["phase_2_step_7"])
        result: PrimaryKeyOutput = await standardized_llm_call(
            llm=llm,
            output_schema=PrimaryKeyOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        # Validate that primary key attributes exist
        pk_attrs = result.primary_key
        invalid_attrs = [attr for attr in pk_attrs if attr not in attributes]
        if invalid_attrs:
            logger.warning(
                f"Entity {entity_name}: Primary key contains invalid attributes {invalid_attrs} "
                f"that don't exist in attribute list. Available attributes: {attributes}"
            )
            # Remove invalid attributes - create new model instance
            valid_pk = [attr for attr in pk_attrs if attr in attributes]
            if not valid_pk:
                logger.error(
                    f"Entity {entity_name}: No valid primary key attributes found after validation. "
                    f"LLM suggested: {pk_attrs}, but available attributes are: {attributes}. "
                    f"Injecting deterministic surrogate key as fallback."
                )
                # Deterministic fallback: inject surrogate key
                surrogate_key = f"{entity_name.lower()}_id"
                logger.warning(
                    f"Entity {entity_name}: Injecting surrogate key '{surrogate_key}' as fallback. "
                    f"This will be added to the entity's attribute list automatically."
                )
                valid_pk = [surrogate_key]
                # Update reasoning to reflect the fallback
                updated_reasoning = (
                    f"Original LLM suggestion ({pk_attrs}) contained invalid attributes not in the attribute list. "
                    f"Deterministic fallback: using surrogate key '{surrogate_key}'."
                )
            else:
                updated_reasoning = result.reasoning
            
            result = PrimaryKeyOutput(
                primary_key=valid_pk,
                reasoning=updated_reasoning,
                alternative_keys=result.alternative_keys
            )
        
        # Validate alternative keys
        alt_keys = result.alternative_keys
        validated_alt_keys = []
        for alt_key in alt_keys:
            if isinstance(alt_key, list):
                valid_alt_key = [attr for attr in alt_key if attr in attributes]
                if valid_alt_key:
                    validated_alt_keys.append(valid_alt_key)
        
        if validated_alt_keys != alt_keys:
            # Create new model instance with validated alternative keys
            result = PrimaryKeyOutput(
                primary_key=result.primary_key,
                reasoning=result.reasoning,
                alternative_keys=validated_alt_keys
            )
        
        logger.debug(
            f"Entity {entity_name}: Primary key identified as {result.primary_key}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error identifying primary key for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_7_primary_key_identification_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> final attribute list (from Step 2.3)
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.7: Identify primary keys for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to their final attribute lists from Step 2.3
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Primary key identification results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_7_primary_key_identification_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": ["customer_id", "name", "email"]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.7: Primary Key Identification for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for primary key identification")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        
        task = step_2_7_primary_key_identification(
            entity_name=entity_name,
            attributes=attributes,
            nl_description=nl_description,
            entity_description=entity_desc,
            domain=domain,
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
                "primary_key": [],
                "reasoning": f"Error during analysis: {str(result)}",
                "alternative_keys": []
            }
        else:
            entity_results[entity_name] = result
    
    total_with_pk = sum(1 for r in entity_results.values() if r.get("primary_key"))
    logger.info(f"Primary key identification completed: {total_with_pk}/{len(entity_results)} entities have primary keys")
    
    return {"entity_results": entity_results}

