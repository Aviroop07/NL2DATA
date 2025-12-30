"""Phase 2, Step 2.11: Nullability Constraints.

Determines which attributes can be NULL.
Critical for schema correctness - affects DDL generation and data validation rules.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class NullabilityConstraintsOutput(BaseModel):
    """Output structure for nullability constraint identification."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    nullable_attributes: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that can be NULL (optional attributes)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    non_nullable_attributes: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that cannot be NULL (required attributes)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    reasoning: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to reasoning for why they are nullable or non-nullable"
    )


@traceable_step("2.11", phase=2, tags=['phase_2_step_11'])
async def step_2_11_nullability_constraints(
    entity_name: str,
    attributes: List[str],  # All attributes for the entity
    primary_key: Optional[List[str]] = None,  # Primary key from Step 2.7 (usually non-nullable)
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.11 (per-entity): Determine which attributes can be NULL.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attribute names
        primary_key: Optional primary key attributes (usually non-nullable)
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Nullability constraints result with nullable_attributes, non_nullable_attributes, and reasoning
        
    Example:
        >>> result = await step_2_11_nullability_constraints(
        ...     "Customer",
        ...     ["customer_id", "email", "phone", "address"]
        ... )
        >>> "email" in result["non_nullable_attributes"]
        True
        >>> "phone" in result["nullable_attributes"]
        True
    """
    logger.debug(f"Identifying nullability constraints for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot identify nullability constraints")
        return {
            "nullable_attributes": [],
            "non_nullable_attributes": [],
            "reasoning": {}
        }
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"All attributes: {', '.join(attributes)}")
    if primary_key:
        context_parts.append(f"Primary key: {', '.join(primary_key)} (typically non-nullable)")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to determine which attributes can be NULL.

NULLABILITY RULES:
- Primary key attributes are typically NON-NULLABLE (required)
- Attributes that uniquely identify or are critical for the entity are typically NON-NULLABLE
- Optional attributes (e.g., middle name, phone number, optional description) are typically NULLABLE
- Attributes that may not be available at creation time can be NULLABLE
- Attributes with default values can be NULLABLE (but may also be non-nullable with defaults)

Consider:
- Business requirements: Is this attribute required for a valid entity instance?
- Data availability: Can this value be unknown or missing?
- Default values: Does the attribute have a default that makes it effectively required?

Return a JSON object with:
- nullable_attributes: List of attribute names that can be NULL
- non_nullable_attributes: List of attribute names that cannot be NULL
- reasoning: REQUIRED - Dictionary mapping each attribute name to explanation of nullability decision (every attribute MUST have an entry in reasoning)

Note: Every attribute should be in either nullable_attributes or non_nullable_attributes (no overlap, complete coverage)."""
    
    # Human prompt
    human_prompt_template = """Determine nullability constraints for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes are nullable and which are non-nullable, along with reasoning for each."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.11")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.11", phase=2, tags=["phase_2_step_11"])
        result: NullabilityConstraintsOutput = await standardized_llm_call(
            llm=llm,
            output_schema=NullabilityConstraintsOutput,
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
        # Validate that attributes exist
        nullable_attrs = result.nullable_attributes
        non_nullable_attrs = result.non_nullable_attributes
        
        # Filter invalid attributes
        nullable_filtered = [attr for attr in nullable_attrs if attr in attributes]
        non_nullable_filtered = [attr for attr in non_nullable_attrs if attr in attributes]
        
        invalid_nullable = [attr for attr in nullable_attrs if attr not in attributes]
        invalid_non_nullable = [attr for attr in non_nullable_attrs if attr not in attributes]
        
        if invalid_nullable:
            logger.warning(
                f"Entity {entity_name}: Nullable attributes {invalid_nullable} don't exist in attribute list"
            )
        if invalid_non_nullable:
            logger.warning(
                f"Entity {entity_name}: Non-nullable attributes {invalid_non_nullable} don't exist in attribute list"
            )
        
        # Check for overlap (attributes in both lists)
        overlap = set(nullable_filtered) & set(non_nullable_filtered)
        if overlap:
            logger.warning(
                f"Entity {entity_name}: Attributes {overlap} appear in both nullable and non-nullable lists. "
                f"Removing from nullable list."
            )
            nullable_filtered = [attr for attr in nullable_filtered if attr not in overlap]
        
        # Check for missing attributes (not in either list)
        all_classified = set(nullable_filtered) | set(non_nullable_filtered)
        missing = set(attributes) - all_classified
        if missing:
            logger.warning(
                f"Entity {entity_name}: Attributes {missing} are not classified. "
                f"Assuming non-nullable for safety."
            )
            non_nullable_filtered.extend(list(missing))
        
        # Clean up reasoning to only include valid attributes
        reasoning = dict(result.reasoning)
        valid_attrs = set(nullable_filtered) | set(non_nullable_filtered)
        reasoning_cleaned = {
            attr: reason for attr, reason in reasoning.items()
            if attr in valid_attrs
        }
        
        # Create new model instance if modifications were made
        if (nullable_filtered != nullable_attrs or 
            non_nullable_filtered != non_nullable_attrs or
            reasoning_cleaned != reasoning):
            result = NullabilityConstraintsOutput(
                nullable_attributes=nullable_filtered,
                non_nullable_attributes=non_nullable_filtered,
                reasoning=reasoning_cleaned
            )
        
        logger.debug(
            f"Entity {entity_name}: Found {len(result.nullable_attributes)} nullable and "
            f"{len(result.non_nullable_attributes)} non-nullable attributes"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error identifying nullability constraints for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_11_nullability_constraints_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attribute names
    entity_primary_keys: Optional[Dict[str, List[str]]] = None,  # entity_name -> primary key from Step 2.7
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.11: Identify nullability constraints for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to all their attribute names
        entity_primary_keys: Optional dictionary mapping entity names to their primary keys from Step 2.7
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Nullability constraints results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_11_nullability_constraints_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": ["customer_id", "email", "phone"]},
        ...     entity_primary_keys={"Customer": ["customer_id"]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.11: Nullability Constraints for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for nullability constraint identification")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        primary_key = (entity_primary_keys or {}).get(entity_name)
        
        task = step_2_11_nullability_constraints(
            entity_name=entity_name,
            attributes=attributes,
            primary_key=primary_key,
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
                "nullable_attributes": [],
                "non_nullable_attributes": [],
                "reasoning": {}
            }
        else:
            entity_results[entity_name] = result
    
    total_nullable = sum(len(r.get("nullable_attributes", [])) for r in entity_results.values())
    total_non_nullable = sum(len(r.get("non_nullable_attributes", [])) for r in entity_results.values())
    logger.info(
        f"Nullability constraint identification completed: {total_nullable} nullable and "
        f"{total_non_nullable} non-nullable attributes across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

