"""Phase 2, Step 2.8: Multivalued/Derived Detection.

Identifies attributes that can have multiple values or are computed from others.
Affects normalization (multivalued) and generation strategy (derived).
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class MultivaluedDerivedOutput(BaseModel):
    """Output structure for multivalued/derived attribute detection."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    multivalued: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that can have multiple values (e.g., phone numbers, addresses)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    derived: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that are computed from other attributes (e.g., total_price = quantity * unit_price)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    derivation_rules: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping derived attribute names to their derivation formulas/descriptions"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    multivalued_handling: Optional[Dict[str, Literal["separate_table", "array", "json"]]] = Field(
        default_factory=dict,
        description="Dictionary mapping multivalued attribute names to their handling strategy: 'separate_table' (normalized), 'array' (array column), or 'json' (JSON column)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    reasoning: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to reasoning for why they are multivalued or derived"
    )


@traceable_step("2.8", phase=2, tags=['phase_2_step_8'])
async def step_2_8_multivalued_derived_detection(
    entity_name: str,
    attributes: List[str],  # Final attribute list from Step 2.3
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.8 (per-entity): Identify multivalued and derived attributes.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of attribute names (final list from Step 2.3)
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Multivalued/derived detection result with multivalued, derived, derivation_rules, multivalued_handling, and reasoning
        
    Example:
        >>> result = await step_2_8_multivalued_derived_detection(
        ...     "Order",
        ...     ["order_id", "quantity", "unit_price", "total_price", "phone_numbers"]
        ... )
        >>> "total_price" in result["derived"]
        True
        >>> "phone_numbers" in result["multivalued"]
        True
    """
    logger.debug(f"Detecting multivalued/derived attributes for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot detect multivalued/derived attributes")
        return {
            "multivalued": [],
            "derived": [],
            "derivation_rules": {},
            "multivalued_handling": {},
            "reasoning": {}
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
    system_prompt = """You are a database schema design expert. Your task is to identify multivalued and derived attributes for an entity.

MULTIVALUED ATTRIBUTES:
- Attributes that can have multiple values for a single entity instance
- Examples: phone_numbers (a person can have multiple phones), addresses, tags, categories
- Handling strategies:
  * "separate_table": Create a separate table (normalized approach, recommended for relational databases)
  * "array": Store as array column (if database supports arrays)
  * "json": Store as JSON column (flexible but less queryable)

DERIVED ATTRIBUTES:
- Attributes that are computed from other attributes
- Examples: total_price = quantity * unit_price, age = current_date - birth_date, full_name = first_name + " " + last_name
- These should NOT be stored in the database (computed on-the-fly) but may be needed for data generation

Return a JSON object with:
- multivalued: List of attribute names that are multivalued
- derived: List of attribute names that are derived/computed
- derivation_rules: Dictionary mapping derived attribute names to their formulas/descriptions
- multivalued_handling: Dictionary mapping multivalued attribute names to handling strategy ("separate_table", "array", or "json")
- reasoning: REQUIRED - Dictionary mapping attribute names to explanations (every attribute in multivalued and derived lists MUST have an entry in reasoning)"""
    
    # Human prompt
    human_prompt_template = """Identify multivalued and derived attributes for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes are multivalued or derived, along with handling strategies and derivation rules."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.8")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.8", phase=2, tags=["phase_2_step_8"])
        result: MultivaluedDerivedOutput = await standardized_llm_call(
            llm=llm,
            output_schema=MultivaluedDerivedOutput,
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
        # Validate that multivalued attributes exist
        multivalued_attrs = result.multivalued or []
        invalid_multivalued = [attr for attr in multivalued_attrs if attr not in attributes]
        if invalid_multivalued:
            logger.warning(
                f"Entity {entity_name}: Multivalued attributes {invalid_multivalued} don't exist in attribute list"
            )
            multivalued_attrs = [attr for attr in multivalued_attrs if attr in attributes]
        
        # Validate that derived attributes exist
        derived_attrs = result.derived or []
        invalid_derived = [attr for attr in derived_attrs if attr not in attributes]
        if invalid_derived:
            logger.warning(
                f"Entity {entity_name}: Derived attributes {invalid_derived} don't exist in attribute list"
            )
            derived_attrs = [attr for attr in derived_attrs if attr in attributes]
        
        # Clean up derivation_rules and multivalued_handling to only include valid attributes
        derivation_rules = dict(result.derivation_rules or {})
        derivation_rules_cleaned = {
            attr: rule for attr, rule in derivation_rules.items()
            if attr in derived_attrs
        }
        
        multivalued_handling = dict(result.multivalued_handling or {})
        multivalued_handling_cleaned = {
            attr: strategy for attr, strategy in multivalued_handling.items()
            if attr in multivalued_attrs
        }
        
        # Clean up reasoning to only include valid attributes
        reasoning = dict(result.reasoning or {})
        valid_attrs = set(multivalued_attrs) | set(derived_attrs)
        reasoning_cleaned = {
            attr: reason for attr, reason in reasoning.items()
            if attr in valid_attrs
        }
        
        # Create new model instance with cleaned data if needed
        if (multivalued_attrs != result.multivalued or 
            derived_attrs != result.derived or
            derivation_rules_cleaned != derivation_rules or
            multivalued_handling_cleaned != multivalued_handling or
            reasoning_cleaned != reasoning):
            result = MultivaluedDerivedOutput(
                multivalued=multivalued_attrs,
                derived=derived_attrs,
                derivation_rules=derivation_rules_cleaned,
                multivalued_handling=multivalued_handling_cleaned,
                reasoning=reasoning_cleaned
            )
        
        logger.debug(
            f"Entity {entity_name}: Found {len(result.multivalued)} multivalued and "
            f"{len(result.derived)} derived attributes"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error detecting multivalued/derived attributes for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_8_multivalued_derived_detection_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> final attribute list (from Step 2.3)
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.8: Detect multivalued/derived attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to their final attribute lists from Step 2.3
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Multivalued/derived detection results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_8_multivalued_derived_detection_batch(
        ...     entities=[{"name": "Order"}],
        ...     entity_attributes={"Order": ["order_id", "quantity", "unit_price", "total_price"]}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.8: Multivalued/Derived Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for multivalued/derived detection")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        
        task = step_2_8_multivalued_derived_detection(
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
                "multivalued": [],
                "derived": [],
                "derivation_rules": {},
                "multivalued_handling": {},
                "reasoning": {}
            }
        else:
            entity_results[entity_name] = result
    
    total_multivalued = sum(len(r.get("multivalued", [])) for r in entity_results.values())
    total_derived = sum(len(r.get("derived", [])) for r in entity_results.values())
    logger.info(
        f"Multivalued/derived detection completed: {total_multivalued} multivalued and "
        f"{total_derived} derived attributes across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

