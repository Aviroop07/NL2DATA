"""Phase 4, Step 4.4: Categorical Detection.

Identify attributes with fixed value sets (e.g., status, category).
Categorical attributes need special generation strategies (categorical distribution) vs. continuous distributions.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase4.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
    extract_attribute_type_hint,
)

logger = get_logger(__name__)


class CategoricalDetectionOutput(BaseModel):
    """Output structure for categorical attribute detection."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    categorical_attributes: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that are categorical (have fixed value sets)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    reasoning: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping each categorical attribute name to explanation of why it's categorical (every attribute in categorical_attributes should have an entry)"
    )


@traceable_step("4.4", phase=4, tags=['phase_4_step_4'])
async def step_4_4_categorical_detection(
    entity_name: str,
    attributes: List[Dict[str, Any]],  # All attributes with descriptions and types
    attribute_types: Optional[Dict[str, Dict[str, Any]]] = None,  # From Step 4.3
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
    categorical_definition: Optional[str] = None,  # External context: what constitutes categorical
) -> Dict[str, Any]:
    """
    Step 4.4 (per-entity): Identify attributes with fixed value sets (categorical attributes).
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attributes with descriptions, type_hints
        attribute_types: Optional data types from Step 4.3 (to understand attribute types)
        nl_description: Optional original NL description
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        categorical_definition: Optional definition of what constitutes a categorical attribute (external context)
        
    Returns:
        dict: Categorical detection result with categorical_attributes list and reasoning dictionary
        
    Example:
        >>> result = await step_4_4_categorical_detection(
        ...     entity_name="Order",
        ...     attributes=[{"name": "status", "description": "Order status"}]
        ... )
        >>> "status" in result["categorical_attributes"]
        True
    """
    logger.debug(f"Detecting categorical attributes for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot detect categorical attributes")
        return {
            "categorical_attributes": [],
            "reasoning": {}
        }
    
    # Build comprehensive context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    # Attributes summary with descriptions and types
    attr_details = []
    for attr in attributes:
        attr_name = extract_attribute_name(attr)
        attr_desc = extract_attribute_description(attr)
        attr_type_hint = extract_attribute_type_hint(attr)
        
        # Add type info from Step 4.3 if available
        type_info = ""
        if attribute_types and attr_name in attribute_types:
            type_data = attribute_types[attr_name]
            sql_type = type_data.get("type", "")
            if sql_type:
                type_info = f" (SQL type: {sql_type})"
        
        attr_info = f"  - {attr_name}"
        if attr_desc:
            attr_info += f": {attr_desc}"
        if attr_type_hint:
            attr_info += f" (hint: {attr_type_hint})"
        attr_info += type_info
        attr_details.append(attr_info)
    
    context_parts.append(f"Attributes ({len(attributes)}):\n" + "\n".join(attr_details))
    
    # Categorical definition (external context)
    if categorical_definition:
        context_parts.append(f"\nCategorical Attribute Definition:\n{categorical_definition}")
    else:
        # Default definition if not provided
        context_parts.append(
            "\nCategorical Attribute Definition:\n"
            "A categorical attribute has a fixed, limited set of possible values. Examples:\n"
            "- Status fields: 'active', 'inactive', 'pending', 'completed'\n"
            "- Category fields: 'electronics', 'clothing', 'food'\n"
            "- Type fields: 'premium', 'standard', 'basic'\n"
            "- Boolean-like fields with named values: 'yes'/'no', 'true'/'false'\n"
            "Attributes with continuous values (numbers, dates, free text) are NOT categorical."
        )
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to identify categorical attributes for an entity.

CATEGORICAL ATTRIBUTES:
A categorical attribute has a fixed, limited set of possible values. Examples:
- **Status fields**: 'active', 'inactive', 'pending', 'completed', 'cancelled'
- **Category fields**: 'electronics', 'clothing', 'food', 'books'
- **Type fields**: 'premium', 'standard', 'basic', 'trial'
- **Boolean-like fields**: 'yes'/'no', 'true'/'false', 'enabled'/'disabled'
- **Enum-like fields**: 'small'/'medium'/'large', 'low'/'medium'/'high'
- **State fields**: 'draft', 'published', 'archived'

NOT CATEGORICAL:
- **Continuous numeric values**: prices, quantities, measurements (these have infinite possible values)
- **Dates/timestamps**: These are continuous (infinite possible values)
- **Free text**: descriptions, comments, names (these have infinite possible values)
- **IDs**: customer_id, order_id (these are unique identifiers, not categories)

IMPORTANT DISTINCTIONS:
- An attribute with CHECK constraint limiting values IS categorical
- An attribute that can only take a few specific values IS categorical
- An attribute with many possible values (even if commonly used values are limited) is NOT categorical
- Boolean attributes (true/false) ARE categorical

**OUTPUT REQUIREMENTS**:
Return a JSON object with:
- categorical_attributes: List of attribute names that are categorical (can be empty if none are categorical)
- reasoning: REQUIRED - Dictionary mapping each categorical attribute name to explanation of why it's categorical
  * EVERY attribute in categorical_attributes MUST have a corresponding entry in reasoning
  * The reasoning dictionary MUST NOT be null/empty if categorical_attributes is not empty
  * If categorical_attributes is empty, reasoning can be an empty dictionary

**CRITICAL**: Only include attributes that are truly categorical (fixed value set). Not all attributes are categorical. If an attribute has infinite possible values (like names, descriptions, prices), it is NOT categorical.

**VALIDATION**: Your output will be validated. If you include an attribute in categorical_attributes but don't provide reasoning for it, your response will be rejected."""

    # Human prompt template
    human_prompt_template = """Identify categorical attributes for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes are categorical, along with reasoning for each."""

    # Initialize model and create chain
    llm = get_model_for_step("4.4")  # Step 4.4 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("4.4", phase=4, tags=["phase_4_step_4"])
        result: CategoricalDetectionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CategoricalDetectionOutput,
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
        # Validate that all categorical attributes have reasoning
        categorical_attrs = result.categorical_attributes or []
        reasoning = result.reasoning
        # Fix: Handle None case for reasoning
        if reasoning is None:
            reasoning = {}
        missing_reasoning = [attr for attr in categorical_attrs if attr not in reasoning]
        if missing_reasoning:
            logger.warning(
                f"Missing reasoning for categorical attributes {missing_reasoning} in entity {entity_name}. "
                f"Adding default reasoning."
            )
            # Update reasoning dict
            updated_reasoning = dict(reasoning)
            for attr in missing_reasoning:
                updated_reasoning[attr] = f"Attribute {attr} identified as categorical but reasoning not provided"
            # Create new model instance with updated reasoning
            result = CategoricalDetectionOutput(
                categorical_attributes=result.categorical_attributes,
                reasoning=updated_reasoning
            )
        
        logger.info(
            f"Categorical detection completed for {entity_name}: {len(result.categorical_attributes)} categorical attributes"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(
            f"Error detecting categorical attributes for entity {entity_name}: {e}",
            exc_info=True
        )
        raise


async def step_4_4_categorical_detection_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> attributes
    entity_attribute_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # entity_name -> {attr: type_info}
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    categorical_definition: Optional[str] = None,  # External context
) -> Dict[str, Any]:
    """
    Step 4.4: Detect categorical attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to their attributes
        entity_attribute_types: Optional dictionary mapping entity names to their attribute types from Step 4.3
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        categorical_definition: Optional definition of categorical attributes (external context)
        
    Returns:
        dict: Categorical detection results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_4_4_categorical_detection_batch(
        ...     entities=[{"name": "Order"}],
        ...     entity_attributes={"Order": [{"name": "status", "description": "Order status"}]}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 4.4: Categorical Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for categorical detection")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        attribute_types = (entity_attribute_types or {}).get(entity_name)
        
        task = step_4_4_categorical_detection(
            entity_name=entity_name,
            attributes=attributes,
            attribute_types=attribute_types,
            nl_description=nl_description,
            entity_description=entity_desc,
            domain=domain,
            categorical_definition=categorical_definition,
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
                "categorical_attributes": [],
                "reasoning": {},
                "error": str(result)
            }
        else:
            entity_results[entity_name] = result
    
    total_categorical = sum(
        len(result.get("categorical_attributes", []))
        for result in entity_results.values()
        if not result.get("error")
    )
    logger.info(
        f"Categorical detection completed: {len(entity_results)} entities, {total_categorical} total categorical attributes"
    )
    
    return {"entity_results": entity_results}

