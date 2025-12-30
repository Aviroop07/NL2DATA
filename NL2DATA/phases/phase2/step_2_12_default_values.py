"""Phase 2, Step 2.12: Default Values.

Determines default values for attributes (e.g., created_at defaults to CURRENT_TIMESTAMP, status defaults to "active").
Important for data generation and application logic.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import re

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class DefaultValuesOutput(BaseModel):
    """Output structure for default value identification."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    default_values: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to their default values (e.g., {'status': 'active', 'created_at': 'CURRENT_TIMESTAMP'})"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    reasoning: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to reasoning for why they have these default values"
    )


def _validate_default_value_syntax(attr: str, default: str) -> tuple[bool, Optional[str]]:
    """
    Basic validation of default value syntax.
    
    Checks:
    - Not empty
    - Common SQL defaults are valid (CURRENT_TIMESTAMP, CURRENT_DATE, etc.)
    - Numeric defaults are valid
    - String defaults are quoted or unquoted appropriately
    
    Returns:
        (is_valid, error_message)
    """
    if not default or not default.strip():
        return False, "Default value cannot be empty"
    
    default = default.strip()
    
    # Common SQL function defaults (case-insensitive)
    sql_functions = [
        "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
        "NOW()", "GETDATE()", "SYSDATE",
        "TRUE", "FALSE", "NULL"
    ]
    
    default_upper = default.upper()
    if default_upper in sql_functions or default_upper.startswith("CURRENT_"):
        return True, None
    
    # Numeric defaults
    try:
        float(default)
        return True, None
    except ValueError:
        pass
    
    # String defaults (may be quoted or unquoted)
    # Remove quotes for validation
    unquoted = default
    if (default.startswith("'") and default.endswith("'")) or (default.startswith('"') and default.endswith('"')):
        unquoted = default[1:-1]
    
    # If it's not a SQL function, numeric, or quoted string, it might still be valid
    # (e.g., unquoted string literals, expressions)
    # We'll be lenient here and let the database validate
    return True, None


@traceable_step("2.12", phase=2, tags=['phase_2_step_12'])
async def step_2_12_default_values(
    entity_name: str,
    attributes: List[str],  # All attributes for the entity
    nullable_attributes: Optional[List[str]] = None,  # From Step 2.11
    non_nullable_attributes: Optional[List[str]] = None,  # From Step 2.11
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.12 (per-entity): Determine default values for attributes.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attribute names
        nullable_attributes: Optional list of nullable attributes from Step 2.11
        non_nullable_attributes: Optional list of non-nullable attributes from Step 2.11
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Default values result with default_values and reasoning
        
    Example:
        >>> result = await step_2_12_default_values(
        ...     "Order",
        ...     ["order_id", "status", "created_at"],
        ...     nullable_attributes=["status"],
        ...     non_nullable_attributes=["order_id", "created_at"]
        ... )
        >>> result["default_values"].get("status")
        "pending"
        >>> result["default_values"].get("created_at")
        "CURRENT_TIMESTAMP"
    """
    logger.debug(f"Identifying default values for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot identify default values")
        return {
            "default_values": {},
            "reasoning": {}
        }
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"All attributes: {', '.join(attributes)}")
    if nullable_attributes:
        context_parts.append(f"Nullable attributes: {', '.join(nullable_attributes)}")
    if non_nullable_attributes:
        context_parts.append(f"Non-nullable attributes: {', '.join(non_nullable_attributes)}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to determine default values for attributes.

DEFAULT VALUES:
- Default values are used when a value is not explicitly provided during insertion
- Common defaults:
  * Timestamps: CURRENT_TIMESTAMP, NOW() for created_at, updated_at
  * Status fields: 'active', 'pending', 'inactive' for status attributes
  * Boolean fields: TRUE, FALSE, or 'true', 'false'
  * Numeric fields: 0, 1, or other appropriate numeric defaults
  * String fields: Empty string '', or meaningful defaults like 'unknown'
- Attributes with defaults are often (but not always) nullable
- Primary keys typically don't have defaults (auto-increment or explicit values)
- Consider business logic: What should the value be if not specified?

Return a JSON object with:
- default_values: Dictionary mapping attribute names to their default values (SQL-compatible syntax)
- reasoning: Dictionary mapping attribute names to explanations of why these defaults are appropriate

IMPORTANT JSON FORMAT:
The reasoning dictionary must have the SAME keys as default_values, with string values (not nested objects).
Example:
{{
  "default_values": {{
    "status": "'active'",
    "created_at": "CURRENT_TIMESTAMP"
  }},
  "reasoning": {{
    "status": "Default status is active for new records",
    "created_at": "Automatically set to current timestamp on insert"
  }}
}}

DO NOT use invalid JSON syntax like: "status": "'active'": "reasoning text" (this is invalid).
Use: "status": "reasoning text" (this is valid).

Only include attributes that should have defaults. Not all attributes need defaults."""
    
    # Human prompt
    human_prompt_template = """Determine default values for attributes in the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes should have default values and what those defaults should be, along with reasoning."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.12")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.12", phase=2, tags=["default_values"])
        result: DefaultValuesOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DefaultValuesOutput,
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
        default_values = dict(result.default_values)
        invalid_attrs = [attr for attr in default_values.keys() if attr not in attributes]
        
        if invalid_attrs:
            logger.warning(
                f"Entity {entity_name}: Default value attributes {invalid_attrs} don't exist in attribute list"
            )
            # Remove invalid attributes
            default_values = {
                attr: value for attr, value in default_values.items()
                if attr in attributes
            }
        
        # Validate default value syntax
        validated_defaults = {}
        for attr, default in default_values.items():
            is_valid, error_msg = _validate_default_value_syntax(attr, default)
            if is_valid:
                validated_defaults[attr] = default
            else:
                logger.warning(
                    f"Entity {entity_name}, attribute {attr}: Invalid default value syntax: {error_msg}. "
                    f"Default value: {default}"
                )
        
        # Clean up reasoning to only include valid attributes
        reasoning = dict(result.reasoning)
        reasoning_cleaned = {
            attr: reason for attr, reason in reasoning.items()
            if attr in validated_defaults
        }
        
        # Create new model instance if modifications were made
        if (validated_defaults != default_values or reasoning_cleaned != reasoning):
            result = DefaultValuesOutput(
                default_values=validated_defaults,
                reasoning=reasoning_cleaned
            )
        
        logger.debug(
            f"Entity {entity_name}: Found {len(result.default_values)} attributes with default values"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        error_str = str(e)
        logger.error(f"Error identifying default values for entity {entity_name}: {error_str}", exc_info=True)
        
        # Deterministic fallback: Return empty defaults instead of crashing
        # This allows the pipeline to continue even if LLM calls fail
        logger.warning(
            f"Returning empty default values for entity {entity_name} due to error. "
            f"Pipeline will continue with no defaults for this entity."
        )
        return {
            "default_values": {},
            "reasoning": {}
        }


async def step_2_12_default_values_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attribute names
    entity_nullability: Optional[Dict[str, Dict[str, Any]]] = None,  # entity_name -> nullability result from Step 2.11
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.12: Identify default values for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to all their attribute names
        entity_nullability: Optional dictionary mapping entity names to nullability results from Step 2.11
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Default values results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_12_default_values_batch(
        ...     entities=[{"name": "Order"}],
        ...     entity_attributes={"Order": ["order_id", "status", "created_at"]},
        ...     entity_nullability={"Order": {"nullable_attributes": ["status"], "non_nullable_attributes": ["order_id", "created_at"]}}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.12: Default Values for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for default value identification")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        
        # Extract nullability info from Step 2.11 results
        nullability_info = (entity_nullability or {}).get(entity_name, {})
        nullable_attrs = nullability_info.get("nullable_attributes", [])
        non_nullable_attrs = nullability_info.get("non_nullable_attributes", [])
        
        task = step_2_12_default_values(
            entity_name=entity_name,
            attributes=attributes,
            nullable_attributes=nullable_attrs if nullable_attrs else None,
            non_nullable_attributes=non_nullable_attrs if non_nullable_attrs else None,
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
                "default_values": {},
                "reasoning": {}
            }
        else:
            entity_results[entity_name] = result
    
    total_defaults = sum(len(r.get("default_values", {})) for r in entity_results.values())
    logger.info(
        f"Default value identification completed: {total_defaults} default values identified "
        f"across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

