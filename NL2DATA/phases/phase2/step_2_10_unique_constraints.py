"""Phase 2, Step 2.10: Unique Constraints.

Identifies attributes that must be unique beyond the primary key (e.g., email, username).
Ensures data integrity.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class UniqueConstraintsOutput(BaseModel):
    """Output structure for unique constraint identification."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    unique_attributes: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attribute names that must be unique (e.g., email, username, ISBN)"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    unique_combinations: Optional[List[List[str]]] = Field(
        default_factory=list,
        description="List of attribute combinations that together must be unique (e.g., [['order_id', 'item_id']])"
    )
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    reasoning: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names (or combination strings) to reasoning for why they must be unique"
    )


@traceable_step("2.10", phase=2, tags=['phase_2_step_10'])
async def step_2_10_unique_constraints(
    entity_name: str,
    attributes: List[str],  # All attributes for the entity
    primary_key: Optional[List[str]] = None,  # Primary key from Step 2.7
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.10 (per-entity): Identify attributes that must be unique beyond the primary key.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attribute names
        primary_key: Optional primary key attributes (to exclude from unique constraints)
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Unique constraints result with unique_attributes, unique_combinations, and reasoning
        
    Example:
        >>> result = await step_2_10_unique_constraints(
        ...     "Customer",
        ...     ["customer_id", "email", "username", "phone"],
        ...     primary_key=["customer_id"]
        ... )
        >>> "email" in result["unique_attributes"]
        True
    """
    logger.debug(f"Identifying unique constraints for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot identify unique constraints")
        return {
            "unique_attributes": [],
            "unique_combinations": [],
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
        context_parts.append(f"Primary key: {', '.join(primary_key)} (already unique, exclude from unique constraints)")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to identify unique constraints for an entity.

UNIQUE CONSTRAINTS:
- Attributes that must be unique across all instances (beyond the primary key)
- Examples: email addresses, usernames, ISBNs, social security numbers, license plates
- Can be single attributes or combinations of attributes
- Primary key attributes are already unique, so exclude them from unique constraints

UNIQUE COMBINATIONS:
- Multiple attributes that together must be unique
- Examples: [order_id, item_id] - same item can appear in different orders, but same item can't appear twice in same order
- Examples: [student_id, course_id, semester] - student can take same course in different semesters

Return a JSON object with:
- unique_attributes: List of single attribute names that must be unique
- unique_combinations: List of lists, where each inner list is a combination of attributes that together must be unique
- reasoning: REQUIRED - Dictionary mapping attribute names (or combination strings like "attr1+attr2") to explanations (every unique attribute and combination MUST have an entry in reasoning)"""
    
    # Human prompt
    human_prompt_template = """Identify unique constraints for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes or attribute combinations must be unique, along with reasoning."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.10")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.10", phase=2, tags=["phase_2_step_10"])
        result: UniqueConstraintsOutput = await standardized_llm_call(
            llm=llm,
            output_schema=UniqueConstraintsOutput,
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
        # Validate that unique attributes exist and exclude primary key
        unique_attrs = result.unique_attributes or []
        pk_set = set(primary_key) if primary_key else set()
        
        # Remove primary key attributes from unique constraints (they're already unique)
        unique_attrs_filtered = [attr for attr in unique_attrs if attr not in pk_set]
        invalid_unique = [attr for attr in unique_attrs_filtered if attr not in attributes]
        
        if invalid_unique:
            logger.warning(
                f"Entity {entity_name}: Unique attributes {invalid_unique} don't exist in attribute list"
            )
            unique_attrs_filtered = [attr for attr in unique_attrs_filtered if attr in attributes]
        
        if len(unique_attrs) != len(unique_attrs_filtered):
            removed = set(unique_attrs) - set(unique_attrs_filtered)
            logger.debug(f"Entity {entity_name}: Removed {removed} from unique constraints (primary key or invalid)")
        
        # Validate unique combinations
        unique_combos = result.unique_combinations or []
        validated_combos = []
        for combo in unique_combos:
            if isinstance(combo, list):
                # Filter out primary key attributes and invalid attributes
                valid_combo = [attr for attr in combo if attr in attributes and attr not in pk_set]
                if len(valid_combo) > 0 and len(valid_combo) == len(combo):
                    validated_combos.append(valid_combo)
                elif len(valid_combo) > 0:
                    logger.debug(
                        f"Entity {entity_name}: Filtered combination {combo} to {valid_combo} "
                        f"(removed primary key or invalid attributes)"
                    )
                    validated_combos.append(valid_combo)
        
        # Clean up reasoning to only include valid attributes/combinations
        reasoning = dict(result.reasoning or {})
        valid_keys = set(unique_attrs_filtered)
        for combo in validated_combos:
            combo_key = "+".join(combo)
            valid_keys.add(combo_key)
        
        reasoning_cleaned = {
            key: reason for key, reason in reasoning.items()
            if key in valid_keys or any(key.startswith(attr + "+") or key.endswith("+" + attr) for attr in valid_keys)
        }
        
        # Create new model instance if modifications were made
        # Also normalize Optional(None) outputs: if the model returned nulls, ensure we
        # return concrete [] / {} to avoid downstream NoneType errors.
        if (
            result.unique_attributes is None
            or result.unique_combinations is None
            or result.reasoning is None
            or (unique_attrs_filtered != unique_attrs)
            or (validated_combos != unique_combos)
            or (reasoning_cleaned != reasoning)
        ):
            result = UniqueConstraintsOutput(
                unique_attributes=unique_attrs_filtered,
                unique_combinations=validated_combos,
                reasoning=reasoning_cleaned
            )
        
        logger.debug(
            f"Entity {entity_name}: Found {len(result.unique_attributes)} unique attributes and "
            f"{len(result.unique_combinations)} unique combinations"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error identifying unique constraints for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_10_unique_constraints_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attribute names
    entity_primary_keys: Optional[Dict[str, List[str]]] = None,  # entity_name -> primary key from Step 2.7
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.10: Identify unique constraints for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to all their attribute names
        entity_primary_keys: Optional dictionary mapping entity names to their primary keys from Step 2.7
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Unique constraints results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_10_unique_constraints_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": ["customer_id", "email", "username"]},
        ...     entity_primary_keys={"Customer": ["customer_id"]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.10: Unique Constraints for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for unique constraint identification")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        primary_key = (entity_primary_keys or {}).get(entity_name)
        
        task = step_2_10_unique_constraints(
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
                "unique_attributes": [],
                "unique_combinations": [],
                "reasoning": {}
            }
        else:
            entity_results[entity_name] = result
    
    total_unique_attrs = sum(len(r.get("unique_attributes", [])) for r in entity_results.values())
    total_unique_combos = sum(len(r.get("unique_combinations", [])) for r in entity_results.values())
    logger.info(
        f"Unique constraint identification completed: {total_unique_attrs} unique attributes and "
        f"{total_unique_combos} unique combinations across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

