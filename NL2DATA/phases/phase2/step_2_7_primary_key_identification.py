"""Phase 2, Step 2.7: Primary Key Identification.

Determines which attribute(s) uniquely identify each entity.
Critical for table design - every table needs a primary key.
"""

from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.pipeline_config import get_phase2_config

logger = get_logger(__name__)

_DT_TOKENS = ("date", "time", "timestamp", "datetime")


def _is_datetime_like(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return False
    if n.endswith("_at"):
        return True
    return any(tok in n for tok in _DT_TOKENS)


def _validate_pk_candidate(
    *,
    entity_name: str,
    attributes: List[str],
    primary_key: List[str],
    surrogate_key: str,
) -> Tuple[List[str], List[str]]:
    """Return (validated_pk, issues)."""
    issues: List[str] = []
    allowed = set([a for a in (attributes or []) if isinstance(a, str) and a])
    allowed.add(surrogate_key)

    pk = [a.strip() for a in (primary_key or []) if isinstance(a, str) and a.strip()]

    invalid = [a for a in pk if a not in allowed]
    if invalid:
        issues.append(
            f"{entity_name}: primary_key contains attributes not in the allowed list: {invalid}. "
            f"Allowed are the provided attributes plus the surrogate key '{surrogate_key}'."
        )
        pk = [a for a in pk if a in allowed]

    if len(pk) == 1 and pk[0] != surrogate_key and _is_datetime_like(pk[0]):
        issues.append(
            f"{entity_name}: primary_key is a single datetime-like column '{pk[0]}'. "
            f"Do not use a datetime/timestamp alone as a primary key unless the NL explicitly guarantees uniqueness. "
            f"Prefer an existing *_id attribute, a stable natural key, or use the surrogate '{surrogate_key}'."
        )

    return pk, issues


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
    cfg = get_phase2_config()
    surrogate_key = f"{entity_name.lower()}_id"
    
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
    context_parts.append(
        f"Surrogate key option: If no suitable stable key exists in the available attributes, "
        f"you may choose '{surrogate_key}' as a surrogate primary key. If chosen, it will be added as a new attribute."
    )
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to identify the primary key for an entity.

A primary key is a set of attributes that uniquely identifies each instance of the entity. Key considerations:
1. The primary key must be unique for every instance
2. It should be stable (not change over time)
3. It should be minimal (no unnecessary attributes)
4. Common patterns: single ID attribute (e.g., customer_id), composite keys (e.g., order_id + item_id), or natural keys (e.g., email, ISBN)

**CRITICAL CONSTRAINT**: You MUST only use attribute names from the provided attribute list. DO NOT invent or suggest attribute names that are not in the list. If you suggest an attribute name that doesn't exist, it will be rejected and the entity will have no primary key.

**SURROGATE KEYS (IMPORTANT)**:
- If no suitable stable natural key exists in the provided attributes, choose a surrogate key in the form `<entity>_id` (e.g., `customer_id`).
- You MUST use the exact surrogate key name given in the context (if you choose a surrogate).
- Do NOT use a datetime/timestamp column alone as a primary key unless the NL explicitly guarantees it is unique.

**AVAILABLE ATTRIBUTES**: You can ONLY use attribute names from the provided attribute list OR the explicitly allowed surrogate key name provided in context. Do NOT invent other new attribute names.

Return a JSON object with:
- primary_key: List of attribute names that form the primary key (MUST be from the provided attribute list)
- reasoning: REQUIRED - Clear explanation of why these attributes form the primary key (cannot be omitted)
- alternative_keys: Any alternative candidate keys (optional, also must be from the provided attribute list)"""
    
    # Human prompt
    human_prompt_template = """Identify the primary key for the entity: {entity_name}

{context}

{nl_section}

Return a JSON object with the primary key attributes, reasoning, and any alternative candidate keys."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.7")
        
        config = get_trace_config("2.7", phase=2, tags=["phase_2_step_7"])

        nl_section = ""
        if cfg.step_2_7_include_nl_context and (nl_description or "").strip():
            nl_section = f"Natural Language Description:\n{nl_description}\n"
        else:
            nl_section = "Natural Language Description: (omitted)\n"

        feedback: str = ""
        last_issues: List[str] = []

        for round_idx in range(cfg.step_2_7_max_revision_rounds + 1):
            prompt = human_prompt_template
            if feedback:
                prompt = (
                    human_prompt_template
                    + "\n\nRevision required. Fix the issues below and return corrected JSON only.\n"
                    + "Issues:\n"
                    + feedback
                )

            result: PrimaryKeyOutput = await standardized_llm_call(
                llm=llm,
                output_schema=PrimaryKeyOutput,
                system_prompt=system_prompt,
                human_prompt_template=prompt,
                input_data={
                    "entity_name": entity_name,
                    "context": context_msg,
                    "nl_section": nl_section,
                },
                config=config,
            )

            validated_pk, issues = _validate_pk_candidate(
                entity_name=entity_name,
                attributes=attributes,
                primary_key=result.primary_key,
                surrogate_key=surrogate_key,
            )
            last_issues = issues

            if not issues and validated_pk:
                result = PrimaryKeyOutput(
                    primary_key=validated_pk,
                    reasoning=result.reasoning,
                    alternative_keys=result.alternative_keys,
                )
                break

            if round_idx >= cfg.step_2_7_max_revision_rounds:
                logger.warning(
                    f"Entity {entity_name}: Step 2.7 could not be fully corrected after {cfg.step_2_7_max_revision_rounds} revision round(s). "
                    f"Falling back to surrogate key '{surrogate_key}'."
                )
                result = PrimaryKeyOutput(
                    primary_key=[surrogate_key],
                    reasoning=(
                        f"Fallback: unable to identify a safe primary key from provided attributes (or LLM output was invalid). "
                        f"Using surrogate key '{surrogate_key}'."
                    ),
                    alternative_keys=[],
                )
                break

            feedback = "\n".join(f"- {x}" for x in (issues or [])) or (
                f"- {entity_name}: primary_key is empty/invalid after validation; choose a stable key or '{surrogate_key}'."
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
        
        logger.debug(f"Entity {entity_name}: Primary key identified as {result.primary_key}")
        
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

