"""Phase 2, Step 2.8: Multivalued/Derived Detection.

Identifies attributes that can have multiple values or are computed from others.
Affects normalization (multivalued) and generation strategy (derived).
"""

from typing import Dict, Any, List, Optional, Literal, Set, Tuple
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.pipeline_config import get_phase2_config

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


def _extract_backticked_identifiers(text: str) -> Set[str]:
    """Extract identifiers mentioned as backticked code tokens in NL (e.g., `duration_minutes`)."""
    import re

    t = (text or "")
    return set(re.findall(r"`([a-zA-Z_][a-zA-Z0-9_]*)`", t))


def _mentions_out_of_entity_columns(
    *,
    text: str,
    allowed: Set[str],
    global_explicit_cols: Set[str],
) -> List[str]:
    """Return explicit NL column identifiers mentioned in text that aren't allowed for this entity.

    We only flag identifiers that were explicitly backticked in the NL description to avoid
    false positives on generic words like "minutes".
    """
    import re

    t = (text or "")
    bad: Set[str] = set()
    for c in global_explicit_cols:
        if c in allowed:
            continue
        if f"`{c}`" in t:
            bad.add(c)
            continue
        if re.search(rf"\b{re.escape(c)}\b", t):
            bad.add(c)
    return sorted(bad)


def _validate_2_8_output(
    *,
    entity_name: str,
    attributes: List[str],
    nl_description: str,
    out: MultivaluedDerivedOutput,
) -> List[str]:
    """Deterministic validation to prevent cross-entity leakage via derivation_rules/reasoning."""
    issues: List[str] = []
    allowed = set([a for a in (attributes or []) if isinstance(a, str) and a])
    global_cols = _extract_backticked_identifiers(nl_description)

    derived = [d for d in (out.derived or []) if isinstance(d, str) and d]
    rules = dict(out.derivation_rules or {})
    reasoning = dict(out.reasoning or {})

    for d in derived:
        rule = str(rules.get(d, "") or "")
        reason = str(reasoning.get(d, "") or "")
        bad_rule = _mentions_out_of_entity_columns(text=rule, allowed=allowed, global_explicit_cols=global_cols)
        bad_reason = _mentions_out_of_entity_columns(text=reason, allowed=allowed, global_explicit_cols=global_cols)
        bad = sorted(set(bad_rule + bad_reason))
        if bad:
            issues.append(
                f"{entity_name}.{d}: derivation text references columns not in this entity: {bad}"
            )

    return issues


def _clean_2_8_result(
    *,
    entity_name: str,
    attributes: List[str],
    result: MultivaluedDerivedOutput,
) -> MultivaluedDerivedOutput:
    """Deterministically enforce Step 2.8's contract: classify only existing attributes."""
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
    derivation_rules_cleaned = {attr: rule for attr, rule in derivation_rules.items() if attr in derived_attrs}

    multivalued_handling = dict(result.multivalued_handling or {})
    multivalued_handling_cleaned = {
        attr: strategy for attr, strategy in multivalued_handling.items() if attr in multivalued_attrs
    }

    # Clean up reasoning to only include valid attributes
    reasoning = dict(result.reasoning or {})
    valid_attrs = set(multivalued_attrs) | set(derived_attrs)
    reasoning_cleaned = {attr: reason for attr, reason in reasoning.items() if attr in valid_attrs}

    return MultivaluedDerivedOutput(
        multivalued=multivalued_attrs,
        derived=derived_attrs,
        derivation_rules=derivation_rules_cleaned,
        multivalued_handling=multivalued_handling_cleaned,
        reasoning=reasoning_cleaned,
    )


def _drop_bad_derived_attrs(
    *,
    entity_name: str,
    cleaned: MultivaluedDerivedOutput,
    issues: List[str],
) -> MultivaluedDerivedOutput:
    """Fail-closed cleanup: drop derived attrs whose derivation text leaks cross-entity columns."""
    bad: Set[str] = set()
    prefix = f"{entity_name}."
    for s in issues:
        if s.startswith(prefix):
            rest = s[len(prefix):]
            bad_attr = rest.split(":", 1)[0].strip()
            if bad_attr:
                bad.add(bad_attr)

    if not bad:
        return cleaned

    derived_keep = [d for d in (cleaned.derived or []) if d not in bad]
    rules_keep = {k: v for k, v in (cleaned.derivation_rules or {}).items() if k in derived_keep}
    # keep reasoning for multivalued and remaining derived
    keep_keys = set(derived_keep) | set(cleaned.multivalued or [])
    reasoning_keep = {k: v for k, v in (cleaned.reasoning or {}).items() if k in keep_keys}
    return MultivaluedDerivedOutput(
        multivalued=cleaned.multivalued or [],
        derived=derived_keep,
        derivation_rules=rules_keep,
        multivalued_handling=cleaned.multivalued_handling or {},
        reasoning=reasoning_keep,
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

Critical constraints:
- You MUST ONLY classify attributes from the provided 'Available attributes' list.
- You MUST NOT reference attributes/columns that are not in the provided list when writing derivation_rules or reasoning.
- If the NL description contains other tables/columns, ignore them unless they are explicitly in the current entity's available attribute list.

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
        cfg = get_phase2_config()
        llm = get_model_for_step("2.8")
        config = get_trace_config("2.8", phase=2, tags=["phase_2_step_8"])

        nl_for_prompt = ""
        if cfg.step_2_8_include_nl_context and (nl_description or "").strip():
            nl_for_prompt = nl_description or ""
        # Note: even if NL is omitted from the prompt, we still pass the full NL into deterministic
        # validators via `nl_description` so we can catch leakage via backticked identifiers.

        feedback: str = ""
        last_cleaned: Optional[MultivaluedDerivedOutput] = None
        last_issues: List[str] = []

        for round_idx in range(cfg.step_2_8_max_revision_rounds + 1):
            prompt = human_prompt_template
            if feedback:
                prompt = (
                    human_prompt_template
                    + "\n\nRevision required. Fix the issues below and return corrected JSON only.\n"
                    + "Issues:\n"
                    + feedback
                )

            raw: MultivaluedDerivedOutput = await standardized_llm_call(
                llm=llm,
                output_schema=MultivaluedDerivedOutput,
                system_prompt=system_prompt,
                human_prompt_template=prompt,
                input_data={
                    "entity_name": entity_name,
                    "context": context_msg,
                    "nl_description": nl_for_prompt,
                },
                config=config,
            )

            cleaned = _clean_2_8_result(entity_name=entity_name, attributes=attributes, result=raw)
            issues = _validate_2_8_output(
                entity_name=entity_name,
                attributes=attributes,
                nl_description=nl_description or "",
                out=cleaned,
            )

            last_cleaned = cleaned
            last_issues = issues

            if not issues:
                break

            if round_idx >= cfg.step_2_8_max_revision_rounds:
                logger.warning(
                    f"Entity {entity_name}: Step 2.8 could not be fully corrected after {cfg.step_2_8_max_revision_rounds} revision round(s)."
                )
                break

            feedback = "\n".join(f"- {x}" for x in issues)

        if last_cleaned is None:
            return {
                "multivalued": [],
                "derived": [],
                "derivation_rules": {},
                "multivalued_handling": {},
                "reasoning": {},
            }

        if last_issues:
            # Fail-closed: drop derived attrs with leaking derivation text so Step 2.9 can't be poisoned.
            last_cleaned = _drop_bad_derived_attrs(entity_name=entity_name, cleaned=last_cleaned, issues=last_issues)

        logger.debug(
            f"Entity {entity_name}: Found {len(last_cleaned.multivalued or [])} multivalued and "
            f"{len(last_cleaned.derived or [])} derived attributes"
        )

        out = last_cleaned.model_dump()
        if last_issues:
            out["validation_errors"] = last_issues
        return out
        
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

