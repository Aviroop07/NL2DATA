"""Phase 2, Step 2.4: Composite Attribute Handling (with decomposition DSLs).

For composite attributes like "address", determines if they should be one field
or decomposed (street, city, zip). Affects normalization and queryability.
"""

from typing import Dict, Any, List, Optional

from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.pipeline_config import get_phase2_config
from NL2DATA.utils.dsl.validator import validate_dsl_expression
from NL2DATA.utils.dsl.analysis import dsl_identifiers_used
from NL2DATA.utils.dsl.prompt_spec import dsl_prompt_spec_text

logger = get_logger(__name__)


class CompositeAttributeInfo(BaseModel):
    """Information about a composite attribute."""
    name: str = Field(description="Name of the composite attribute")
    should_decompose: bool = Field(description="Whether this attribute should be decomposed into sub-attributes")
    decomposition: Optional[List[str]] = Field(
        default=None,
        description="List of sub-attribute names if should_decompose is True (e.g., ['street', 'city', 'zip'] for 'address')"
    )
    decomposition_dsls: Optional[Dict[str, str]] = Field(
        default=None,
        description="If should_decompose is True, map each sub-attribute name to a DSL expression that derives it ONLY from the original composite attribute."
    )
    reasoning: str = Field(description="Reasoning for decomposition decision")


class CompositeAttributeOutput(BaseModel):
    """Output structure for composite attribute handling."""
    composite_attributes: List[CompositeAttributeInfo] = Field(
        default_factory=list,
        description="List of composite attributes with decomposition decisions"
    )


def _validate_decomposition_dsls(
    *,
    composite_attr: str,
    decomposition: List[str],
    decomposition_dsls: Optional[Dict[str, str]],
) -> List[str]:
    """Deterministically validate decomposition DSLs.

    Rules:
    - Must provide a DSL expression for every decomposed sub-attribute.
    - Each DSL must validate with validate_dsl_expression().
    - Each DSL must reference ONLY the composite attribute name (no other identifiers).
    - Each DSL must reference the composite attribute at least once (not constant).
    """
    issues: List[str] = []
    if not decomposition:
        return issues
    if not decomposition_dsls or not isinstance(decomposition_dsls, dict):
        return [f"Missing decomposition_dsls for composite attribute '{composite_attr}'"]

    missing = [s for s in decomposition if s not in decomposition_dsls]
    if missing:
        issues.append(f"Missing DSL for decomposed sub-attributes: {missing}")

    for sub_attr in decomposition:
        expr = (decomposition_dsls.get(sub_attr) or "").strip()
        if not expr:
            issues.append(f"Empty DSL for sub-attribute '{sub_attr}'")
            continue
        v = validate_dsl_expression(expr)
        if not v.get("valid", False):
            issues.append(f"Invalid DSL for '{sub_attr}': {v.get('error')}")
            continue
        used = dsl_identifiers_used(expr)
        dotted = [u for u in used if "." in u]
        if dotted:
            issues.append(f"DSL for '{sub_attr}' uses dotted identifiers (not allowed): {sorted(dotted)}")
        used_bare = {u for u in used if u and "." not in u}
        if composite_attr not in used_bare:
            issues.append(f"DSL for '{sub_attr}' must reference '{composite_attr}'")
        extra = sorted([u for u in used_bare if u != composite_attr])
        if extra:
            issues.append(f"DSL for '{sub_attr}' references other identifiers (not allowed): {extra}")

    return issues


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
    cfg = get_phase2_config()
    
    if not attributes:
        logger.debug(f"No attributes provided for entity {entity_name}")
        return {"composite_attributes": []}
    
    # Build attribute list for prompt
    attribute_list_str = ""
    for i, attr in enumerate(attributes, 1):
        attr_name = attr if isinstance(attr, str) else (attr.get("name", "Unknown") if isinstance(attr, dict) else getattr(attr, "name", "Unknown"))
        attribute_list_str += f"{i}. {attr_name}\n"
    
    dsl_spec = dsl_prompt_spec_text(include_examples=True)

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
- decomposition_dsls: If should_decompose is True, you MUST provide a DSL expression for EACH decomposed sub-attribute.
  Each DSL expression MUST derive the sub-attribute ONLY from the original composite attribute (the attribute named in `name`).
- reasoning: REQUIRED - Clear explanation of the decision (cannot be omitted)

Important:
- Only identify attributes that are truly composite (can be broken down)
- Consider query needs: if users will filter/sort by sub-components, decompose
- Consider normalization: if decomposition helps avoid redundancy, decompose
- Be practical: don't over-decompose simple attributes

DSL constraints (strict):
- The DSL must follow the provided NL2DATA DSL spec exactly.
- Do NOT invent functions or syntax.
- Do NOT use dotted identifiers. Only use bare attribute names.
- For decomposition DSLS: the ONLY allowed identifier is the composite attribute itself (e.g., address).
"""
    # Put DSL spec in the SYSTEM message (user request) so it is always authoritative.
    system_prompt = system_prompt + "\n\n" + dsl_spec
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}

Attributes to check:
{{attribute_list}}

IMPORTANT:
- Do NOT use the original NL description here; focus only on whether the given attributes are composite.
- Do NOT introduce new attributes not listed above."""
    
    # Initialize model
    llm = get_model_for_step("2.4")  # Step 2.4 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("2.4", phase=2, tags=["phase_2_step_4"])
        # NL description is often global and can mislead decomposition; always omit here.
        nl_for_prompt = ""
        feedback: str = ""
        last: Optional[CompositeAttributeOutput] = None

        for round_idx in range(cfg.step_2_4_max_revision_rounds + 1):
            prompt = human_prompt
            if feedback:
                prompt = (
                    human_prompt
                    + "\n\nRevision required. Fix the issues below and return corrected JSON only.\n"
                    + "Issues:\n"
                    + feedback
                )

            result: CompositeAttributeOutput = await standardized_llm_call(
                llm=llm,
                output_schema=CompositeAttributeOutput,
                system_prompt=system_prompt,
                human_prompt_template=prompt,
                input_data={
                    "attribute_list": attribute_list_str,
                    "nl_description": nl_for_prompt,
                },
                config=config,
            )
            last = result

            issues: List[str] = []
            for ca in result.composite_attributes:
                if not ca.should_decompose:
                    continue
                if not ca.decomposition or not isinstance(ca.decomposition, list):
                    issues.append(f"{ca.name}: should_decompose=true but decomposition list is missing/empty")
                    continue
                issues.extend(
                    [f"{ca.name}: {x}" for x in _validate_decomposition_dsls(
                        composite_attr=ca.name,
                        decomposition=ca.decomposition,
                        decomposition_dsls=ca.decomposition_dsls,
                    )]
                )

            if not issues:
                break

            if round_idx >= cfg.step_2_4_max_revision_rounds:
                break
            feedback = "\n".join(f"- {x}" for x in issues)

        if last is None:
            return {"composite_attributes": [], "composite_decompositions": {}}

        # Build a compact mapping for state consumption:
        # entity -> composite_attr -> sub_attr -> dsl
        composite_decompositions: Dict[str, Dict[str, Dict[str, str]]] = {}
        entity_map: Dict[str, Dict[str, str]] = {}
        for ca in last.composite_attributes:
            if not ca.should_decompose:
                continue
            if not ca.decomposition or not ca.decomposition_dsls:
                continue
            sub_map = {k: v for k, v in (ca.decomposition_dsls or {}).items() if isinstance(k, str) and isinstance(v, str)}
            if sub_map:
                entity_map[ca.name] = sub_map
        composite_decompositions[entity_name] = entity_map
        
        # Work with Pydantic model directly
        composite_count = len(last.composite_attributes)
        
        logger.debug(
            f"Entity {entity_name}: {composite_count} composite attributes identified"
        )
        
        # Convert to dict only at return boundary
        out = last.model_dump()
        out["composite_decompositions"] = composite_decompositions
        return out
        
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
    composite_decompositions: Dict[str, Any] = {}
    for i, ((entity_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results[entity_name] = {"composite_attributes": []}
        else:
            entity_results[entity_name] = result
            if isinstance(result, dict) and result.get("composite_decompositions"):
                # merge entity-level mapping
                composite_decompositions.update(result.get("composite_decompositions") or {})
    
    total_composite = sum(len(r.get("composite_attributes", [])) for r in entity_results.values())
    logger.info(f"Composite attribute handling completed: {total_composite} composite attributes identified across {len(entity_results)} entities")
    
    return {"entity_results": entity_results, "composite_decompositions": composite_decompositions}

