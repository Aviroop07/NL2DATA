"""Phase 2, Step 2.9: Derived Attribute Formulas (DSL).

Extracts the computation logic for derived attributes.
Needed for data generation - derived columns are computed, not sampled.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.pipeline_config import get_phase2_config
from NL2DATA.utils.dsl.validator import validate_dsl_expression
from NL2DATA.utils.dsl.analysis import dsl_identifiers_used, dsl_contains_aggregate
from NL2DATA.utils.dsl.prompt_spec import dsl_prompt_spec_text

logger = get_logger(__name__)


class DerivedFormulaOutput(BaseModel):
    """Output structure for derived attribute formula extraction."""
    formula: str = Field(
        description="The computation expression for the derived attribute in NL2DATA DSL (strict allowlist; no unknown functions)."
    )
    expression_type: str = Field(
        description="Type of expression: 'arithmetic', 'string_concatenation', 'conditional', 'date_calculation', 'aggregation', 'other'"
    )
    dependencies: List[str] = Field(
        description="List of attribute names that this derived attribute depends on (e.g., ['quantity', 'unit_price'] for total_price)"
    )
    reasoning: str = Field(description="Explanation of the derivation logic and why this formula is correct")




@traceable_step("2.9", phase=2, tags=['phase_2_step_9'])
async def step_2_9_derived_attribute_formula(
    entity_name: str,
    derived_attribute: str,
    all_attributes: List[str],  # All attributes for the entity
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    derivation_rule: Optional[str] = None,  # Optional hint from Step 2.8
) -> Dict[str, Any]:
    """
    Step 2.9 (per-derived-attribute): Extract the computation formula for a derived attribute.
    
    This is designed to be called in parallel for multiple derived attributes.
    
    Args:
        entity_name: Name of the entity
        derived_attribute: Name of the derived attribute
        all_attributes: List of all attribute names for the entity
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        derivation_rule: Optional derivation rule hint from Step 2.8
        
    Returns:
        dict: Derived formula result with formula, expression_type, dependencies, and reasoning
        
    Example:
        >>> result = await step_2_9_derived_attribute_formula(
        ...     "Order",
        ...     "total_price",
        ...     ["order_id", "quantity", "unit_price", "total_price"]
        ... )
        >>> result["formula"]
        "quantity * unit_price"
        >>> result["dependencies"]
        ["quantity", "unit_price"]
    """
    logger.debug(f"Extracting formula for derived attribute: {entity_name}.{derived_attribute}")
    
    # Validate that derived attribute exists
    if derived_attribute not in all_attributes:
        logger.warning(
            f"Derived attribute '{derived_attribute}' not found in attributes list for entity {entity_name}"
        )
        return {
            "formula": "",
            "expression_type": "other",
            "dependencies": [],
            "reasoning": f"Attribute {derived_attribute} not found in entity attributes"
        }
    
    # Build context
    context_parts = []
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"All attributes: {', '.join(all_attributes)}")
    if derivation_rule:
        context_parts.append(f"Derivation rule hint: {derivation_rule}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    dsl_spec = dsl_prompt_spec_text(include_examples=True)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to extract the computation expression for a derived attribute.

A derived attribute is computed from other attributes, not stored directly. Examples:
- total_price = quantity * unit_price (arithmetic)
- full_name = CONCAT(first_name, " ", last_name) (string concatenation)
- age = YEAR(CURRENT_DATE) - YEAR(birth_date) (date calculation)
- is_eligible = IF (age >= 18 AND status = 'active') THEN true ELSE false (conditional)
- total_orders = COUNT(order_id) (aggregation / metric)

IMPORTANT (strict):
- Output `formula` MUST be a valid NL2DATA DSL expression (not free-form SQL).
- Do NOT invent functions. Only use supported functions (basic math ops, IF/CASE, and allowlisted functions like CONCAT/COALESCE/DATEADD/DATEDIFF/COUNT/SUM/AVG/MIN/MAX etc).
- Do NOT qualify column names with dots here. Use ONLY bare attribute names from the provided attribute list.
- The formula must reference ONLY attributes that exist in this entity.
- `dependencies` must list ONLY attributes used by the formula (subset of the provided list).

Return a JSON object with:
- formula: The computation expression
- expression_type: Type of expression (arithmetic, string_concatenation, conditional, date_calculation, aggregation, other)
- dependencies: List of attribute names this formula depends on
- reasoning: REQUIRED - Explanation of the derivation logic (cannot be omitted)"""
    # Put DSL spec in the SYSTEM message (user request) so it is always authoritative.
    system_prompt = system_prompt + "\n\n" + dsl_spec
    
    # Human prompt
    human_prompt_template = """Extract the computation formula for the derived attribute: {entity_name}.{derived_attribute}

{context}

Natural Language Description:
{nl_description}

Return a JSON object with the formula, expression type, dependencies, and reasoning."""
    
    try:
        cfg = get_phase2_config()
        llm = get_model_for_step("2.9")
        config = get_trace_config("2.9", phase=2, tags=["phase_2_step_9"])

        feedback: str = ""
        last: Optional[DerivedFormulaOutput] = None

        for round_idx in range(cfg.step_2_9_max_revision_rounds + 1):
            prompt = human_prompt_template
            if feedback:
                prompt = (
                    human_prompt_template
                    + "\n\nRevision required. Fix the issues below and return corrected JSON only.\n"
                    + "Issues:\n"
                    + feedback
                )

            result: DerivedFormulaOutput = await standardized_llm_call(
                llm=llm,
                output_schema=DerivedFormulaOutput,
                system_prompt=system_prompt,
                human_prompt_template=prompt,
                input_data={
                    "entity_name": entity_name,
                    "derived_attribute": derived_attribute,
                    "context": context_msg,
                    "nl_description": nl_description or "",
                },
                config=config,
            )
            last = result

            issues: List[str] = []
            formula = (result.formula or "").strip()
            if not formula:
                issues.append("formula is empty")
            else:
                v = validate_dsl_expression(formula)
                if not v.get("valid", False):
                    issues.append(f"formula is not valid NL2DATA DSL: {v.get('error')}")

                used = dsl_identifiers_used(formula)
                dotted = sorted([u for u in used if "." in u])
                if dotted:
                    issues.append(f"formula uses dotted identifiers (not allowed here): {dotted}")

                # enforce only entity attributes referenced
                used_bare = sorted([u for u in used if u and "." not in u])
                unknown_used = [u for u in used_bare if u not in all_attributes]
                if unknown_used:
                    issues.append(
                        f"formula references unknown attributes (must be from list): {unknown_used}. "
                        f"Allowed: {all_attributes}"
                    )

            deps = [d for d in (result.dependencies or []) if isinstance(d, str) and d]
            invalid_deps = [d for d in deps if d not in all_attributes]
            if invalid_deps:
                issues.append(f"dependencies contains attributes not in entity attribute list: {invalid_deps}")

            # ensure deps cover used identifiers (best-effort)
            if formula:
                used = dsl_identifiers_used(formula)
                used_bare = {u for u in used if u and "." not in u}
                missing_deps = sorted([u for u in used_bare if u in all_attributes and u not in set(deps)])
                extra_deps = sorted([d for d in deps if d not in used_bare])
                if missing_deps:
                    issues.append(f"dependencies is missing attributes used in formula: {missing_deps}")
                if extra_deps:
                    issues.append(f"dependencies contains attributes not used in formula: {extra_deps}")

            if not issues:
                out = result.model_dump()
                out["is_aggregate_metric"] = dsl_contains_aggregate(formula) or (
                    (result.expression_type or "").strip().lower() == "aggregation"
                )
                return out

            if round_idx >= cfg.step_2_9_max_revision_rounds:
                # Return last result with warning-like info; caller can decide what to do.
                out = result.model_dump()
                out["is_aggregate_metric"] = dsl_contains_aggregate(formula) or (
                    (result.expression_type or "").strip().lower() == "aggregation"
                )
                out["validation_errors"] = issues
                return out

            feedback = "\n".join(f"- {x}" for x in issues)

        # Should never happen
        return (last.model_dump() if last else {"formula": "", "expression_type": "other", "dependencies": [], "reasoning": ""})

    except Exception as e:
        logger.error(
            f"Error extracting formula for derived attribute {entity_name}.{derived_attribute}: {e}",
            exc_info=True
        )
        raise


async def step_2_9_derived_attribute_formulas_batch(
    entity_derived_attributes: Dict[str, List[str]],  # entity_name -> list of derived attribute names
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attribute names
    entity_descriptions: Optional[Dict[str, str]] = None,  # entity_name -> description
    derivation_rules: Optional[Dict[str, Dict[str, str]]] = None,  # entity_name -> {attr: rule} from Step 2.8
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.9: Extract formulas for all derived attributes (parallel execution).
    
    Args:
        entity_derived_attributes: Dictionary mapping entity names to their derived attribute lists (from Step 2.8)
        entity_attributes: Dictionary mapping entity names to all their attribute names
        entity_descriptions: Optional dictionary mapping entity names to descriptions
        derivation_rules: Optional dictionary from Step 2.8 mapping entity names to derivation rules
        nl_description: Optional original NL description
        
    Returns:
        dict: Derived formula results for all entities, keyed by entity name, then by attribute name
        
    Example:
        >>> result = await step_2_9_derived_attribute_formulas_batch(
        ...     entity_derived_attributes={"Order": ["total_price"]},
        ...     entity_attributes={"Order": ["order_id", "quantity", "unit_price", "total_price"]}
        ... )
        >>> "Order" in result["entity_results"]
        True
        >>> "total_price" in result["entity_results"]["Order"]
        True
    """
    logger.info(
        f"Starting Step 2.9: Derived Attribute Formulas for "
        f"{sum(len(attrs) for attrs in entity_derived_attributes.values())} derived attributes"
    )
    
    if not entity_derived_attributes:
        logger.warning("No derived attributes provided for formula extraction")
        return {"entity_results": {}}
    
    # Execute in parallel for all derived attributes across all entities
    import asyncio
    
    tasks = []
    for entity_name, derived_attrs in entity_derived_attributes.items():
        all_attrs = entity_attributes.get(entity_name, [])
        entity_desc = (entity_descriptions or {}).get(entity_name, "")
        entity_rules = (derivation_rules or {}).get(entity_name, {})
        
        for derived_attr in derived_attrs:
            derivation_rule = entity_rules.get(derived_attr)
            
            task = step_2_9_derived_attribute_formula(
                entity_name=entity_name,
                derived_attribute=derived_attr,
                all_attributes=all_attrs,
                nl_description=nl_description,
                entity_description=entity_desc,
                derivation_rule=derivation_rule,
            )
            tasks.append((entity_name, derived_attr, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, _, task in tasks],
        return_exceptions=True
    )
    
    # Process results - organize by entity, then by attribute
    entity_results = {}
    entity_metrics: Dict[str, Dict[str, Any]] = {}
    for i, ((entity_name, derived_attr, _), result) in enumerate(zip(tasks, results)):
        if entity_name not in entity_results:
            entity_results[entity_name] = {}
        if entity_name not in entity_metrics:
            entity_metrics[entity_name] = {}
        
        if isinstance(result, Exception):
            logger.error(f"Error processing {entity_name}.{derived_attr}: {result}")
            entity_results[entity_name][derived_attr] = {
                "formula": "",
                "expression_type": "other",
                "dependencies": [],
                "reasoning": f"Error during analysis: {str(result)}",
                "is_aggregate_metric": False,
            }
        else:
            # If flagged as aggregate metric, store separately too (do not discard).
            try:
                is_metric = bool(result.get("is_aggregate_metric", False)) if isinstance(result, dict) else False
            except Exception:
                is_metric = False
            entity_results[entity_name][derived_attr] = result
            if is_metric:
                entity_metrics[entity_name][derived_attr] = result
    
    total_formulas = sum(len(attrs) for attrs in entity_results.values())
    logger.info(
        f"Derived attribute formula extraction completed: {total_formulas} formulas extracted "
        f"across {len(entity_results)} entities"
    )
    
    # Keep both: entity_results (all), and entity_metrics (aggregate/query-level derived).
    # Callers that only want row-level derived attributes can filter by is_aggregate_metric == False.
    return {"entity_results": entity_results, "entity_metrics": entity_metrics}

