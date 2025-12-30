"""Phase 2, Step 2.9: Derived Attribute Formulas.

Extracts the computation logic for derived attributes.
Needed for data generation - derived columns are computed, not sampled.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class DerivedFormulaOutput(BaseModel):
    """Output structure for derived attribute formula extraction."""
    formula: str = Field(
        description="The computation formula/expression for the derived attribute (e.g., 'quantity * unit_price', 'CONCAT(first_name, \" \", last_name)')"
    )
    expression_type: str = Field(
        description="Type of expression: 'arithmetic', 'string_concatenation', 'conditional', 'date_calculation', 'aggregation', 'other'"
    )
    dependencies: List[str] = Field(
        description="List of attribute names that this derived attribute depends on (e.g., ['quantity', 'unit_price'] for total_price)"
    )
    reasoning: str = Field(description="Explanation of the derivation logic and why this formula is correct")


def _validate_formula_syntax(formula: str) -> tuple[bool, Optional[str]]:
    """
    Basic validation of formula syntax.
    
    Checks:
    - Balanced parentheses
    - Valid operators (basic check)
    
    Returns:
        (is_valid, error_message)
    """
    if not formula or not formula.strip():
        return False, "Formula cannot be empty"
    
    # Check balanced parentheses
    paren_count = 0
    for char in formula:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
            if paren_count < 0:
                return False, "Unbalanced parentheses: closing ')' before opening '('"
    
    if paren_count != 0:
        return False, f"Unbalanced parentheses: {paren_count} unclosed opening '('"
    
    return True, None


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
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to extract the computation formula for a derived attribute.

A derived attribute is computed from other attributes, not stored directly. Examples:
- total_price = quantity * unit_price (arithmetic)
- full_name = CONCAT(first_name, " ", last_name) (string concatenation)
- age = YEAR(CURRENT_DATE) - YEAR(birth_date) (date calculation)
- is_eligible = IF (age >= 18 AND status = 'active') THEN true ELSE false (conditional)
- average_score = AVG(scores) (aggregation)

The formula should:
1. Reference only attributes that exist in the entity
2. Use standard SQL-like syntax or mathematical expressions
3. Be clear and unambiguous
4. Include all necessary dependencies

Return a JSON object with:
- formula: The computation expression
- expression_type: Type of expression (arithmetic, string_concatenation, conditional, date_calculation, aggregation, other)
- dependencies: List of attribute names this formula depends on
- reasoning: REQUIRED - Explanation of the derivation logic (cannot be omitted)"""
    
    # Human prompt
    human_prompt_template = """Extract the computation formula for the derived attribute: {entity_name}.{derived_attribute}

{context}

Natural Language Description:
{nl_description}

Return a JSON object with the formula, expression type, dependencies, and reasoning."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.9")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.9", phase=2, tags=["phase_2_step_9"])
        result: DerivedFormulaOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DerivedFormulaOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "derived_attribute": derived_attribute,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        # Validate formula syntax
        formula = result.formula
        is_valid, error_msg = _validate_formula_syntax(formula)
        if not is_valid:
            logger.warning(
                f"Entity {entity_name}, attribute {derived_attribute}: Formula syntax validation failed: {error_msg}"
            )
            # Don't fail, but log the warning
        
        # Validate that dependencies exist
        dependencies = result.dependencies
        invalid_deps = [dep for dep in dependencies if dep not in all_attributes]
        if invalid_deps:
            logger.warning(
                f"Entity {entity_name}, attribute {derived_attribute}: Dependencies {invalid_deps} don't exist in attribute list"
            )
            # Remove invalid dependencies - create new model instance
            dependencies = [dep for dep in dependencies if dep in all_attributes]
            result = DerivedFormulaOutput(
                formula=result.formula,
                expression_type=result.expression_type,
                dependencies=dependencies,
                reasoning=result.reasoning
            )
        
        logger.debug(
            f"Entity {entity_name}, attribute {derived_attribute}: Formula extracted: {formula}, "
            f"dependencies: {result.dependencies}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
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
    for i, ((entity_name, derived_attr, _), result) in enumerate(zip(tasks, results)):
        if entity_name not in entity_results:
            entity_results[entity_name] = {}
        
        if isinstance(result, Exception):
            logger.error(f"Error processing {entity_name}.{derived_attr}: {result}")
            entity_results[entity_name][derived_attr] = {
                "formula": "",
                "expression_type": "other",
                "dependencies": [],
                "reasoning": f"Error during analysis: {str(result)}"
            }
        else:
            entity_results[entity_name][derived_attr] = result
    
    total_formulas = sum(len(attrs) for attrs in entity_results.values())
    logger.info(
        f"Derived attribute formula extraction completed: {total_formulas} formulas extracted "
        f"across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

