"""Phase 7, Step 7.1: Numerical Range Definition.

Determine value ranges and distribution types for numerical attributes using tool-based strategy selection.
"""

from typing import Dict, Any, List, Optional

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.phases.phase7.tools.strategy_selection import select_strategy_via_tool
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


@traceable_step("7.1", phase=7, tags=['phase_7_step_1'])
async def step_7_1_numerical_range_definition(
    attribute_name: str,
    attribute_description: Optional[str],
    attribute_type: str,  # SQL type from Step 4.3
    entity_name: str,
    constraints: Optional[List[Dict[str, Any]]] = None,  # Constraints affecting this attribute
    relations: Optional[List[Dict[str, Any]]] = None,
    entity_cardinality: Optional[str] = None,  # From Step 1.8
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 7.1 (per-numerical attribute, LLM): Define numerical range and distribution.
    
    Args:
        attribute_name: Name of the numerical attribute
        attribute_description: Optional description of the attribute
        attribute_type: SQL type (e.g., "INTEGER", "DECIMAL(10,2)")
        entity_name: Name of the entity/table
        constraints: Optional constraints affecting this attribute
        relations: Optional relations involving this entity
        entity_cardinality: Optional cardinality from Step 1.8
        nl_description: Optional original NL description
        
    Returns:
        dict: Range definition with min, max, distribution_type, parameters, reasoning
    """
    logger.debug(f"Defining numerical range for {entity_name}.{attribute_name}")
    
    # Get model
    model = get_model_for_step("7.1")
    
    # Create prompt for tool-based strategy selection
    system_prompt = """You are a data generation expert. Your task is to select an appropriate generation strategy for numerical attributes.

You have access to generation tools that can create values from different distributions:
- **generate_normal**: Normal (Gaussian) distribution - use for bell curves (heights, test scores)
- **generate_lognormal**: Log-normal distribution - use for positive values with heavy tails (transaction amounts, incomes)
- **generate_uniform**: Uniform distribution - use for equal probability across range (random IDs, quantities)
- **generate_pareto**: Pareto (power-law) distribution - use for power-law patterns (wealth distribution, file sizes)
- **generate_zipf**: Zipfian distribution - use for popularity rankings (product rankings, page views)
- **generate_exponential**: Exponential distribution - use for time-based attributes (inter-arrival times)

SELECTION RULES:
- Consider the attribute type (INTEGER vs DECIMAL)
- Consider constraints (e.g., CHECK constraints, business rules)
- Consider the domain context
- Choose the tool that best matches the attribute's natural distribution pattern

Call the appropriate tool with parameters that define the distribution. The tool will validate your parameters."""
    
    constraints_context = ""
    if constraints:
        constraints_context = "\n\nConstraints affecting this attribute:\n" + "\n".join(
            f"- {c.get('description', '')}" for c in constraints[:5]
        )
    else:
        constraints_context = ""
    
    # Format human prompt (not using template since we have all values)
    human_prompt = f"""Attribute: {entity_name}.{attribute_name}
Type: {attribute_type}
Description: {attribute_description or 'No description'}
Entity Cardinality: {entity_cardinality or 'Unknown'}
{constraints_context}

Select an appropriate generation strategy for this numerical attribute by calling one of the available tools."""
    
    # Use tool-based strategy selection
    try:
        strategy_result = await select_strategy_via_tool(
            llm=model,
            attribute_name=attribute_name,
            attribute_type=attribute_type,
            attribute_description=attribute_description,
            entity_name=entity_name,
            is_categorical=False,
            is_boolean=False,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,  # Pass formatted string, not template
        )
        
        # Extract min/max from parameters if available (for distributions that support it)
        parameters = strategy_result.get("parameters", {})
        min_val = parameters.get("min", 0.0)
        max_val = parameters.get("max", 100.0)
        
        # For distributions without explicit min/max, try to infer from parameters
        if "min" not in parameters and "max" not in parameters:
            # Try to infer reasonable defaults based on distribution type
            strategy_name = strategy_result.get("strategy_name", "uniform")
            if strategy_name == "normal":
                mu = parameters.get("mu", 50.0)
                sigma = parameters.get("sigma", 10.0)
                min_val = mu - 3 * sigma  # 3-sigma rule
                max_val = mu + 3 * sigma
            elif strategy_name == "lognormal":
                min_val = parameters.get("min", 0.0)
                max_val = parameters.get("max", 1000.0)
            elif strategy_name == "exponential":
                lambda_val = parameters.get("lambda_", 0.1)
                min_val = parameters.get("min", 0.0)
                max_val = parameters.get("max", 10.0 / lambda_val)  # Rough estimate
        
        return {
            "min": min_val,
            "max": max_val,
            "distribution_type": strategy_result.get("strategy_name", "uniform"),
            "parameters": parameters,
            "reasoning": strategy_result.get("reasoning", "Strategy selected via tool call")
        }
    except Exception as e:
        logger.error(f"Numerical range definition failed: {e}")
        raise


async def step_7_1_numerical_range_definition_batch(
    numerical_attributes: List[Dict[str, Any]],  # List of numerical attributes with metadata
    constraints_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,  # attribute -> constraints
) -> Dict[str, Dict[str, Any]]:
    """Define ranges for multiple numerical attributes in parallel."""
    import asyncio
    
    tasks = []
    attribute_keys = []
    
    for attr in numerical_attributes:
        attr_key = f"{attr.get('entity_name', '')}.{attr.get('attribute_name', '')}"
        attribute_keys.append(attr_key)
        
        constraints = None
        if constraints_map and attr_key in constraints_map:
            constraints = constraints_map[attr_key]
        
        tasks.append(
            step_7_1_numerical_range_definition(
                attribute_name=attr.get("attribute_name", ""),
                attribute_description=attr.get("attribute_description"),
                attribute_type=attr.get("attribute_type", "INTEGER"),
                entity_name=attr.get("entity_name", ""),
                constraints=constraints,
                relations=attr.get("relations"),
                entity_cardinality=attr.get("entity_cardinality"),
                nl_description=attr.get("nl_description"),
            )
        )
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = {}
    for key, result in zip(attribute_keys, results):
        if isinstance(result, Exception):
            logger.error(f"Range definition failed for {key}: {result}")
            output[key] = {
                "min": 0.0,
                "max": 100.0,
                "distribution_type": "uniform",
                "parameters": {},
                "reasoning": f"Generation failed: {str(result)}"
            }
        else:
            output[key] = result
    
    return output

