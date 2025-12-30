"""Phase 4, Step 4.7: Categorical Distribution.

Determine probability distribution over categorical values.
Ensures realistic data generation - some values may be more common than others (e.g., "active" > "inactive").
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
)

logger = get_logger(__name__)


class CategoricalDistributionOutput(BaseModel):
    """Output structure for categorical distribution."""
    distribution: Dict[str, float] = Field(
        description="Dictionary mapping each categorical value to its probability (must sum to 1.0)"
    )
    reasoning: str = Field(description="REQUIRED - Explanation of why this probability distribution was chosen (cannot be omitted)")


@traceable_step("4.7", phase=4, tags=['phase_4_step_7'])
async def step_4_7_categorical_distribution(
    entity_name: str,
    categorical_attribute: str,  # Name of the categorical attribute
    values: List[str],  # Possible values from Step 4.6
    attribute_description: Optional[str] = None,  # Description of the attribute
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.7 (per-categorical-attribute): Determine probability distribution over categorical values.
    
    This is designed to be called in parallel for multiple categorical attributes.
    
    Args:
        entity_name: Name of the entity
        categorical_attribute: Name of the categorical attribute
        values: List of possible values from Step 4.6
        attribute_description: Optional description of the attribute
        nl_description: Optional original NL description
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Categorical distribution result with distribution dictionary and reasoning
        
    Example:
        >>> result = await step_4_7_categorical_distribution(
        ...     entity_name="Order",
        ...     categorical_attribute="status",
        ...     values=["pending", "processing", "shipped", "delivered"]
        ... )
        >>> sum(result["distribution"].values())
        1.0
    """
    logger.debug(f"Determining categorical distribution for {entity_name}.{categorical_attribute}")
    
    # Validate that values exist
    if not values:
        logger.warning(
            f"No values provided for {entity_name}.{categorical_attribute}, cannot determine distribution"
        )
        return {
            "distribution": {},
            "reasoning": "No values provided, cannot determine distribution"
        }
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"Attribute: {categorical_attribute}")
    if attribute_description:
        context_parts.append(f"Attribute description: {attribute_description}")
    context_parts.append(f"Possible values: {', '.join(values)}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to determine a probability distribution over categorical values for realistic data generation.

PROBABILITY DISTRIBUTIONS FOR CATEGORICAL ATTRIBUTES:
A probability distribution assigns a probability (between 0.0 and 1.0) to each categorical value.
The sum of all probabilities MUST equal 1.0.

DISTRIBUTION PATTERNS:
1. **Uniform**: All values equally likely (e.g., "red": 0.25, "blue": 0.25, "green": 0.25, "yellow": 0.25)
2. **Skewed**: Some values more common than others (e.g., "active": 0.7, "inactive": 0.2, "pending": 0.1)
3. **Lifecycle-based**: Values follow a natural progression (e.g., order status: "pending" > "processing" > "shipped" > "delivered")
4. **Domain-based**: Common values in the domain are more frequent (e.g., "standard" > "premium" > "trial")

CONSIDERATIONS:
- **Status fields**: Active/current states are usually more common than inactive/terminal states
- **Priority fields**: Lower priorities (e.g., "low", "normal") are usually more common than high priorities
- **Category fields**: Common categories are more frequent than rare categories
- **Lifecycle fields**: Early stages are more common than later stages (more items start than finish)

Return a JSON object with:
- distribution: Dictionary mapping each categorical value to its probability (e.g., {{"active": 0.7, "inactive": 0.3}})
- reasoning: REQUIRED - Explanation of why this probability distribution was chosen (cannot be omitted)

CRITICAL: The sum of all probabilities in the distribution MUST equal 1.0. Round to avoid floating-point errors if needed."""

    # Human prompt template
    human_prompt_template = """Determine probability distribution for the categorical attribute {categorical_attribute} in entity {entity_name}.

{context}

Natural Language Description:
{nl_description}

Return a JSON object with the probability distribution (probabilities must sum to 1.0) and reasoning."""

    # Initialize model and create chain
    llm = get_model_for_step("4.7")  # Step 4.7 maps to "high_fanout" task type
    try:
        config = get_trace_config("4.7", phase=4, tags=["phase_4_step_7"])
        result: CategoricalDistributionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CategoricalDistributionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "categorical_attribute": categorical_attribute,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        output_dict = result.model_dump()
        
        # Validate that distribution sums to 1.0
        distribution = output_dict.get("distribution", {})
        total_prob = sum(distribution.values())
        
        if abs(total_prob - 1.0) > 0.01:  # Allow small floating-point errors
            logger.warning(
                f"Distribution for {entity_name}.{categorical_attribute} sums to {total_prob}, not 1.0. "
                f"Normalizing probabilities."
            )
            # Normalize probabilities
            if total_prob > 0:
                normalized_dist = {k: v / total_prob for k, v in distribution.items()}
                output_dict["distribution"] = normalized_dist
            else:
                # If all zeros, use uniform distribution
                num_values = len(values)
                if num_values > 0:
                    uniform_prob = 1.0 / num_values
                    output_dict["distribution"] = {v: uniform_prob for v in values}
                    logger.warning(f"All probabilities were zero, using uniform distribution")
        
        logger.debug(
            f"Categorical distribution completed for {entity_name}.{categorical_attribute}: "
            f"{len(distribution)} values, sum={sum(output_dict.get('distribution', {}).values()):.3f}"
        )
        return output_dict
        
    except Exception as e:
        logger.error(
            f"Error determining categorical distribution for {entity_name}.{categorical_attribute}: {e}",
            exc_info=True
        )
        raise


async def step_4_7_categorical_distribution_batch(
    entity_categorical_values: Dict[str, Dict[str, List[str]]],  # entity_name -> {attr: values} from Step 4.6
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> all attributes with descriptions
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.7: Determine categorical distributions for all categorical attributes (parallel execution).
    
    Args:
        entity_categorical_values: Dictionary mapping entity names to their categorical attributes and values from Step 4.6
            Format: {entity_name: {attr_name: [values]}}
        entity_attributes: Dictionary mapping entity names to all their attributes with descriptions
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Categorical distribution results organized by entity, then by attribute
        
    Example:
        >>> result = await step_4_7_categorical_distribution_batch(
        ...     entity_categorical_values={"Order": {"status": ["pending", "processing", "shipped"]}},
        ...     entity_attributes={"Order": [{"name": "status", "description": "Order status"}]}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(
        f"Starting Step 4.7: Categorical Distribution for categorical attributes"
    )
    
    if not entity_categorical_values:
        logger.warning("No categorical values provided for distribution determination")
        return {"entity_results": {}}
    
    # Execute in parallel for all categorical attributes across all entities
    import asyncio
    
    tasks = []
    for entity_name, attr_values_dict in entity_categorical_values.items():
        # Get entity attributes for descriptions
        all_attrs = entity_attributes.get(entity_name, [])
        attr_dict = {extract_attribute_name(attr): attr for attr in all_attrs}
        
        for cat_attr, values in attr_values_dict.items():
            attr_info = attr_dict.get(cat_attr, {})
            attr_desc = extract_attribute_description(attr_info) if attr_info else ""
            
            task = step_4_7_categorical_distribution(
                entity_name=entity_name,
                categorical_attribute=cat_attr,
                values=values,
                attribute_description=attr_desc,
                nl_description=nl_description,
                domain=domain,
            )
            tasks.append((entity_name, cat_attr, task))
    
    if not tasks:
        logger.warning("No categorical attributes to process for distribution determination")
        return {"entity_results": {}}
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, _, task in tasks],
        return_exceptions=True
    )
    
    # Process results - organize by entity, then by attribute
    entity_results = {}
    for i, ((entity_name, cat_attr, _), result) in enumerate(zip(tasks, results)):
        if entity_name not in entity_results:
            entity_results[entity_name] = {}
        
        if isinstance(result, Exception):
            logger.error(f"Error processing {entity_name}.{cat_attr}: {result}")
            entity_results[entity_name][cat_attr] = {
                "distribution": {},
                "reasoning": f"Error during distribution determination: {str(result)}",
                "error": str(result)
            }
        else:
            entity_results[entity_name][cat_attr] = result
    
    # Validate all distributions sum to 1.0
    invalid_distributions = []
    for entity_name, attr_results in entity_results.items():
        for attr_name, attr_result in attr_results.items():
            if attr_result.get("error"):
                continue
            distribution = attr_result.get("distribution", {})
            total = sum(distribution.values())
            if abs(total - 1.0) > 0.01:
                invalid_distributions.append(f"{entity_name}.{attr_name} (sum={total:.3f})")
    
    if invalid_distributions:
        logger.warning(
            f"Found {len(invalid_distributions)} distributions that don't sum to 1.0: {invalid_distributions}"
        )
    
    total_distributions = sum(
        len([r for r in attr_results.values() if not r.get("error")])
        for attr_results in entity_results.values()
    )
    logger.info(
        f"Categorical distribution determination completed: {len(entity_results)} entities, {total_distributions} total distributions"
    )
    
    return {"entity_results": entity_results}

