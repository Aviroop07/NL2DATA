"""Cost estimation functions."""

from typing import Dict, Any
from .pricing import MODEL_PRICING
from NL2DATA.orchestration.step_registry import estimate_total_calls


def estimate_total_cost(
    entity_count: int = 0,
    attribute_count: int = 0,
    relation_count: int = 0,
    information_need_count: int = 0,
    constraint_count: int = 0,
    text_attribute_count: int = 0,
    numeric_attribute_count: int = 0,
    boolean_attribute_count: int = 0,
    temporal_attribute_count: int = 0,
    derived_attribute_count: int = 0,
    categorical_attribute_count: int = 0,
    model: str = "gpt-4o-mini"
) -> Dict[str, Any]:
    """
    Estimate total cost for IR generation using StepDefinition registry.
    
    Uses estimate_total_calls() from the StepDefinition registry to compute
    accurate call counts based on actual schema complexity.
    
    Args:
        entity_count: Number of entities
        attribute_count: Total number of attributes
        relation_count: Number of relations
        information_need_count: Number of information needs
        constraint_count: Number of constraints
        text_attribute_count: Number of text attributes
        numeric_attribute_count: Number of numeric attributes
        boolean_attribute_count: Number of boolean attributes
        temporal_attribute_count: Number of temporal attributes
        derived_attribute_count: Number of derived attributes
        categorical_attribute_count: Number of categorical attributes
        model: Model name for pricing
        
    Returns:
        Dict with estimated_calls, estimated_cost, breakdown_by_phase
    """
    from NL2DATA.orchestration.step_registry import estimate_cost
    
    return estimate_cost(
        entity_count=entity_count,
        attribute_count=attribute_count,
        relation_count=relation_count,
        information_need_count=information_need_count,
        constraint_count=constraint_count,
        text_attribute_count=text_attribute_count,
        numeric_attribute_count=numeric_attribute_count,
        boolean_attribute_count=boolean_attribute_count,
        temporal_attribute_count=temporal_attribute_count,
        derived_attribute_count=derived_attribute_count,
        categorical_attribute_count=categorical_attribute_count,
        model=model,
        input_price_per_1m=MODEL_PRICING.get(model, MODEL_PRICING["gpt-4o-mini"]).input_price,
        output_price_per_1m=MODEL_PRICING.get(model, MODEL_PRICING["gpt-4o-mini"]).output_price,
    )

