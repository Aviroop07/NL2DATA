"""Cost and call estimation functions for step registry.

Provides functions to estimate total LLM calls and costs based on
schema complexity and step definitions.
"""

from typing import Dict, Any, Optional
from .registry import STEP_REGISTRY, get_llm_steps
from .types import CallType, StepType


def estimate_total_calls(
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
) -> Dict[str, Any]:
    """
    Estimate total LLM calls based on step registry and schema complexity.
    
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
        
    Returns:
        Dict with total_calls, breakdown by phase, and formula
    """
    total = 0
    breakdown = {}
    
    # Map fanout units to counts
    fanout_counts = {
        "entity": entity_count,
        "attribute": attribute_count,
        "relation": relation_count,
        "information_need": information_need_count,
        "constraint": constraint_count,
        "text_attribute": text_attribute_count,
        "numeric_attribute": numeric_attribute_count,
        "boolean_attribute": boolean_attribute_count,
        "temporal_attribute": temporal_attribute_count,
        "derived_attribute": derived_attribute_count,
        "categorical_attribute": categorical_attribute_count,
    }
    
    for step in get_llm_steps():
        # Calculate call count based on call_type
        if step.call_type == CallType.SINGULAR:
            count = 1
        elif step.call_type == CallType.LOOP:
            # For loops, estimate average iterations
            avg_iterations = step.max_iters if step.max_iters else 3
            count = avg_iterations
        else:
            # Per-X type: multiply by fanout count
            count = fanout_counts.get(step.fanout_unit, 0)
        
        # Apply loop multiplier if applicable
        if step.is_loop and step.call_type != CallType.LOOP:
            count *= (step.max_iters if step.max_iters else 3)
        
        total += count
        phase_key = f"Phase {step.phase}"
        breakdown[phase_key] = breakdown.get(phase_key, 0) + count
    
    return {
        "total_llm_calls": total,
        "breakdown_by_phase": breakdown,
        "formula": "Sum of (fanout_count × loop_iterations) for all LLM steps"
    }


def estimate_phase_calls(
    phase: int,
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
) -> int:
    """
    Estimate LLM calls for a specific phase.
    
    Args:
        phase: Phase number (1-7)
        ... (same as estimate_total_calls)
        
    Returns:
        Estimated number of LLM calls for this phase
    """
    fanout_counts = {
        "entity": entity_count,
        "attribute": attribute_count,
        "relation": relation_count,
        "information_need": information_need_count,
        "constraint": constraint_count,
        "text_attribute": text_attribute_count,
        "numeric_attribute": numeric_attribute_count,
        "boolean_attribute": boolean_attribute_count,
        "temporal_attribute": temporal_attribute_count,
        "derived_attribute": derived_attribute_count,
        "categorical_attribute": categorical_attribute_count,
    }
    
    total = 0
    for step in STEP_REGISTRY.values():
        if step.phase != phase or step.step_type != StepType.LLM:
            continue
        
        if step.call_type == CallType.SINGULAR:
            count = 1
        elif step.call_type == CallType.LOOP:
            count = step.max_iters if step.max_iters else 3
        else:
            count = fanout_counts.get(step.fanout_unit, 0)
        
        if step.is_loop and step.call_type != CallType.LOOP:
            count *= (step.max_iters if step.max_iters else 3)
        
        total += count
    
    return total


def estimate_cost(
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
    model: str = "gpt-4o-mini",
    input_price_per_1m: float = 0.15,
    output_price_per_1m: float = 0.60,
) -> Dict[str, Any]:
    """
    Estimate total cost based on step registry and schema complexity.
    
    Args:
        ... (same as estimate_total_calls)
        model: Model name (for reference)
        input_price_per_1m: Input price per 1M tokens
        output_price_per_1m: Output price per 1M tokens
        
    Returns:
        Dict with estimated cost, token counts, and breakdown
    """
    call_estimate = estimate_total_calls(
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
    )
    
    total_calls = call_estimate["total_llm_calls"]
    
    # Estimate tokens from step registry
    total_input_tokens = 0
    total_output_tokens = 0
    
    fanout_counts = {
        "entity": entity_count,
        "attribute": attribute_count,
        "relation": relation_count,
        "information_need": information_need_count,
        "constraint": constraint_count,
        "text_attribute": text_attribute_count,
        "numeric_attribute": numeric_attribute_count,
        "boolean_attribute": boolean_attribute_count,
        "temporal_attribute": temporal_attribute_count,
        "derived_attribute": derived_attribute_count,
        "categorical_attribute": categorical_attribute_count,
    }
    
    for step in get_llm_steps():
        if step.call_type == CallType.SINGULAR:
            step_calls = 1
        elif step.call_type == CallType.LOOP:
            step_calls = step.max_iters if step.max_iters else 3
        else:
            step_calls = fanout_counts.get(step.fanout_unit, 0)
        
        if step.is_loop and step.call_type != CallType.LOOP:
            step_calls *= (step.max_iters if step.max_iters else 3)
        
        # Estimate tokens (75% input, 25% output)
        avg_tokens = step.avg_tokens_per_call
        total_input_tokens += step_calls * int(avg_tokens * 0.75)
        total_output_tokens += step_calls * int(avg_tokens * 0.25)
    
    # Calculate cost
    input_cost = (total_input_tokens / 1_000_000) * input_price_per_1m
    output_cost = (total_output_tokens / 1_000_000) * output_price_per_1m
    total_cost = input_cost + output_cost
    
    return {
        "estimated_calls": total_calls,
        "estimated_input_tokens": total_input_tokens,
        "estimated_output_tokens": total_output_tokens,
        "estimated_cost": round(total_cost, 2),
        "model": model,
        "breakdown_by_phase": call_estimate["breakdown_by_phase"],
    }

