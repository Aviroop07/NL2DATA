"""Phase 7, Step 7.6: Distribution Compilation.

Combine all generation strategies into a unified GenerationIR specification.
Deterministic transformation - structures all attribute generation strategies.
"""

import json
from typing import Dict, Any, List, Optional

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def step_7_6_distribution_compilation(
    numerical_strategies: Dict[str, Dict[str, Any]],  # From Step 7.1: attribute -> strategy
    text_strategies: Dict[str, Dict[str, Any]],  # From Step 7.2: attribute -> strategy
    boolean_strategies: Dict[str, Dict[str, Any]],  # From Step 7.3: attribute -> strategy
    categorical_strategies: Optional[Dict[str, Dict[str, Any]]] = None,  # From Phase 4.7: attribute -> distribution
    entity_volumes: Optional[Dict[str, Dict[str, Any]]] = None,  # From Step 7.4: entity -> volume
    partitioning_strategies: Optional[Dict[str, Dict[str, Any]]] = None,  # From Step 7.5: entity -> strategy
) -> Dict[str, Any]:
    """
    Step 7.6 (deterministic): Compile all generation strategies into GenerationIR.
    
    This is a deterministic transformation that combines all attribute generation
    strategies into a unified GenerationIR format for the data generation engine.
    
    Args:
        numerical_strategies: Numerical attribute strategies from Step 7.1
        text_strategies: Text attribute strategies from Step 7.2
        boolean_strategies: Boolean attribute strategies from Step 7.3
        categorical_strategies: Optional categorical distributions from Phase 4.7
        entity_volumes: Optional entity volume specifications from Step 7.4
        partitioning_strategies: Optional partitioning strategies from Step 7.5
        
    Returns:
        dict: Compiled GenerationIR with column generation specs
        
    Example:
        >>> result = step_7_6_distribution_compilation(
        ...     numerical_strategies={"Customer.age": {"distribution_type": "normal", ...}},
        ...     text_strategies={"Customer.name": {"generator_type": "faker.name", ...}},
        ...     boolean_strategies={}
        ... )
        >>> len(result["column_gen_specs"]) > 0
        True
    """
    logger.info("Starting Step 7.6: Distribution Compilation (deterministic)")
    
    column_gen_specs = []
    
    # Compile numerical strategies
    for attr_key, strategy in numerical_strategies.items():
        table, column = attr_key.split(".", 1) if "." in attr_key else ("", attr_key)
        column_gen_specs.append({
            "table": table,
            "column": column,
            "type": "numerical",
            "distribution": {
                "type": strategy.get("distribution_type", "uniform"),
                "parameters": strategy.get("parameters", {}),
                "range": {
                    "min": strategy.get("min", 0.0),
                    "max": strategy.get("max", 100.0)
                }
            },
            "provider": None,
        })
    
    # Compile text strategies
    for attr_key, strategy in text_strategies.items():
        table, column = attr_key.split(".", 1) if "." in attr_key else ("", attr_key)
        column_gen_specs.append({
            "table": table,
            "column": column,
            "type": "text",
            "distribution": None,
            "provider": {
                "type": strategy.get("generator_type", "faker.text"),
                "parameters": strategy.get("parameters", {}),
                "fallback": strategy.get("fallback"),
            },
            "not_possible": strategy.get("not_possible", False),
        })
    
    # Compile boolean strategies
    for attr_key, strategy in boolean_strategies.items():
        table, column = attr_key.split(".", 1) if "." in attr_key else ("", attr_key)
        is_random = strategy.get("is_random", True)
        
        if is_random:
            # Use bernoulli distribution for random booleans
            column_gen_specs.append({
                "table": table,
                "column": column,
                "type": "boolean",
                "distribution": {
                    "type": "bernoulli",
                    "parameters": {
                        "p_true": 0.5  # Default probability
                    }
                },
                "provider": None,
            })
        else:
            # Use conditional distribution for dependent booleans
            column_gen_specs.append({
                "table": table,
                "column": column,
                "type": "boolean",
                "distribution": {
                    "type": "conditional",
                    "dependency_dsl": strategy.get("dependency_dsl"),
                },
                "provider": None,
            })
    
    # Compile categorical strategies (from Phase 4.7)
    if categorical_strategies:
        for attr_key, strategy in categorical_strategies.items():
            table, column = attr_key.split(".", 1) if "." in attr_key else ("", attr_key)
            column_gen_specs.append({
                "table": table,
                "column": column,
                "type": "categorical",
                "distribution": {
                    "type": "categorical",
                    "values": strategy.get("distribution", {}),
                },
                "provider": None,
            })
    
    logger.info(
        f"Distribution compilation completed: {len(column_gen_specs)} column generation specs "
        f"({len(numerical_strategies)} numerical, {len(text_strategies)} text, "
        f"{len(boolean_strategies)} boolean, {len(categorical_strategies or {})} categorical)"
    )
    
    generation_ir = {
        "column_gen_specs": column_gen_specs,
        "entity_volumes": entity_volumes or {},
        "partitioning_strategies": partitioning_strategies or {},
    }
    
    # Log the complete GenerationIR
    logger.info("=== GENERATIONIR (Step 7.6 Output) ===")
    logger.info(json.dumps(generation_ir, indent=2, default=str))
    logger.info("=== END GENERATIONIR ===")
    
    return generation_ir

