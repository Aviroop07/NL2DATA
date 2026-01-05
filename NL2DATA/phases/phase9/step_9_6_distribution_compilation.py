"""Phase 9, Step 9.6: Distribution Compilation.

Compile all generation strategies into final format.
TODO: This file needs full implementation - original was lost during phase reordering.
"""

from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class ColumnGenSpec(BaseModel):
    """Column generation specification."""
    table: str = Field(description="Table name")
    column: str = Field(description="Column name")
    type: str = Field(description="Strategy type")
    strategy_data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional strategy data"
    )

    model_config = ConfigDict(extra="forbid")


class DistributionCompilationOutput(BaseModel):
    """Output structure for distribution compilation."""
    column_gen_specs: List[ColumnGenSpec] = Field(
        default_factory=list,
        description="List of column generation specifications"
    )
    entity_volumes: Dict[str, Any] = Field(
        default_factory=dict,
        description="Entity volume specifications"
    )
    partitioning_strategies: Dict[str, Any] = Field(
        default_factory=dict,
        description="Partitioning strategies"
    )

    model_config = ConfigDict(extra="forbid")


async def step_9_6_distribution_compilation(
    numerical_strategies: Dict[str, Any],
    text_strategies: Dict[str, Any],
    boolean_strategies: Dict[str, Any],
    categorical_strategies: Optional[Dict[str, Any]] = None,
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
    entity_volumes: Optional[Dict[str, Any]] = None,
    partitioning_strategies: Optional[Dict[str, Any]] = None,
) -> DistributionCompilationOutput:
    """
    Step 9.6 (deterministic): Compile all strategies into column generation specs.
    
    This step compiles generation strategies including categorical distributions.
    Categorical distributions are created from categorical values identified in step 8.3.
    
    Args:
        numerical_strategies: Numerical attribute strategies
        text_strategies: Text attribute strategies
        boolean_strategies: Boolean attribute strategies
        categorical_strategies: Optional categorical strategies (deprecated, use categorical_values instead)
        categorical_values: Optional dictionary mapping entity_name -> attribute_name -> CategoricalValueIdentificationOutput
                          Contains the identified categorical values from step 8.3
        entity_volumes: Optional entity volume specifications
        partitioning_strategies: Optional partitioning strategies
        
    Returns:
        dict: Compiled column generation specifications
    """
    logger.info("Step 9.6: Distribution Compilation (including categorical distributions)")
    
    # Placeholder return - compile all strategies into column_gen_specs
    column_gen_specs = []
    
    # Helper function to convert strategy dict to ColumnGenSpec
    def create_spec(attr_key: str, strategy: Dict[str, Any]) -> Optional[ColumnGenSpec]:
        if "." in attr_key:
            table, column = attr_key.split(".", 1)
            strategy_type = strategy.get("type", "unknown")
            # Extract strategy data (everything except type)
            strategy_data = {k: v for k, v in strategy.items() if k != "type"}
            return ColumnGenSpec(
                table=table,
                column=column,
                type=strategy_type,
                strategy_data=strategy_data,
            )
        return None
    
    # Add numerical strategies
    for attr_key, strategy in numerical_strategies.items():
        # Handle both dict and Pydantic model
        if hasattr(strategy, 'model_dump'):
            strategy_dict = strategy.model_dump()
        else:
            strategy_dict = strategy
        spec = create_spec(attr_key, strategy_dict)
        if spec is not None:
            column_gen_specs.append(spec)
    
    # Add text strategies
    for attr_key, strategy in text_strategies.items():
        if hasattr(strategy, 'model_dump'):
            strategy_dict = strategy.model_dump()
        else:
            strategy_dict = strategy
        spec = create_spec(attr_key, strategy_dict)
        if spec is not None:
            column_gen_specs.append(spec)
    
    # Add boolean strategies
    for attr_key, strategy in boolean_strategies.items():
        if hasattr(strategy, 'model_dump'):
            strategy_dict = strategy.model_dump()
        else:
            strategy_dict = strategy
        spec = create_spec(attr_key, strategy_dict)
        if spec is not None:
            column_gen_specs.append(spec)
    
    # Add categorical strategies from categorical values (step 8.3)
    if categorical_values:
        for entity_name, attr_results in categorical_values.items():
            for attr_name, cat_value_result in attr_results.items():
                attr_key = f"{entity_name}.{attr_name}"
                
                # Extract categorical values
                if hasattr(cat_value_result, 'categorical_values'):
                    cat_values = cat_value_result.categorical_values
                elif isinstance(cat_value_result, dict):
                    cat_values = cat_value_result.get("categorical_values", [])
                else:
                    continue
                
                if not cat_values:
                    continue
                
                # Build categorical distribution
                # Extract value strings
                value_strings = []
                for cv in cat_values:
                    if hasattr(cv, 'value'):
                        value_strings.append(cv.value)
                    elif isinstance(cv, dict):
                        value_strings.append(cv.get("value", ""))
                    elif isinstance(cv, str):
                        value_strings.append(cv)
                
                if not value_strings:
                    continue
                
                # Create categorical distribution strategy
                # Use uniform weights by default (can be customized later)
                weights = [1.0 / len(value_strings)] * len(value_strings)
                categorical_distribution = {
                    "type": "categorical",
                    "values": value_strings,
                    "weights": weights,
                }
                
                spec = ColumnGenSpec(
                    table=entity_name,
                    column=attr_name,
                    type="categorical",
                    strategy_data={
                        "distribution": categorical_distribution,
                        "values": value_strings,
                    },
                )
                column_gen_specs.append(spec)
                logger.debug(f"Added categorical distribution for {attr_key} with {len(value_strings)} values")
    
    logger.info(f"Compiled {len(column_gen_specs)} column generation specs (including categorical distributions)")
    
    return DistributionCompilationOutput(
        column_gen_specs=column_gen_specs,
        entity_volumes=entity_volumes or {},
        partitioning_strategies=partitioning_strategies or {},
    )
