"""Phase 7, Step 7.5: Partitioning Strategy.

For very large tables (≥50M rows), determine partitioning strategy.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class PartitioningStrategyOutput(BaseModel):
    """Output structure for partitioning strategy."""
    needs_partitioning: bool = Field(description="Whether this table needs partitioning (≥50M rows)")
    partitioning_strategy: Optional[Literal["range", "hash", "list"]] = Field(
        default=None,
        description="Partitioning strategy: 'range' (by date/range), 'hash' (by hash), 'list' (by list of values)"
    )
    partition_key: Optional[str] = Field(
        default=None,
        description="Column name to partition by (e.g., 'transaction_date', 'region_id')"
    )
    partition_definition: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Partition definition details (e.g., date ranges, hash buckets)"
    )
    reasoning: str = Field(description="Reasoning for the partitioning strategy")


@traceable_step("7.5", phase=7, tags=['phase_7_step_5'])
async def step_7_5_partitioning_strategy(
    entity_name: str,
    entity_volume: Dict[str, Any],  # Volume info from Step 7.4
    table_type: Optional[str] = None,  # From Step 1.8
    columns: Optional[List[Dict[str, Any]]] = None,  # Table columns
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 7.5 (per-entity, conditional, LLM): Determine partitioning strategy.
    
    Only relevant for very large tables (≥50M rows). Returns needs_partitioning=False
    for smaller tables.
    
    Args:
        entity_name: Name of the entity/table
        entity_volume: Volume info from Step 7.4 (min_rows, max_rows, expected_rows)
        table_type: Optional table type (fact/dimension) from Step 1.8
        columns: Optional list of columns
        nl_description: Optional original NL description
        
    Returns:
        dict: Partitioning strategy with needs_partitioning, strategy, partition_key, etc.
    """
    logger.debug(f"Determining partitioning strategy for {entity_name}")
    
    expected_rows = entity_volume.get("expected_rows", 0)
    
    # Quick check: if < 50M rows, skip partitioning
    if expected_rows < 50_000_000:
        return {
            "needs_partitioning": False,
            "partitioning_strategy": None,
            "partition_key": None,
            "partition_definition": None,
            "reasoning": f"Table has {expected_rows:,} rows, below 50M threshold for partitioning"
        }
    
    # Get model
    model = get_model_for_step("7.5")
    
    # Create prompt
    system_prompt = """You are a database performance expert. Your task is to determine partitioning strategies for very large tables.

PARTITIONING STRATEGIES:
1. **range**: Partition by date ranges or numeric ranges (e.g., by month, by year)
2. **hash**: Partition by hash of a column (e.g., hash of customer_id for even distribution)
3. **list**: Partition by list of values (e.g., by region, by category)

SELECTION RULES:
- Use range partitioning for time-series data (transaction dates, timestamps)
- Use hash partitioning for even distribution across partitions
- Use list partitioning for categorical partitioning (regions, categories)
- Choose partition key that aligns with common query patterns"""
    
    columns_context = ""
    if columns:
        column_names = [col.get("name", "") for col in columns[:20]]
        columns_context = f"\n\nAvailable Columns: {', '.join(column_names)}"
    
    human_prompt = f"""Entity: {entity_name}
Expected Rows: {expected_rows:,}
Table Type: {table_type or 'Unknown'}
{columns_context}

Determine if this table needs partitioning and what strategy to use."""
    
    # Create structured chain
    # Invoke standardized LLM call
    try:
        result: PartitioningStrategyOutput = await standardized_llm_call(
            llm=model,
            output_schema=PartitioningStrategyOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
        )
        
        # Work with Pydantic model directly
        # Convert to dict only at return boundary
        return {
            "needs_partitioning": result.needs_partitioning,
            "partitioning_strategy": result.partitioning_strategy,
            "partition_key": result.partition_key,
            "partition_definition": result.partition_definition,
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"Partitioning strategy failed: {e}")
        raise


async def step_7_5_partitioning_strategy_batch(
    entities_with_volumes: List[Dict[str, Any]],  # Entities with volume info
) -> Dict[str, Dict[str, Any]]:
    """Determine partitioning strategy for multiple entities in parallel."""
    import asyncio
    
    tasks = [
        step_7_5_partitioning_strategy(
            entity_name=entity.get("entity_name", ""),
            entity_volume=entity.get("entity_volume", {}),
            table_type=entity.get("table_type"),
            columns=entity.get("columns"),
            nl_description=entity.get("nl_description"),
        )
        for entity in entities_with_volumes
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = {}
    for entity, result in zip(entities_with_volumes, results):
        key = entity.get("entity_name", "")
        if isinstance(result, Exception):
            logger.error(f"Partitioning strategy failed for {key}: {result}")
            output[key] = {
                "needs_partitioning": False,
                "partitioning_strategy": None,
                "partition_key": None,
                "partition_definition": None,
                "reasoning": f"Strategy determination failed: {str(result)}"
            }
        else:
            output[key] = result
    
    return output

