"""Feasibility validation tools for foreign keys, queries, partitioning, and generators."""

from typing import Dict, Any
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException

from .existence import _check_entity_exists_impl


@tool
def check_foreign_key_feasibility(
    from_entity: str,
    to_entity: str,
    schema_state: Dict[str, Any]
) -> Dict[str, Any]:
    """Check if a foreign key relationship is feasible.
    
    Args:
        from_entity: Source entity name
        to_entity: Target entity name
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'feasible' (bool), 'reason' (str), and details
    """
    # Check both entities exist
    from_exists = _check_entity_exists_impl(from_entity, schema_state)
    to_exists = _check_entity_exists_impl(to_entity, schema_state)
    
    if not from_exists:
        return {
            "feasible": False,
            "reason": f"Source entity '{from_entity}' does not exist"
        }
    
    if not to_exists:
        return {
            "feasible": False,
            "reason": f"Target entity '{to_entity}' does not exist"
        }
    
    # Check if target entity has a primary key
    primary_keys = schema_state.get("primary_keys", {})
    to_pk = primary_keys.get(to_entity, [])
    
    if not to_pk:
        return {
            "feasible": False,
            "reason": f"Target entity '{to_entity}' has no primary key defined"
        }
    
    return {
        "feasible": True,
        "reason": "Both entities exist and target has a primary key",
        "target_pk": to_pk
    }


@tool
def check_generator_exists(generator_type: str) -> bool:
    """Check if a data generator type is available.
    
    Args:
        generator_type: Generator type name (e.g., "faker.name", "mimesis.address")
        
    Returns:
        True if generator is available, False otherwise
    """
    # Common generator types
    valid_generators = [
        "faker.name", "faker.first_name", "faker.last_name", "faker.email",
        "faker.address", "faker.city", "faker.state", "faker.zipcode",
        "faker.company", "faker.job", "faker.phone_number",
        "faker.date_time", "faker.date_between",
        "mimesis.name", "mimesis.address",
        "uuid4", "random_int", "random_float",
    ]
    
    return generator_type.lower() in [g.lower() for g in valid_generators]


@tool
def check_partition_feasibility(table: str, strategy: str, schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Check if a partitioning strategy is feasible for a table.
    
    Args:
        table: Table name
        strategy: Partitioning strategy ("range", "hash", "list")
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'feasible' (bool) and 'reason' (str)
    """
    valid_strategies = ["range", "hash", "list"]
    if strategy.lower() not in valid_strategies:
        return {
            "feasible": False,
            "reason": f"Invalid partitioning strategy '{strategy}'. Must be one of: {valid_strategies}"
        }
    
    # Check if table exists
    if not _check_entity_exists_impl(table, schema_state):
        return {
            "feasible": False,
            "reason": f"Table '{table}' does not exist"
        }
    
    return {
        "feasible": True,
        "reason": f"Partitioning strategy '{strategy}' is feasible for table '{table}'"
    }

