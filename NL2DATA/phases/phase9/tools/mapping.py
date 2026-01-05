"""Tool-to-strategy mapping and column type filtering."""

from typing import Dict, List, Any, Optional, Type
from pydantic import create_model, Field

from NL2DATA.phases.phase9.strategies.base import BaseGenerationStrategy
from NL2DATA.phases.phase9.strategies.distributions import (
    NormalDistribution,
    LognormalDistribution,
    UniformDistribution,
    ParetoDistribution,
    ZipfDistribution,
    ExponentialDistribution,
    CategoricalDistribution,
    BernoulliDistribution,
)
from NL2DATA.phases.phase9.strategies.faker_strategies import (
    FakerNameStrategy,
    FakerEmailStrategy,
    FakerAddressStrategy,
    FakerCompanyStrategy,
    FakerTextStrategy,
    FakerURLStrategy,
    FakerPhoneStrategy,
)
from NL2DATA.phases.phase9.strategies.mimesis_strategies import (
    MimesisNameStrategy,
    MimesisEmailStrategy,
    MimesisTextStrategy,
    MimesisAddressStrategy,
    MimesisCoordinatesStrategy,
    MimesisCountryStrategy,
)
from NL2DATA.phases.phase9.strategies.regex_strategy import RegexStrategy
from NL2DATA.phases.phase9.tools.catalog import GENERATION_TOOL_CATALOG

# ============================================================================
# TOOL â†’ STRATEGY CLASS MAPPING
# ============================================================================

TOOL_TO_STRATEGY_MAP: Dict[str, Type[BaseGenerationStrategy]] = {
    "generate_normal": NormalDistribution,
    "generate_lognormal": LognormalDistribution,
    "generate_uniform": UniformDistribution,
    "generate_pareto": ParetoDistribution,
    "generate_zipf": ZipfDistribution,
    "generate_exponential": ExponentialDistribution,
    "generate_categorical": CategoricalDistribution,
    "generate_bernoulli": BernoulliDistribution,
    "generate_faker_name": FakerNameStrategy,
    "generate_faker_email": FakerEmailStrategy,
    "generate_faker_address": FakerAddressStrategy,
    "generate_faker_company": FakerCompanyStrategy,
    "generate_faker_text": FakerTextStrategy,
    "generate_faker_url": FakerURLStrategy,
    "generate_faker_phone": FakerPhoneStrategy,
    "generate_mimesis_name": MimesisNameStrategy,
    "generate_mimesis_email": MimesisEmailStrategy,
    "generate_mimesis_text": MimesisTextStrategy,
    "generate_mimesis_address": MimesisAddressStrategy,
    "generate_mimesis_coordinates": MimesisCoordinatesStrategy,
    "generate_mimesis_country": MimesisCountryStrategy,
    "generate_regex": RegexStrategy,
}


def create_strategy_from_tool_call(tool_name: str, tool_args: Dict[str, Any]) -> BaseGenerationStrategy:
    """
    Create a strategy instance from a tool call.
    
    Args:
        tool_name: Name of the tool that was called
        tool_args: Arguments passed to the tool (from LLM)
        
    Returns:
        Instantiated strategy with generate method
    """
    strategy_class = TOOL_TO_STRATEGY_MAP.get(tool_name)
    if not strategy_class:
        raise ValueError(f"Unknown tool name: {tool_name}")
    
    # Handle special parameter name mappings
    # Pydantic field is "lambda_" with alias="lambda", so we need to use "lambda" as the key
    # LLM might use "lambda" or "lambda_", normalize to "lambda" (the alias)
    if "lambda_" in tool_args and "lambda" not in tool_args:
        tool_args["lambda"] = tool_args.pop("lambda_")
    # If both are present, prefer "lambda" (the alias)
    if "lambda_" in tool_args and "lambda" in tool_args:
        del tool_args["lambda_"]
    
    # Create strategy instance
    try:
        strategy = strategy_class(**tool_args)
        return strategy
    except Exception as e:
        raise ValueError(f"Failed to create strategy from tool call {tool_name} with args {tool_args}: {e}")


def get_allowed_strategy_kinds_for_column(
    sql_type: str,
    is_categorical: bool = False,
    is_boolean: bool = False,
) -> List[str]:
    """
    Determine which strategy kinds are allowed for a column based on its type.
    
    Args:
        sql_type: SQL data type (e.g., "INTEGER", "VARCHAR(255)")
        is_categorical: Whether the column is categorical
        is_boolean: Whether the column is boolean
        
    Returns:
        List of allowed strategy kinds
    """
    sql_type_upper = sql_type.upper()
    
    # Boolean columns: only distribution (bernoulli)
    if is_boolean or sql_type_upper in ("BOOLEAN", "BOOL", "TINYINT(1)"):
        return ["distribution"]
    
    # Numerical columns: only distribution
    if sql_type_upper in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", 
                          "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
        return ["distribution"]
    
    # Text columns: string strategies (and distribution if categorical)
    if sql_type_upper.startswith(("VARCHAR", "CHAR", "TEXT", "STRING")):
        if is_categorical:
            return ["distribution", "string"]
        return ["string"]
    
    # Date/time columns: datetime and distribution
    if sql_type_upper in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
        return ["datetime", "distribution"]
    
    # Default: allow all kinds
    return ["distribution", "string", "location", "datetime"]


def get_tools_for_column(
    sql_type: str,
    is_categorical: bool = False,
    is_boolean: bool = False,
) -> List[str]:
    """
    Get available tool names for a column based on its type.
    
    Args:
        sql_type: SQL data type
        is_categorical: Whether the column is categorical
        is_boolean: Whether the column is boolean
        
    Returns:
        List of tool names (strings) that can be used for this column
    """
    allowed_kinds = get_allowed_strategy_kinds_for_column(sql_type, is_categorical, is_boolean)
    
    tools = []
    for tool_name, tool_def in GENERATION_TOOL_CATALOG.items():
        if tool_def.kind in allowed_kinds:
            tools.append(tool_name)
    
    return tools

