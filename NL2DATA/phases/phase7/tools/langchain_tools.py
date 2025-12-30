"""LangChain tool creation from tool definitions."""

from typing import List, Dict, Any, Type, Optional
from langchain_core.tools import StructuredTool
from pydantic import BaseModel as PydanticBaseModel, Field, create_model

from NL2DATA.phases.phase7.tools.catalog import GENERATION_TOOL_CATALOG, GenerationToolDefinition
from NL2DATA.phases.phase7.tools.mapping import (
    get_tools_for_column,
    get_allowed_strategy_kinds_for_column,
    create_strategy_from_tool_call,
)
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def _map_parameter_type_to_python(type_str: str) -> Type:
    """Map JSON schema type to Python type."""
    mapping = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": List,
        "object": Dict[str, Any],
    }
    return mapping.get(type_str, str)


def create_generation_tool_from_definition(tool_def: GenerationToolDefinition) -> StructuredTool:
    """
    Convert a tool definition to a LangChain StructuredTool.
    
    Args:
        tool_def: Tool definition from catalog
        
    Returns:
        LangChain StructuredTool instance
    """
    # Build Pydantic model for tool arguments from parameters
    fields = {}
    for param in tool_def.parameters:
        field_type = _map_parameter_type_to_python(param.type)
        field_kwargs = {"description": param.description}
        
        if not param.required:
            field_kwargs["default"] = param.default
        if param.enum:
            # For enum, use Literal type
            from typing import Literal
            field_type = Literal[tuple(param.enum)]
        
        fields[param.name] = (field_type, Field(**field_kwargs))
    
    # Create Pydantic model for arguments
    ArgsModel = create_model(f"{tool_def.name}_Args", **fields)
    
    # Tool function that validates parameters and returns success message
    def tool_func(args: ArgsModel) -> str:
        """
        Tool function that validates strategy parameters.
        
        Returns a success message if parameters are valid.
        The actual strategy creation happens later when we extract the tool call.
        """
        try:
            # Convert args to dict
            if hasattr(args, "model_dump"):
                args_dict = args.model_dump(exclude_none=True)
            elif hasattr(args, "dict"):
                args_dict = args.dict(exclude_none=True)
            else:
                args_dict = dict(args)
            
            # Validate by creating strategy instance (but don't keep it)
            create_strategy_from_tool_call(tool_def.name, args_dict)
            
            # Return success message
            return f"Strategy {tool_def.name} selected with parameters: {args_dict}"
        except Exception as e:
            error_msg = f"Invalid parameters for {tool_def.name}: {str(e)}"
            logger.error(f"Tool {tool_def.name} validation failed: {error_msg}")
            return error_msg
    
    # Create LangChain tool
    return StructuredTool.from_function(
        func=tool_func,
        name=tool_def.name,
        description=tool_def.description,
        args_schema=ArgsModel,
    )


def get_langchain_tools_for_column(
    sql_type: str,
    is_categorical: bool = False,
    is_boolean: bool = False,
) -> List[StructuredTool]:
    """
    Get available LangChain tools for a column based on its type.
    
    Args:
        sql_type: SQL data type
        is_categorical: Whether the column is categorical
        is_boolean: Whether the column is boolean
        
    Returns:
        List of LangChain StructuredTool instances the LLM can call
    """
    tool_names = get_tools_for_column(sql_type, is_categorical, is_boolean)
    
    tools = []
    for tool_name in tool_names:
        if tool_name in GENERATION_TOOL_CATALOG:
            tool_def = GENERATION_TOOL_CATALOG[tool_name]
            try:
                tool = create_generation_tool_from_definition(tool_def)
                tools.append(tool)
            except Exception as e:
                logger.warning(f"Failed to create LangChain tool for {tool_name}: {e}")
    
    return tools

