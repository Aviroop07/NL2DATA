"""Tool conversion utilities for LangChain agent executors.

Handles conversion of regular Python functions, closures, and bound functions
into LangChain StructuredTool objects that can be used with agent executors.
"""

from typing import Any, List, Optional, Type
from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel
import asyncio
import inspect

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.tool_schemas import TOOL_ARG_SCHEMAS

logger = get_logger(__name__)


def convert_to_structured_tools(tools: List[Any]) -> List[BaseTool]:
    """
    Convert a list of tools (functions, closures, BaseTool instances) into StructuredTool objects.
    
    Args:
        tools: List of tools - can be BaseTool instances, callable functions, closures, etc.
        
    Returns:
        List of BaseTool instances ready for use with agent executors
    """
    structured_tools = []
    
    for tool_item in tools:
        if isinstance(tool_item, BaseTool):
            # Already a BaseTool (includes StructuredTool), use as-is
            structured_tools.append(tool_item)
        elif callable(tool_item):
            # Convert regular function to StructuredTool
            try:
                structured_tool = _convert_function_to_tool(tool_item)
                structured_tools.append(structured_tool)
                logger.debug(f"Successfully converted function {getattr(tool_item, '__name__', 'unknown')} to StructuredTool")
            except Exception as e:
                logger.warning(f"Failed to convert function {getattr(tool_item, '__name__', 'unknown')} to StructuredTool: {e}. Using as-is.")
                # If conversion fails, try to use it as-is (might work if it's already compatible)
                structured_tools.append(tool_item)
        else:
            logger.warning(f"Tool item {tool_item} is not callable and not a BaseTool. Skipping.")
            # Skip non-callable, non-BaseTool items
    
    return structured_tools


def _convert_function_to_tool(tool_item: Any) -> BaseTool:
    """
    Convert a single function to a StructuredTool with proper argument schema.
    
    Args:
        tool_item: Callable function (may be a closure or bound function)
        
    Returns:
        StructuredTool instance with proper argument schema
    """
    # Get function name
    tool_name = tool_item.__name__ if hasattr(tool_item, '__name__') else 'tool'
    
    # Get function description
    tool_description = getattr(tool_item, '__doc__', 'A tool function')
    if not tool_description or tool_description.strip() == '':
        tool_description = f"Tool function: {tool_name}"
    
    # Get argument schema if available
    args_schema: Optional[Type[BaseModel]] = TOOL_ARG_SCHEMAS.get(tool_name)
    
    # Inspect function signature to understand parameters
    try:
        sig = inspect.signature(tool_item)
        param_names = list(sig.parameters.keys())
        logger.debug(f"Function {tool_name} has parameters: {param_names}")
    except Exception as e:
        logger.warning(f"Could not inspect signature for {tool_name}: {e}")
        param_names = []
    
    # Add explicit argument format instructions to description
    # This helps the LLM understand the correct format
    if args_schema:
        # Extract field names from schema
        field_names = list(args_schema.model_fields.keys())
        if field_names:
            format_example = ", ".join([f'"{name}": value' for name in field_names])
            tool_description += f"\n\nCRITICAL: When calling this tool, provide arguments as a JSON object: {{{format_example}}}. DO NOT provide arguments as a list or string."
    elif param_names:
        # If no schema but we have parameter names, add format instructions
        format_example = ", ".join([f'"{name}": value' for name in param_names])
        tool_description += f"\n\nCRITICAL: When calling this tool, provide arguments as a JSON object: {{{format_example}}}. DO NOT provide arguments as a list or string."
    
    # For closures or bound functions, create a wrapper that properly handles the call
    # This ensures the function can be properly invoked by StructuredTool
    if hasattr(tool_item, '__closure__') and tool_item.__closure__:
        # It's a closure - create a wrapper function
        wrapped_func = _create_closure_wrapper(tool_item, tool_name, tool_description)
    else:
        wrapped_func = tool_item
    
    # Create StructuredTool from function with schema
    if args_schema:
        structured_tool = StructuredTool.from_function(
            func=wrapped_func,
            name=tool_name,
            description=tool_description,
            args_schema=args_schema,
        )
        logger.debug(f"Created StructuredTool '{tool_name}' with Pydantic argument schema")
    else:
        # Fallback: create without explicit schema (LangChain will infer from type hints)
        structured_tool = StructuredTool.from_function(
            func=wrapped_func,
            name=tool_name,
            description=tool_description,
        )
        logger.debug(f"Created StructuredTool '{tool_name}' without explicit schema (using type hints)")
    
    # Verify the tool was created correctly
    if not isinstance(structured_tool, BaseTool):
        raise ValueError(f"StructuredTool.from_function did not return a BaseTool, got {type(structured_tool)}")
    
    return structured_tool


def _create_closure_wrapper(original_func: Any, tool_name: str, tool_description: str) -> Any:
    """
    Create a wrapper function for closures to ensure proper invocation.
    
    Args:
        original_func: The original closure/function
        tool_name: Name for the tool
        tool_description: Description for the tool
        
    Returns:
        Wrapped function that can be used with StructuredTool
    """
    # Inspect the original function signature to preserve parameter names
    sig = inspect.signature(original_func)
    param_names = list(sig.parameters.keys())
    
    # Create wrapper that properly handles the function signature
    async def async_wrapper(**kwargs):
        # Filter kwargs to only include parameters that the original function accepts
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in param_names}
        if asyncio.iscoroutinefunction(original_func):
            return await original_func(**filtered_kwargs)
        else:
            return original_func(**filtered_kwargs)
    
    def sync_wrapper(**kwargs):
        # Filter kwargs to only include parameters that the original function accepts
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in param_names}
        return original_func(**filtered_kwargs)
    
    # Return appropriate wrapper based on whether original is async
    if asyncio.iscoroutinefunction(original_func):
        async_wrapper.__name__ = tool_name
        async_wrapper.__doc__ = tool_description
        # Preserve signature for proper tool binding
        async_wrapper.__signature__ = sig
        return async_wrapper
    else:
        sync_wrapper.__name__ = tool_name
        sync_wrapper.__doc__ = tool_description
        # Preserve signature for proper tool binding
        sync_wrapper.__signature__ = sig
        return sync_wrapper

