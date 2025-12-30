"""Tool execution, validation, and error handling.

Handles the execution of individual tools, including argument validation,
error handling, and fallback mechanisms.
"""

from typing import Any, Dict, List, Optional, Tuple
from langchain_core.tools import BaseTool
import asyncio
import inspect

from NL2DATA.utils.llm.tool_utils import extract_tool_name, extract_tool_args, get_tool_name
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


async def execute_tool_call(tool_call: Any, structured_tools: List[BaseTool]) -> Tuple[Any, Optional[str]]:
    """
    Execute a single tool call.
    
    Args:
        tool_call: Tool call object or dict from LLM
        structured_tools: List of available tools
        
    Returns:
        Tuple of (tool execution result, error_message if error occurred)
    """
    # Extract tool call information
    tool_name = extract_tool_name(tool_call)
    tool_args, _ = extract_tool_args(tool_call)  # Ignore original_args here, only need normalized
    
    # Find tool in available tools
    for tool in structured_tools:
        tool_name_attr = get_tool_name(tool)
        
        if tool_name_attr == tool_name:
            # Found tool, execute it
            return await execute_single_tool(tool, tool_name, tool_args)
    
    # Tool not found
    error_msg = f"Tool '{tool_name}' not found in available tools."
    logger.warning(f"Tool '{tool_name}' not found for execution.")
    return error_msg, f"Tool '{tool_name}' not found"


async def execute_single_tool(
    tool: BaseTool,
    tool_name: str,
    tool_args: Dict[str, Any],
) -> Tuple[Any, Optional[str]]:
    """
    Execute a single tool with validation and error handling.
    
    Args:
        tool: Tool instance to execute
        tool_name: Name of the tool (for logging)
        tool_args: Arguments to pass to the tool
        
    Returns:
        Tuple of (tool_result, tool_error)
    """
    # Convert tool_args to dict if needed
    if not isinstance(tool_args, dict):
        tool_args = dict(tool_args) if hasattr(tool_args, '__dict__') else {}
    
    # Validate required arguments
    validation_error = validate_tool_args(tool, tool_name, tool_args)
    if validation_error:
        return validation_error
    
    # Execute tool
    try:
        tool_result = await invoke_tool(tool, tool_name, tool_args)
        # Check if result is an error message (string indicating failure)
        if isinstance(tool_result, str) and (
            "failed" in tool_result.lower() or 
            tool_result.startswith("Tool") and ("no callable" in tool_result.lower() or "invocation failed" in tool_result.lower())
        ):
            # This is an error message, not a valid result
            return tool_result, tool_result
        return tool_result, None
    except TypeError as e:
        return handle_type_error(e, tool_name, tool_args)
    except Exception as e:
        return handle_tool_error(e, tool_name)


def validate_tool_args(
    tool: BaseTool,
    tool_name: str,
    tool_args: Dict[str, Any],
) -> Optional[Tuple[str, str]]:
    """
    Validate that required tool arguments are present.
    
    Returns:
        Tuple of (error_msg, error_type) if validation fails, None otherwise
    """
    if not (hasattr(tool, 'args_schema') and tool.args_schema):
        return None
    
    try:
        from pydantic import BaseModel
        if not issubclass(tool.args_schema, BaseModel):
            return None
        
        required_fields = {
            name for name, field in tool.args_schema.model_fields.items()
            if field.is_required()
        }
        missing_fields = required_fields - set(tool_args.keys())
        
        if missing_fields:
            error_msg = (
                f"Tool {tool_name} requires the following arguments that were not provided: "
                f"{', '.join(missing_fields)}. "
                f"Provided arguments: {list(tool_args.keys())}. "
                f"Please provide all required arguments with correct parameter names."
            )
            logger.warning(error_msg)
            return (error_msg, f"Missing required arguments: {', '.join(missing_fields)}")
    except Exception:
        pass  # If schema check fails, proceed with tool invocation
    
    return None


def handle_type_error(
    error: TypeError,
    tool_name: str,
    tool_args: Dict[str, Any],
) -> Tuple[str, str]:
    """Handle TypeError from tool execution (usually missing arguments)."""
    error_str = str(error)
    if "missing" in error_str and "required positional argument" in error_str:
        import re
        match = re.search(r"missing \d+ required positional argument[s]?: '?(\w+)'?", error_str)
        missing_arg = match.group(1) if match else "unknown"
        error_msg = (
            f"Tool {tool_name} call failed: Missing required argument '{missing_arg}'. "
            f"Please provide all required arguments. Provided: {list(tool_args.keys())}"
        )
        logger.warning(error_msg)
        return (error_msg, f"Missing required argument: {missing_arg}")
    else:
        error_msg = f"Tool error: {error_str}"
        logger.warning(f"Tool {tool_name} failed: {error}")
        import traceback
        logger.debug(traceback.format_exc())
        return (error_msg, error_str)


def handle_tool_error(
    error: Exception,
    tool_name: str,
) -> Tuple[str, str]:
    """Handle general exceptions from tool execution."""
    error_msg = f"Tool error: {str(error)}"
    logger.warning(f"Tool {tool_name} failed: {error}")
    import traceback
    logger.debug(traceback.format_exc())
    return (error_msg, str(error))


async def invoke_tool(tool: BaseTool, tool_name: str, tool_args: Dict[str, Any]) -> Any:
    """
    Invoke a tool using the appropriate method.
    
    Args:
        tool: Tool instance to invoke
        tool_name: Name of the tool (for logging)
        tool_args: Arguments to pass to the tool
        
    Returns:
        Tool execution result
    """
    # Try different invocation methods
    if isinstance(tool, BaseTool):
        # BaseTool (includes StructuredTool) - use invoke/ainvoke
        try:
            # StructuredTool expects arguments as a dict, passed to ainvoke/invoke
            # The dict keys should match the args_schema field names
            if hasattr(tool, 'ainvoke') and callable(getattr(tool, 'ainvoke', None)):
                # For StructuredTool, ainvoke expects a dict with the argument values
                # Log the arguments being passed for debugging
                logger.debug(f"Invoking tool {tool_name} with ainvoke, args: {list(tool_args.keys())}")
                result = await tool.ainvoke(tool_args)
                return result
            elif hasattr(tool, 'invoke') and callable(getattr(tool, 'invoke', None)):
                logger.debug(f"Invoking tool {tool_name} with invoke, args: {list(tool_args.keys())}")
                result = tool.invoke(tool_args)
                return result
            elif hasattr(tool, 'arun') and callable(getattr(tool, 'arun', None)):
                # arun might expect **kwargs
                logger.debug(f"Invoking tool {tool_name} with arun, args: {list(tool_args.keys())}")
                result = await tool.arun(**tool_args)
                return result
            elif hasattr(tool, 'run') and callable(getattr(tool, 'run', None)):
                logger.debug(f"Invoking tool {tool_name} with run, args: {list(tool_args.keys())}")
                result = tool.run(**tool_args)
                return result
            else:
                error_msg = f"Tool {tool_name} has no callable invoke/ainvoke/run/arun method"
                logger.warning(error_msg)
                return error_msg
        except TypeError as type_error:
            # TypeError often means wrong argument format - try fallback
            error_str = str(type_error)
            logger.warning(f"Tool {tool_name} invocation failed with TypeError: {type_error}")
            if "not callable" in error_str or "missing" in error_str.lower():
                logger.debug(f"Trying fallback for {tool_name} due to TypeError")
                return await invoke_tool_fallback(tool, tool_name, tool_args, type_error)
            else:
                # Re-raise other TypeErrors
                raise
        except ValueError as value_error:
            # ValueError might indicate schema validation failure
            error_str = str(value_error)
            logger.warning(f"Tool {tool_name} invocation failed with ValueError: {value_error}")
            if "not callable" in error_str.lower():
                logger.debug(f"Trying fallback for {tool_name} due to ValueError")
                return await invoke_tool_fallback(tool, tool_name, tool_args, value_error)
            else:
                # Re-raise other ValueErrors (might be validation errors we want to see)
                raise
        except Exception as invoke_error:
            # If ainvoke/invoke fails, try accessing underlying function
            error_str = str(invoke_error)
            logger.warning(f"Tool {tool_name} ainvoke/invoke failed: {invoke_error} (type: {type(invoke_error).__name__})")
            if "not callable" in error_str.lower():
                logger.debug(f"Trying fallback for {tool_name} due to 'not callable' error")
                return await invoke_tool_fallback(tool, tool_name, tool_args, invoke_error)
            else:
                # For other errors, try fallback anyway
                logger.debug(f"Trying fallback for {tool_name} due to unexpected error")
                return await invoke_tool_fallback(tool, tool_name, tool_args, invoke_error)
    elif hasattr(tool, 'ainvoke') and callable(getattr(tool, 'ainvoke', None)):
        # Tool has ainvoke method but isn't a BaseTool/StructuredTool
        return await tool.ainvoke(tool_args)
    elif hasattr(tool, 'invoke') and callable(getattr(tool, 'invoke', None)):
        # Tool has invoke method but isn't a BaseTool/StructuredTool
        return tool.invoke(tool_args)
    elif callable(tool):
        # Direct function call
        if asyncio.iscoroutinefunction(tool):
            return await tool(**tool_args)
        else:
            return tool(**tool_args)
    else:
        return f"Tool {tool_name} is not callable"


async def invoke_tool_fallback(tool: BaseTool, tool_name: str, tool_args: Dict[str, Any], original_error: Exception) -> Any:
    """Fallback method to invoke tool when standard methods fail."""
    try:
        # Try to get the underlying function from StructuredTool
        # StructuredTool stores the function in different attributes depending on version
        underlying_func = None
        
        # Try different ways to access the underlying function
        if hasattr(tool, 'func') and callable(tool.func):
            underlying_func = tool.func
            logger.debug(f"Found underlying function for {tool_name} via 'func' attribute")
        elif hasattr(tool, '_func') and callable(tool._func):
            underlying_func = tool._func
            logger.debug(f"Found underlying function for {tool_name} via '_func' attribute")
        elif hasattr(tool, 'function') and callable(tool.function):
            underlying_func = tool.function
            logger.debug(f"Found underlying function for {tool_name} via 'function' attribute")
        
        if underlying_func:
            # Found the underlying function, call it directly
            # Validate arguments match function signature if possible
            try:
                sig = inspect.signature(underlying_func)
                # Check if all required parameters are provided
                # Exclude: self, *args (VAR_POSITIONAL), **kwargs (VAR_KEYWORD)
                required_params = {
                    k: v for k, v in sig.parameters.items() 
                    if (v.default == inspect.Parameter.empty 
                        and k != 'self'
                        and v.kind != inspect.Parameter.VAR_POSITIONAL  # *args
                        and v.kind != inspect.Parameter.VAR_KEYWORD)    # **kwargs
                }
                missing_params = set(required_params.keys()) - set(tool_args.keys())
                if missing_params:
                    error_msg = (
                        f"Tool {tool_name} missing required parameters: {missing_params}. "
                        f"Provided: {list(tool_args.keys())}, Required: {list(required_params.keys())}"
                    )
                    logger.warning(error_msg)
                    return error_msg
            except Exception:
                pass  # If signature inspection fails, proceed anyway
            
            # Call the underlying function
            logger.debug(f"Calling underlying function for {tool_name} with args: {list(tool_args.keys())}")
            if asyncio.iscoroutinefunction(underlying_func):
                result = await underlying_func(**tool_args)
            else:
                result = underlying_func(**tool_args)
            logger.debug(f"Underlying function for {tool_name} returned successfully")
            return result
        
        # If we can't find the underlying function, return an error message
        # Don't try to call tool() directly as StructuredTool is not callable
        args_repr = list(tool_args.keys()) if isinstance(tool_args, dict) and tool_args else "[] (empty or invalid format)"
        error_msg = (
            f"Tool {tool_name} invocation failed: {original_error}. "
            f"Could not access underlying function. "
            f"Tool type: {type(tool)}, has func: {hasattr(tool, 'func')}, "
            f"has _func: {hasattr(tool, '_func')}, has function: {hasattr(tool, 'function')}. "
            f"Args provided: {args_repr}"
        )
        logger.error(error_msg)
        return error_msg
        
    except Exception as fallback_error:
        # Show original args format if available, otherwise show keys
        args_repr = list(tool_args.keys()) if isinstance(tool_args, dict) and tool_args else "[] (empty or invalid format)"
        error_msg = (
            f"All invocation methods failed for {tool_name}. "
            f"Original error: {original_error}. "
            f"Fallback error: {fallback_error}. "
            f"Args provided: {args_repr}"
        )
        logger.error(error_msg)
        return error_msg  # Return error message instead of raising

