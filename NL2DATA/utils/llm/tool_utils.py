"""Tool-related utility functions.

Handles tool name/argument extraction, tool call ID extraction,
and tool error message formatting for LLM feedback.
"""

from typing import Any, Dict
from langchain_core.tools import BaseTool
import json

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def extract_tool_name(tool_call: Any) -> str:
    """Extract tool name from tool call object or dict."""
    if isinstance(tool_call, dict):
        return tool_call.get("name", "")
    else:
        return getattr(tool_call, "name", "")


def extract_tool_args(tool_call: Any) -> tuple[Dict[str, Any], Any]:
    """
    Extract tool arguments from tool call object or dict.
    
    Returns:
        Tuple of (normalized_args_dict, original_args) for error tracking
    """
    # Extract args from dict or object
    if isinstance(tool_call, dict):
        args = tool_call.get("args", {})
    else:
        args = getattr(tool_call, "args", {})
    
    original_args = args  # Preserve original for error messages
    
    # Handle case where args might be a string (JSON string)
    if isinstance(args, str):
        try:
            args = json.loads(args)
            original_args = args  # Update original after parsing
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Tool call args is a string but not valid JSON: {args}")
            return {}, original_args
    
    # Handle case where args is a list (LLM provided parameter names instead of dict)
    # This is an error - we can't use it, but we'll log it clearly
    if isinstance(args, list):
        logger.error(
            f"Tool call args provided as list instead of dict: {args}. "
            f"This indicates the LLM formatted tool arguments incorrectly. "
            f"Expected format: {{'param_name': value}}, got: {args}"
        )
        # Return empty dict but preserve original list for error messages
        return {}, original_args
    
    # Ensure args is a dict
    if not isinstance(args, dict):
        logger.warning(f"Tool call args is not a dict: {type(args)}, value: {args}")
        return {}, original_args
    
    return args, original_args


def extract_tool_call_id(tool_call: Any) -> str:
    """Extract tool call ID from tool call object or dict."""
    if isinstance(tool_call, dict):
        return tool_call.get("id", "")
    else:
        return getattr(tool_call, "id", "")


def get_tool_name(tool: BaseTool) -> str:
    """Get tool name from tool instance."""
    if hasattr(tool, "name"):
        return tool.name
    elif hasattr(tool, "__name__"):
        return tool.__name__
    else:
        return ""


def format_tool_error_for_llm(
    tool_name: str,
    error_message: str,
    provided_args: Any,
    tool_args: Dict[str, Any],
) -> str:
    """
    Format tool error message for LLM feedback.
    
    Provides immediate, actionable feedback to help LLM correct tool call format.
    """
    error_msg = f"ERROR: Tool '{tool_name}' failed to execute.\n\n"
    error_msg += f"Error: {error_message}\n\n"
    
    # Check if args were in wrong format
    if isinstance(provided_args, list) and provided_args:
        if isinstance(provided_args[0], str):
            error_msg += "CRITICAL FORMAT ERROR: You provided arguments as a LIST of parameter names!\n"
            error_msg += f"What you provided: {provided_args} (this is just parameter names, not values)\n"
            error_msg += "What is required: A dictionary/object with parameter names as keys and their values\n\n"
            
            # Provide specific examples based on tool name
            if "component" in tool_name.lower() or "schema" in tool_name.lower():
                error_msg += "CORRECT FORMAT for this tool:\n"
                error_msg += "{\n"
                error_msg += '  "component_type": "table",\n'
                error_msg += '  "name": "Customer"\n'
                error_msg += "}\n\n"
                error_msg += "WRONG FORMAT (what you did):\n"
                error_msg += f'["component_type", "name"]\n\n'
            elif "entities" in tool_name.lower():
                error_msg += "CORRECT FORMAT for this tool:\n"
                error_msg += '{"entities": ["Customer", "Order"]}\n\n'
                error_msg += "WRONG FORMAT (what you did):\n"
                error_msg += '["entities"]\n\n'
            elif "sql" in tool_name.lower():
                error_msg += "CORRECT FORMAT for this tool:\n"
                error_msg += '{"sql": "SELECT * FROM Customer;"}\n\n'
                error_msg += "WRONG FORMAT (what you did):\n"
                error_msg += '["sql"]\n\n'
            
            error_msg += "REMEMBER: Tool arguments MUST be a JSON object (dictionary), not a list!\n"
            error_msg += "Format: {\"parameter_name\": value}\n"
            error_msg += "NOT: [\"parameter_name\"]\n"
    elif isinstance(provided_args, dict) and not provided_args:
        error_msg += "ERROR: No arguments were provided to the tool.\n"
        error_msg += "You must provide all required arguments as a dictionary.\n"
    elif isinstance(provided_args, dict):
        error_msg += f"Arguments provided: {list(provided_args.keys())}\n"
        error_msg += "The tool may be missing required arguments or the arguments may be invalid.\n"
    
    error_msg += "\nPlease fix your tool call and try again with the correct format."
    return error_msg

