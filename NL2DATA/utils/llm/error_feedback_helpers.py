"""Helper functions for formatting specific error types in error feedback."""

from typing import TypeVar, Type, Optional, Dict, Any, List
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def format_missing_fields_feedback(
    missing_fields: List[str],
    feedback: str,
) -> str:
    """Format feedback for missing required fields."""
    feedback += f"ERROR Missing required fields: {', '.join(missing_fields)}\n"
    feedback += f"   These fields are REQUIRED and must be included in your output.\n"
    
    # Special handling for attribute_types field (common issue with empty dicts)
    if "attribute_types" in missing_fields:
        feedback += f"\n   CRITICAL: The 'attribute_types' field is a REQUIRED dictionary that MUST NOT be empty.\n"
        feedback += f"   You returned an empty dictionary {{}} which is INVALID.\n"
        feedback += f"   The 'attribute_types' dictionary MUST contain at least one entry with the attribute name as the key.\n"
        feedback += f"   Example: {{\"attribute_types\": {{\"customer_name\": {{\"type\": \"VARCHAR\", \"size\": 255, ...}}}}}}\n"
        feedback += f"   NOT: {{\"attribute_types\": {{}}}} (empty dict is INVALID)\n"
        feedback += f"   NOT: {{}} (missing field is INVALID)\n\n"
    else:
        feedback += "\n"
    
    return feedback


def format_wrong_fields_feedback(
    wrong_fields: List[str],
    output_schema: Optional[Type[T]],
    feedback: str,
) -> str:
    """Format feedback for incorrect field names."""
    feedback += f"ERROR Incorrect field names used: {', '.join(wrong_fields)}\n"
    # Try to suggest correct field names (only if schema available)
    if output_schema is not None:
        try:
            schema_fields = list(output_schema.model_fields.keys())
            feedback += f"   Expected field names: {', '.join(schema_fields)}\n"
        except Exception:
            pass
    feedback += f"   Please use the EXACT field names from the schema.\n\n"
    return feedback


def format_type_errors_feedback(
    type_errors: List[tuple],
    feedback: str,
) -> str:
    """Format feedback for type errors."""
    feedback += "ERROR Type errors:\n"
    for field, expected_type, actual_value in type_errors:
        feedback += f"   - Field '{field}': Expected {expected_type}, got {type(actual_value).__name__}\n"
    feedback += "\n"
    return feedback


def format_json_parse_error_feedback(
    error_details: Dict[str, Any],
    feedback: str,
) -> str:
    """Format feedback for JSON parsing errors."""
    feedback += "ERROR JSON parsing failed.\n"
    feedback += f"   Your output must be ONLY valid JSON, with no markdown formatting, no code blocks, no explanatory text.\n"
    feedback += f"   Do NOT use ```json``` blocks or any markdown.\n"
    feedback += f"   Return ONLY the raw JSON object starting with {{ and ending with }}.\n\n"
    
    raw_output = error_details.get("raw_output", "")
    if raw_output:
        feedback += f"   Your previous output (first 200 chars): {raw_output[:200]}...\n\n"
    return feedback


def format_none_fields_feedback(
    none_fields: List[str],
    feedback: str,
) -> str:
    """Format feedback for None values in required fields."""
    feedback += f"ERROR None values found in required fields: {', '.join(none_fields)}\n"
    feedback += f"   These fields MUST have non-None values.\n"
    feedback += f"   None values are NOT acceptable for these fields.\n\n"
    return feedback


def format_empty_output_feedback(feedback: str) -> str:
    """Format feedback for empty or None output."""
    feedback += "ERROR Empty or None output received.\n"
    feedback += f"   You MUST return a valid JSON object matching the required schema.\n"
    feedback += f"   Returning None or empty output is NOT acceptable.\n\n"
    return feedback


def format_tool_call_errors_feedback(
    tool_call_errors: List[Dict[str, Any]],
    feedback: str,
) -> str:
    """Format feedback for tool call errors with concrete examples."""
    if not tool_call_errors:
        return feedback
    
    feedback += "ERROR TOOL CALL ERRORS (These occurred during tool execution):\n"
    for tool_error in tool_call_errors:
        tool_name = tool_error.get("tool", "unknown")
        error_msg = tool_error.get("error", "unknown error")
        provided_args = tool_error.get("provided_args", [])
        error_message = tool_error.get("error_message", error_msg)
        
        feedback += f"\n   Tool: {tool_name}\n"
        feedback += f"   Error: {error_message}\n"
        
        # Check if provided_args is a list (wrong format)
        if isinstance(provided_args, list) and provided_args:
            # Check if it's a list of strings (parameter names) vs list of dicts
            if isinstance(provided_args[0], str):
                feedback += "   ERROR CRITICAL FORMAT ERROR: You provided arguments as a LIST of parameter names.\n"
                feedback += f"      What you provided: {provided_args} (this is just parameter names, not values)\n"
                feedback += f"      What is required: A dictionary/object with parameter names as keys and their values\n"
                feedback += f"      Example for {tool_name}:\n"
                if "entities" in tool_name.lower():
                    feedback += "         WRONG: [\"entities\"]\n"
                    feedback += "         CORRECT: {\"entities\": [\"Customer\", \"Order\"]}\n"
                elif "component" in tool_name.lower() or "schema" in tool_name.lower():
                    feedback += "         WRONG: [\"component_type\", \"name\"]\n"
                    feedback += "         CORRECT: {\"component_type\": \"table\", \"name\": \"Customer\"}\n"
                elif "sql" in tool_name.lower():
                    feedback += "         WRONG: [\"sql\"]\n"
                    feedback += "         CORRECT: {\"sql\": \"SELECT * FROM Customer;\"}\n"
                else:
                    feedback += "         WRONG: [\"param_name\"]\n"
                    feedback += "         CORRECT: {\"param_name\": value}\n"
                feedback += f"      When calling tools, you MUST provide arguments as a JSON object (dictionary), not as a list.\n"
            else:
                feedback += f"   What you provided: {provided_args}\n"
        else:
            feedback += f"   What you provided: {provided_args if provided_args else 'None'}\n"
        
        # Provide specific guidance with concrete examples based on error type
        if "Missing required argument" in error_message or "Missing required arguments" in error_message:
            # Determine which argument is missing and provide concrete example
            if "entities" in error_message.lower() or tool_name in ("verify_entities_exist", "verify_entities_exist_bound"):
                feedback += "\n   WRONG FORMAT (what you did):\n"
                feedback += f"      - Providing arguments as a list: ['entities']\n"
                feedback += f"      - Providing empty arguments: []\n"
                feedback += f"      - Omitting the argument name\n"
                feedback += "\n   CORRECT FORMAT (what you should do):\n"
                feedback += f"      When calling {tool_name}, you MUST provide arguments as a JSON object (dictionary):\n"
                feedback += f"      {{\n"
                feedback += f"        \"entities\": [\"Customer\", \"Order\", \"Book\"]\n"
                feedback += f"      }}\n"
                feedback += "\n      NOT as a list: [\"entities\"] (WRONG)\n"
                feedback += "      NOT as a string: \"entities\" (WRONG)\n"
                feedback += "      But as a dict: {\"entities\": [...]} (CORRECT)\n"
            elif "sql" in error_message.lower() or tool_name in ("validate_query_against_schema", "validate_query_against_schema_bound", "validate_sql_syntax"):
                feedback += "\n   WRONG FORMAT (what you did):\n"
                feedback += f"      - Providing arguments as a list: ['sql']\n"
                feedback += f"      - Providing empty arguments: []\n"
                feedback += f"      - Omitting the argument name\n"
                feedback += "\n   CORRECT FORMAT (what you should do):\n"
                feedback += f"      When calling {tool_name}, you MUST provide arguments as a JSON object (dictionary):\n"
                feedback += f"      {{\n"
                feedback += f"        \"sql\": \"SELECT * FROM Customer;\"\n"
                feedback += f"      }}\n"
                feedback += "\n      NOT as a list: [\"sql\"] (WRONG)\n"
                feedback += "      NOT as a string: \"sql\" (WRONG)\n"
                feedback += "      But as a dict: {\"sql\": \"SELECT ...\"} (CORRECT)\n"
            elif "component" in tool_name.lower() or "schema" in tool_name.lower() or tool_name in ("check_schema_component_exists", "check_schema_component_exists_bound"):
                feedback += "\n   WRONG FORMAT (what you did):\n"
                feedback += f"      - Providing arguments as a list: ['component_type', 'name']\n"
                feedback += f"      - Providing empty arguments: []\n"
                feedback += f"      - Omitting the argument names\n"
                feedback += "\n   CORRECT FORMAT (what you should do):\n"
                feedback += f"      When calling {tool_name}, you MUST provide arguments as a JSON object (dictionary):\n"
                feedback += f"      {{\n"
                feedback += f"        \"component_type\": \"table\",\n"
                feedback += f"        \"name\": \"Customer\"\n"
                feedback += f"      }}\n"
                feedback += "\n      NOT as a list: [\"component_type\", \"name\"] (WRONG)\n"
                feedback += "      NOT as separate strings: \"component_type\", \"name\" (WRONG)\n"
                feedback += "      But as a dict: {\"component_type\": \"table\", \"name\": \"Customer\"} (CORRECT)\n"
                feedback += f"\n      Example calls:\n"
                feedback += f"      - Check if table exists: {{\"component_type\": \"table\", \"name\": \"Customer\"}}\n"
                feedback += f"      - Check if column exists: {{\"component_type\": \"column\", \"name\": \"customer_id\"}}\n"
            else:
                feedback += "\n   CORRECT FORMAT: When calling this tool, provide ALL required arguments as a JSON object (dictionary).\n"
                feedback += f"      Example: {{\"{tool_name}\": {{\"arg_name\": value, \"other_arg\": value}}}}\n"
                feedback += f"      DO NOT provide arguments as a list [\"arg_name\"] or omit parameter names.\n"
                feedback += f"      Arguments must be a dictionary/object with key-value pairs.\n"
        elif "not found" in error_message.lower():
            feedback += "   CORRECT FORMAT: Use the exact tool name as defined in the available tools.\n"
        else:
            feedback += "   CORRECT FORMAT: Ensure tool arguments match the tool's expected schema.\n"
            feedback += f"      Arguments must be provided as a JSON object (dictionary), not as a list or string.\n"
    
    feedback += "\n   CRITICAL RULE: Tool arguments MUST be a JSON object (dictionary) with key-value pairs.\n"
    feedback += f"      Format: {{\"argument_name\": argument_value}}\n"
    feedback += f"      NOT: [\"argument_name\"] or \"argument_name\" or []\n"
    feedback += f"\n   IMPORTANT: After fixing tool calls, you MUST still return your final answer as JSON.\n"
    feedback += f"   Tool calls are for validation/checking - your final output must be the structured JSON response.\n\n"
    return feedback


def format_schema_reference(
    output_schema: Optional[Type[T]],
    feedback: str,
) -> str:
    """Format schema reference for error feedback."""
    import json
    
    if output_schema is not None:
        try:
            schema_json = output_schema.model_json_schema()
            feedback += "REQUIRED OUTPUT SCHEMA:\n"
            feedback += f"{json.dumps(schema_json, indent=2)}\n\n"
        except Exception as e:
            from NL2DATA.utils.logging import get_logger
            logger = get_logger(__name__)
            logger.debug(f"Failed to generate schema JSON for feedback: {e}")
            try:
                feedback += f"REQUIRED OUTPUT SCHEMA: {output_schema.__name__}\n\n"
            except Exception:
                feedback += "REQUIRED OUTPUT SCHEMA: (schema details unavailable)\n\n"
    else:
        feedback += "NOTE: Output schema details are unavailable, but you must return valid JSON.\n\n"
    
    return feedback

