"""Error feedback utilities for LLM retry logic.

Provides structured error feedback to LLMs when retrying failed calls,
helping them learn from mistakes and improve output quality.
"""

from typing import TypeVar, Type, Dict, Any, Optional, List
from pydantic import BaseModel, ValidationError
from langchain_core.exceptions import OutputParserException
import json

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.error_feedback_helpers import (
    format_missing_fields_feedback,
    format_wrong_fields_feedback,
    format_type_errors_feedback,
    format_json_parse_error_feedback,
    format_none_fields_feedback,
    format_empty_output_feedback,
    format_tool_call_errors_feedback,
    format_schema_reference,
)

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


class NoneOutputError(ValueError):
    """Raised when LLM returns None or empty output when output is expected."""
    pass


class NoneFieldError(ValueError):
    """Raised when a Pydantic model has None values in required fields."""
    
    def __init__(self, message: str, none_fields: List[str], model_name: str):
        super().__init__(message)
        self.none_fields = none_fields
        self.model_name = model_name


def format_error_feedback(
    error_info: Dict[str, Any],
    output_schema: Optional[Type[T]],  # Make optional
    attempt_number: int,
    tool_call_errors: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Format error information into feedback message for LLM.
    
    Args:
        error_info: Dictionary with error details (from extract_error_details)
        output_schema: Pydantic model class for expected output
        attempt_number: Current retry attempt number
        
    Returns:
        Formatted error feedback string to include in prompt
    """
    error_type = error_info.get("error_type", "Unknown")
    error_msg = error_info.get("error_message", "")
    error_details = error_info.get("error_details", {})
    
    feedback = f"""PREVIOUS ATTEMPT #{attempt_number} FAILED - PLEASE CORRECT YOUR OUTPUT

Error Type: {error_type}
Error Message: {error_msg}

"""
    
    # Add schema-specific feedback using helper functions
    if "missing_fields" in error_details:
        feedback = format_missing_fields_feedback(
            error_details["missing_fields"],
            feedback,
        )
    
    if "wrong_fields" in error_details:
        feedback = format_wrong_fields_feedback(
            error_details["wrong_fields"],
            output_schema,
            feedback,
        )
    
    if "type_errors" in error_details:
        feedback = format_type_errors_feedback(
            error_details["type_errors"],
            feedback,
        )
    
    if "json_parse_error" in error_details:
        feedback = format_json_parse_error_feedback(
            error_details,
            feedback,
        )
    
    if "none_fields" in error_details:
        feedback = format_none_fields_feedback(
            error_details["none_fields"],
            feedback,
        )
    
    if "empty_output" in error_details or "none_output" in error_details:
        feedback = format_empty_output_feedback(feedback)
    
    # Add tool call error guidance
    feedback = format_tool_call_errors_feedback(tool_call_errors or [], feedback)
    
    # Add schema reference
    feedback = format_schema_reference(output_schema, feedback)
    
    feedback += """CORRECT OUTPUT FORMAT:
- Return ONLY a valid JSON object
- No markdown, no code blocks, no explanatory text
- Match the schema exactly (correct field names, correct types)
- Start with { and end with }

Please correct your output based on the errors above."""
    
    return feedback


def extract_error_details(
    error: Exception,
    output_schema: Optional[Type[T]],  # Make optional
    raw_output: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Extract structured error details from exception.
    
    Args:
        error: Exception that occurred
        output_schema: Optional Pydantic model class for expected output (None if unavailable)
        raw_output: Optional raw output string from LLM
        
    Returns:
        Dictionary with error details for feedback
    """
    details = {}
    
    if isinstance(error, NoneOutputError):
        # Specific error for None/empty outputs
        details["none_output"] = True
        details["empty_output"] = True
        if raw_output:
            details["raw_output"] = str(raw_output)[:500]
    
    elif isinstance(error, ValidationError):
        missing_fields = []
        wrong_fields = []
        type_errors = []
        
        for err in error.errors():
            error_type = err.get("type", "")
            error_loc = err.get("loc", ())
            field_name = error_loc[-1] if error_loc else None
            
            if error_type == "missing":
                if field_name:
                    missing_fields.append(str(field_name))
            elif error_type == "value_error":
                # Check for common field name mistakes (only if schema available)
                if output_schema is not None:
                    try:
                        error_msg = str(err.get("msg", "")).lower()
                        if "relationships" in error_msg and "relations" in output_schema.model_fields:
                            wrong_fields.append("relationships")
                            details["suggested_correction"] = "Use 'relations' instead of 'relationships'"
                    except Exception:
                        pass
            elif error_type in ("type_error", "value_error.type"):
                # Type mismatch
                if field_name:
                    expected_type = err.get("ctx", {}).get("expected_type", "unknown")
                    actual_value = err.get("input", None)
                    type_errors.append((str(field_name), str(expected_type), actual_value))
        
        if missing_fields:
            details["missing_fields"] = missing_fields
        if wrong_fields:
            details["wrong_fields"] = wrong_fields
        if type_errors:
            details["type_errors"] = type_errors
    
    elif isinstance(error, OutputParserException):
        details["json_parse_error"] = True
        if hasattr(error, "llm_output") and error.llm_output:
            details["raw_output"] = str(error.llm_output)[:500]
        elif raw_output:
            details["raw_output"] = raw_output[:500]
    
    elif isinstance(error, NoneFieldError):
        # Specific error for None values in fields
        details["none_fields"] = error.none_fields
        details["none_field_error"] = True
        details["model_name"] = error.model_name
        if raw_output:
            details["raw_output"] = raw_output[:500]
    
    elif isinstance(error, (ValueError, json.JSONDecodeError)):
        error_msg = str(error).lower()
        if "empty" in error_msg or "no output" in error_msg or "none" in error_msg:
            details["empty_output"] = True
            details["none_output"] = True
        elif "json" in error_msg or "parse" in error_msg:
            details["json_parse_error"] = True
            if raw_output:
                details["raw_output"] = raw_output[:500]
    
    return details


def create_error_feedback_message(
    error: Exception,
    output_schema: Optional[Type[T]],  # Make optional
    attempt_number: int,
    raw_output: Optional[str] = None,
    tool_call_errors: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Create error feedback message for LLM retry.
    
    This is the main function to call when creating error feedback.
    Handles cases where output_schema is None gracefully.
    
    Args:
        error: Exception that occurred
        output_schema: Optional Pydantic model class for expected output (None if unavailable)
        attempt_number: Current retry attempt number
        raw_output: Optional raw output string from LLM
        
    Returns:
        Formatted error feedback string
    """
    try:
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_details": extract_error_details(error, output_schema, raw_output),
        }
        
        return format_error_feedback(error_info, output_schema, attempt_number, tool_call_errors)
    except Exception as e:
        # Fallback: if error feedback creation fails, provide basic feedback
        logger.warning(f"Failed to create detailed error feedback: {e}. Using fallback feedback.")
        return f"""PREVIOUS ATTEMPT #{attempt_number} FAILED

Error Type: {type(error).__name__}
Error Message: {str(error)[:500]}

Please correct your output. Ensure you return valid JSON matching the required schema.
Return ONLY the raw JSON object, no markdown, no code blocks, no explanatory text.
DO NOT return None or empty output."""

