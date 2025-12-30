"""Helper functions for parsing and validating LLM outputs."""

from typing import TypeVar, Type, Optional
from pydantic import BaseModel, ValidationError

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.error_feedback import NoneOutputError, NoneFieldError
from NL2DATA.utils.llm.model_validation import validate_no_none_fields

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


def validate_and_return_parsed(
    parsed_result: T,
    output_schema: Type[T],
) -> T:
    """
    Validate parsed result for None values and return.
    
    Args:
        parsed_result: Parsed Pydantic model instance
        output_schema: Expected Pydantic model class
        
    Returns:
        Validated parsed result
        
    Raises:
        NoneOutputError: If result is None
        NoneFieldError: If required fields have None values
    """
    if parsed_result is None:
        raise NoneOutputError(f"Parsed result is None. Expected {output_schema.__name__} instance.")
    
    # Validate no None values in required fields
    try:
        validate_no_none_fields(parsed_result, output_schema)
    except NoneFieldError as e:
        # Re-raise with better context
        raise NoneFieldError(
            e.args[0] if e.args else "None values found in required fields",
            e.none_fields,
            e.model_name
        ) from e
    
    return parsed_result


def parse_from_json_string(
    json_str: str,
    output_schema: Type[T],
) -> T:
    """
    Parse JSON string into Pydantic model.
    
    Args:
        json_str: JSON string to parse
        output_schema: Pydantic model class
        
    Returns:
        Parsed Pydantic model instance
        
    Raises:
        ValidationError: If JSON cannot be parsed into schema
        NoneOutputError: If parsed result is None
        NoneFieldError: If required fields have None values
    """
    parsed_result = output_schema.model_validate_json(json_str)
    return validate_and_return_parsed(parsed_result, output_schema)


def parse_from_dict(
    data: dict,
    output_schema: Type[T],
) -> T:
    """
    Parse dictionary into Pydantic model.
    
    Args:
        data: Dictionary to parse
        output_schema: Pydantic model class
        
    Returns:
        Parsed Pydantic model instance
        
    Raises:
        ValidationError: If dict cannot be parsed into schema
        NoneOutputError: If parsed result is None
        NoneFieldError: If required fields have None values
    """
    parsed_result = output_schema(**data)
    return validate_and_return_parsed(parsed_result, output_schema)

