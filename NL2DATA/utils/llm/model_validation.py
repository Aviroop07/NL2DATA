"""Pydantic model validation utilities for detecting None values in fields.

Validates Pydantic model instances to ensure no required fields have None values,
and provides detailed error information for retry feedback.
"""

from typing import TypeVar, Type, List, Any, Dict, Optional
from pydantic import BaseModel, Field
from collections.abc import Sequence, Mapping

from NL2DATA.utils.llm.error_feedback import NoneFieldError
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


def validate_no_none_fields(model_instance: T, output_schema: Type[T]) -> None:
    """
    Validate that a Pydantic model instance has no None values in required fields.
    
    This function recursively checks:
    - Required fields (not Optional) must not be None
    - List items must not be None
    - Dict values must not be None (unless the value type is Optional)
    - Nested Pydantic models are validated recursively
    
    Args:
        model_instance: Pydantic model instance to validate
        output_schema: Pydantic model class (for field metadata)
        
    Raises:
        NoneFieldError: If any required fields have None values
    """
    if model_instance is None:
        raise NoneFieldError(
            "Model instance is None",
            none_fields=["<root>"],
            model_name=output_schema.__name__
        )
    
    none_fields = []
    _check_none_values(model_instance, output_schema, "", none_fields)
    
    if none_fields:
        raise NoneFieldError(
            f"Found None values in required fields: {', '.join(none_fields)}",
            none_fields=none_fields,
            model_name=output_schema.__name__
        )


def _check_none_values(
    value: Any,
    schema: Type[BaseModel],
    field_path: str,
    none_fields: List[str]
) -> None:
    """
    Recursively check for None values in a Pydantic model.
    
    Args:
        value: Value to check (can be model instance, dict, list, etc.)
        schema: Pydantic model class
        field_path: Current field path (for error messages)
        none_fields: List to accumulate None field paths
    """
    if value is None:
        if field_path:
            none_fields.append(field_path)
        return
    
    # If it's a Pydantic model instance, check its fields
    if isinstance(value, BaseModel):
        model_fields = value.model_fields
        for field_name, field_info in model_fields.items():
            field_value = getattr(value, field_name, None)
            new_path = f"{field_path}.{field_name}" if field_path else field_name
            
            # Check if field is required (not Optional)
            is_optional = _is_field_optional(field_info, schema)
            
            if field_value is None and not is_optional:
                none_fields.append(new_path)
            elif field_value is not None:
                # Recursively check nested structures
                _check_nested_value(field_value, new_path, none_fields)
    
    # If it's a dict, check values
    elif isinstance(value, dict):
        for key, val in value.items():
            new_path = f"{field_path}.{key}" if field_path else str(key)
            if val is None:
                none_fields.append(new_path)
            else:
                _check_nested_value(val, new_path, none_fields)
    
    # If it's a list, check items
    elif isinstance(value, (list, tuple)):
        for idx, item in enumerate(value):
            new_path = f"{field_path}[{idx}]" if field_path else f"[{idx}]"
            if item is None:
                none_fields.append(new_path)
            else:
                _check_nested_value(item, new_path, none_fields)


def _check_nested_value(value: Any, field_path: str, none_fields: List[str]) -> None:
    """Check nested values (lists, dicts, Pydantic models) for None."""
    if value is None:
        none_fields.append(field_path)
        return
    
    if isinstance(value, BaseModel):
        # Recursively check nested Pydantic model
        model_fields = value.model_fields
        for field_name, field_info in model_fields.items():
            nested_value = getattr(value, field_name, None)
            new_path = f"{field_path}.{field_name}"
            is_optional = _is_field_optional(field_info, None)
            
            if nested_value is None and not is_optional:
                none_fields.append(new_path)
            elif nested_value is not None:
                _check_nested_value(nested_value, new_path, none_fields)
    
    elif isinstance(value, dict):
        for key, val in value.items():
            new_path = f"{field_path}.{key}"
            if val is None:
                none_fields.append(new_path)
            else:
                _check_nested_value(val, new_path, none_fields)
    
    elif isinstance(value, (list, tuple)):
        for idx, item in enumerate(value):
            new_path = f"{field_path}[{idx}]"
            if item is None:
                none_fields.append(new_path)
            else:
                _check_nested_value(item, new_path, none_fields)


def _is_field_optional(field_info: Any, schema: Optional[Type[BaseModel]]) -> bool:
    """
    Check if a Pydantic field is optional (can be None).
    
    Args:
        field_info: Pydantic FieldInfo object
        schema: Optional schema class (for type checking)
        
    Returns:
        True if field is Optional, False otherwise
    """
    try:
        # Check if field has a default value (including None)
        # In Pydantic v2, default can be PydanticUndefined, Ellipsis, or an actual value
        if hasattr(field_info, 'default'):
            default = field_info.default
            # If default is not Ellipsis and not PydanticUndefined, field is optional
            if default is not ...:
                try:
                    from pydantic_core import PydanticUndefined
                    if default is not PydanticUndefined:
                        return True
                except ImportError:
                    # Fallback for older Pydantic versions
                    if default is not None or str(type(default)) != "<class 'pydantic.fields.UndefinedType'>":
                        return True
        
        # Check if default_factory exists
        if hasattr(field_info, 'default_factory') and field_info.default_factory is not None:
            return True
        
        # Check annotation for Optional type
        if hasattr(field_info, 'annotation'):
            annotation = field_info.annotation
            annotation_str = str(annotation)
            
            # Check for Optional or Union[..., None]
            if 'Optional' in annotation_str or 'Union' in annotation_str:
                # Check if None is in the Union
                if 'None' in annotation_str or 'NoneType' in annotation_str:
                    return True
            
            # Check using typing.get_args for Union types
            try:
                from typing import get_args, get_origin
                origin = get_origin(annotation)
                if origin is not None:
                    args = get_args(annotation)
                    # Check if NoneType is in the args
                    if type(None) in args:
                        return True
            except Exception:
                pass
        
        # Check if field is required (no default)
        # In Pydantic v2, use is_required() method
        if hasattr(field_info, 'is_required'):
            if callable(field_info.is_required):
                return not field_info.is_required()
            else:
                return not field_info.is_required
        
        # Default: assume required if no default value
        return False
    except Exception as e:
        logger.debug(f"Failed to determine if field is optional: {e}")
        # Conservative: assume required if we can't determine
        return False

