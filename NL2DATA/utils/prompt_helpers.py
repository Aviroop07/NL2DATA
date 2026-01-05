"""Utility functions for generating prompt-friendly output structure descriptions from Pydantic models.

This module provides functions to automatically generate the "OUTPUT STRUCTURE (REQUIRED)" 
section for system prompts based on Pydantic output schemas.
"""

from typing import Type, Any, get_args, get_origin, Union, Optional
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo


def _get_type_description(annotation: Any, field_info: FieldInfo) -> str:
    """Get a human-readable type description for a field annotation."""
    origin = get_origin(annotation)
    
    # Handle Optional/Union types
    if origin is Union:
        args = get_args(annotation)
        # Check if it's Optional (Union[T, None])
        if len(args) == 2 and type(None) in args:
            non_none_type = next(t for t in args if t is not type(None))
            return _get_type_description(non_none_type, field_info) + " or null"
        # Regular Union - use first non-None type
        non_none_types = [t for t in args if t is not type(None)]
        if non_none_types:
            return _get_type_description(non_none_types[0], field_info)
    
    # Handle List types
    if origin is list:
        inner_type = get_args(annotation)[0] if get_args(annotation) else Any
        inner_desc = _get_type_description(inner_type, field_info)
        return f"list of {inner_desc}"
    
    # Handle Dict types
    if origin is dict:
        key_type, value_type = get_args(annotation) if get_args(annotation) else (Any, Any)
        key_desc = _get_type_description(key_type, field_info)
        value_desc = _get_type_description(value_type, field_info)
        return f"dict mapping {key_desc} to {value_desc}"
    
    # Handle Literal types
    if hasattr(annotation, '__origin__') and annotation.__origin__ is Union:
        # Check if it's a Literal (Union of literal values)
        args = get_args(annotation)
        if all(isinstance(arg, (str, int, float, bool)) for arg in args):
            values = [repr(v) for v in args]
            if len(values) <= 5:
                return f"one of {', '.join(values)}"
            return f"one of {', '.join(values[:3])}... ({len(values)} options)"
    
    # Handle BaseModel (nested models)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return "object"
    
    # Primitive types
    if hasattr(annotation, '__name__'):
        type_name = annotation.__name__
        # Map common types to more readable names
        type_map = {
            'str': 'string',
            'int': 'integer',
            'float': 'number',
            'bool': 'boolean',
            'dict': 'dict',
            'list': 'list',
        }
        return type_map.get(type_name, type_name.lower())
    
    return "any"


def _get_requirement_status(field_info: FieldInfo) -> str:
    """Get the requirement status string for a field."""
    if field_info.is_required():
        return "REQUIRED"
    
    default = field_info.default
    
    # Check for PydanticUndefined (optional field with no default in Pydantic v2)
    try:
        from pydantic_core import PydanticUndefined
        if default is PydanticUndefined:
            return "optional"
    except ImportError:
        # Fallback for older Pydantic versions - check if default is Ellipsis
        if default is ...:
            return "optional"
    
    # Check for Ellipsis (used in some Pydantic v2 patterns)
    if default is ...:
        return "optional"
    
    if default is None:
        return "optional (default: null)"
    elif default == "":
        return "optional (default: empty string)"
    elif isinstance(default, (list, dict)) and len(default) == 0:
        default_type = "empty list" if isinstance(default, list) else "empty dict"
        return f"optional (default: {default_type})"
    else:
        # For other defaults, show the value
        return f"optional (default: {repr(default)})"


def _format_field_description(field_name: str, field_info: FieldInfo, annotation: Any) -> str:
    """Format a single field description for the output structure."""
    type_desc = _get_type_description(annotation, field_info)
    req_status = _get_requirement_status(field_info)
    description = field_info.description or ""
    
    # Build the description string
    desc_parts = [f'"{field_name}": "{type_desc} ({req_status})']
    if description:
        desc_parts.append(f" - {description}")
    desc_parts.append('"')
    
    return " ".join(desc_parts)


def _walk_model(model: Type[BaseModel], indent: int = 0) -> str:
    """Recursively walk a Pydantic model and generate structure description."""
    indent_str = "  " * indent
    lines = []
    field_items = list(model.model_fields.items())
    
    for idx, (name, field) in enumerate(field_items):
        annotation = field.annotation
        origin = get_origin(annotation)
        
        # Handle List of nested models
        if origin is list:
            inner_type = get_args(annotation)[0] if get_args(annotation) else Any
            if isinstance(inner_type, type) and issubclass(inner_type, BaseModel):
                req_status = _get_requirement_status(field)
                desc = field.description or ""
                desc_text = f' - {desc}' if desc else ''
                lines.append(f'{indent_str}"{name}": ["list of objects ({req_status}){desc_text}"]')
                lines.append(f'{indent_str}  Each item in the list has this structure:')
                lines.append(f'{indent_str}  {{')
                nested = _walk_model(inner_type, indent + 2)
                lines.append(nested)
                lines.append(f'{indent_str}  }}')
                if idx < len(field_items) - 1:
                    lines.append('')
                continue
        
        # Handle single nested model
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            req_status = _get_requirement_status(field)
            desc = field.description or ""
            desc_text = f' - {desc}' if desc else ''
            lines.append(f'{indent_str}"{name}": {{')
            lines.append(f'{indent_str}  "object ({req_status}){desc_text}"')
            lines.append(f'{indent_str}  Structure:')
            lines.append(f'{indent_str}  {{')
            nested = _walk_model(annotation, indent + 2)
            lines.append(nested)
            lines.append(f'{indent_str}  }}')
            lines.append(f'{indent_str}}}')
            if idx < len(field_items) - 1:
                lines.append('')
            continue
        
        # Handle Dict types
        if origin is dict:
            key_type, value_type = get_args(annotation) if get_args(annotation) else (Any, Any)
            req_status = _get_requirement_status(field)
            desc = field.description or ""
            
            if isinstance(value_type, type) and issubclass(value_type, BaseModel):
                desc_text = f' - {desc}' if desc else ''
                lines.append(f'{indent_str}"{name}": {{')
                lines.append(f'{indent_str}  "dict ({req_status}){desc_text}"')
                lines.append(f'{indent_str}  Dictionary mapping keys to objects with this structure:')
                lines.append(f'{indent_str}  {{')
                nested = _walk_model(value_type, indent + 2)
                lines.append(nested)
                lines.append(f'{indent_str}  }}')
                lines.append(f'{indent_str}}}')
            else:
                # Simple dict - format as inline description
                type_desc = _get_type_description(annotation, field)
                desc_text = f' - {desc}' if desc else ''
                lines.append(f'{indent_str}"{name}": {{"{type_desc} ({req_status}){desc_text}"}}')
            
            if idx < len(field_items) - 1:
                lines.append('')
            continue
        
        # Simple field
        field_desc = _format_field_description(name, field, annotation)
        lines.append(f'{indent_str}{field_desc}')
        if idx < len(field_items) - 1:
            lines.append('')
    
    return "\n".join(lines)


def generate_output_structure_section(output_schema: Type[BaseModel]) -> str:
    """Generate the OUTPUT STRUCTURE section for a system prompt from a Pydantic model.
    
    Args:
        output_schema: The Pydantic BaseModel class that defines the output structure
        
    Returns:
        A formatted string containing the OUTPUT STRUCTURE section ready to be inserted
        into a system prompt.
    
    Example:
        >>> from pydantic import BaseModel, Field
        >>> class MyOutput(BaseModel):
        ...     name: str = Field(description="Name field")
        ...     count: int = Field(default=0, description="Count field")
        >>> print(generate_output_structure_section(MyOutput))
        OUTPUT STRUCTURE (REQUIRED):
        You MUST return a Pydantic model that matches this exact structure:
        ...
    """
    structure = _walk_model(output_schema, indent=1)
    
    return f"""OUTPUT STRUCTURE (REQUIRED):
You MUST return a Pydantic model that matches this exact structure:

{{
{structure}
}}

**CRITICAL REQUIREMENTS**:
1. All required fields are REQUIRED - no missing fields, no null values for required fields
2. Optional fields may be omitted or set to their default values
3. Return ONLY valid JSON that matches this Pydantic schema exactly
4. No extra text, no markdown, no code fences, no explanatory text
5. The output will be validated against the Pydantic model - any mismatch will cause an error"""


def generate_output_structure_section_with_custom_requirements(
    output_schema: Type[BaseModel],
    additional_requirements: list[str] = None
) -> str:
    """Generate OUTPUT STRUCTURE section with custom additional requirements.
    
    Args:
        output_schema: The Pydantic BaseModel class that defines the output structure
        additional_requirements: Optional list of additional requirement strings to append
        
    Returns:
        A formatted string containing the OUTPUT STRUCTURE section with custom requirements.
    """
    structure = _walk_model(output_schema, indent=1)
    
    # Build requirements list
    requirements = []
    if additional_requirements:
        requirements.extend(additional_requirements)
    
    # Add standard requirements
    requirements.extend([
        "All required fields are REQUIRED - no missing fields, no null values for required fields",
        "Optional fields may be omitted or set to their default values",
        "Return ONLY valid JSON that matches this Pydantic schema exactly",
        "No extra text, no markdown, no code fences, no explanatory text",
        "The output will be validated against the Pydantic model - any mismatch will cause an error"
    ])
    
    reqs_text = "\n".join(f"{i+1}. {req}" for i, req in enumerate(requirements))
    
    return f"""OUTPUT STRUCTURE (REQUIRED):
You MUST return a Pydantic model that matches this exact structure:

{{
{structure}
}}

**CRITICAL REQUIREMENTS**:
{reqs_text}"""
