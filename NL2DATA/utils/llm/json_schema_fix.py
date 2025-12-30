"""Utility to fix Pydantic JSON schemas for OpenAI response_format compatibility.

OpenAI requires that all object types in JSON schemas have `additionalProperties: false`
explicitly set. Pydantic's default JSON schema generation doesn't always include this.
"""

from typing import Any, Dict
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue, GenerateJsonSchema


class OpenAICompatibleJsonSchema(GenerateJsonSchema):
    """Custom JSON schema generator that adds additionalProperties: false to all objects."""
    
    def generate(self, schema: Any, mode: str = "validation") -> Dict[str, Any]:
        """Generate JSON schema with OpenAI compatibility fixes."""
        # Generate base schema
        base_schema = super().generate(schema, mode)
        # Fix it for OpenAI
        return fix_json_schema_for_openai(base_schema)


def fix_json_schema_for_openai(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively add `additionalProperties: false` to all object types in a JSON schema.
    
    This ensures OpenAI's response_format accepts the schema.
    
    Args:
        schema: JSON schema dictionary (from Pydantic's model_json_schema())
        
    Returns:
        Fixed JSON schema with additionalProperties set for all objects
    """
    if not isinstance(schema, dict):
        return schema
    
    # Create a copy to avoid mutating the original
    fixed_schema = schema.copy()
    
    # If this is an object type, add additionalProperties
    if fixed_schema.get("type") == "object":
        if "additionalProperties" not in fixed_schema:
            fixed_schema["additionalProperties"] = False
        
        # Recursively fix nested properties
        if "properties" in fixed_schema:
            for prop_name, prop_schema in fixed_schema["properties"].items():
                fixed_schema["properties"][prop_name] = fix_json_schema_for_openai(prop_schema)
        
        # Fix patternProperties if present
        if "patternProperties" in fixed_schema:
            for pattern, pattern_schema in fixed_schema["patternProperties"].items():
                fixed_schema["patternProperties"][pattern] = fix_json_schema_for_openai(pattern_schema)
    
    # If this is an array type, fix the items schema
    elif fixed_schema.get("type") == "array":
        if "items" in fixed_schema:
            fixed_schema["items"] = fix_json_schema_for_openai(fixed_schema["items"])
    
    # Handle anyOf, oneOf, allOf
    for key in ["anyOf", "oneOf", "allOf"]:
        if key in fixed_schema:
            fixed_schema[key] = [
                fix_json_schema_for_openai(sub_schema)
                for sub_schema in fixed_schema[key]
            ]
    
    # Handle definitions/$defs (for referenced schemas)
    for defs_key in ["definitions", "$defs"]:
        if defs_key in fixed_schema:
            for ref_name, ref_schema in fixed_schema[defs_key].items():
                fixed_schema[defs_key][ref_name] = fix_json_schema_for_openai(ref_schema)
    
    return fixed_schema


def get_openai_compatible_json_schema(model: type[BaseModel]) -> Dict[str, Any]:
    """
    Get JSON schema from a Pydantic model and fix it for OpenAI compatibility.
    
    Args:
        model: Pydantic model class
        
    Returns:
        OpenAI-compatible JSON schema
    """
    # Use custom schema generator that adds additionalProperties: false
    generator = OpenAICompatibleJsonSchema()
    return generator.generate(model.model_json_schema(), mode="validation")

