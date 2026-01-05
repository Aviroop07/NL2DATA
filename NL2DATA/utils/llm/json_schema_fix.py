"""Utility to fix Pydantic JSON schemas for OpenAI response_format compatibility.

OpenAI requires that all object types in JSON schemas have `additionalProperties: false`
explicitly set. Pydantic's default JSON schema generation doesn't always include this.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue, GenerateJsonSchema


class OpenAICompatibleJsonSchema(GenerateJsonSchema):
    """Custom JSON schema generator that adds additionalProperties: false to all objects."""
    
    def generate(self, schema: Any, mode: str = "validation") -> Dict[str, Any]:
        """Generate JSON schema with OpenAI compatibility fixes."""
        # Generate base schema using parent class
        base_schema = super().generate(schema, mode)
        # Sanitize base schema immediately to remove any type objects
        base_schema = _sanitize_for_json(base_schema) if isinstance(base_schema, dict) else base_schema
        # Fix it for OpenAI (adds additionalProperties: false)
        fixed_schema = fix_json_schema_for_openai(base_schema)
        # Final sanitization to ensure no type objects remain (after all processing)
        return _sanitize_for_json(fixed_schema) if isinstance(fixed_schema, dict) else fixed_schema


def _sanitize_for_json(obj: Any, _visited: Optional[set] = None) -> Any:
    """
    Recursively sanitize objects to ensure they're JSON serializable.
    Converts type objects to strings.
    Uses _visited set to prevent infinite recursion on circular references.
    """
    if _visited is None:
        _visited = set()
    
    # Handle circular references
    obj_id = id(obj)
    if obj_id in _visited:
        return "<circular_reference>"
    _visited.add(obj_id)
    
    try:
        if isinstance(obj, type):
            # Convert Python type objects to their name
            return obj.__name__
        elif isinstance(obj, dict):
            return {k: _sanitize_for_json(v, _visited) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [_sanitize_for_json(item, _visited) for item in obj]
        elif isinstance(obj, set):
            return [_sanitize_for_json(item, _visited) for item in obj]
        elif hasattr(obj, 'model_dump'):
            # Handle Pydantic BaseModel instances
            try:
                return _sanitize_for_json(obj.model_dump(), _visited)
            except:
                return str(obj)
        elif hasattr(obj, '__dict__'):
            # Handle objects with __dict__
            try:
                return _sanitize_for_json(obj.__dict__, _visited)
            except:
                return str(obj)
        elif obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        else:
            # For any other type, check if it's type-like
            if hasattr(obj, '__class__') and obj.__class__ == type:
                return obj.__name__
            return str(obj)
    finally:
        _visited.discard(obj_id)


def fix_json_schema_for_openai(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively add `additionalProperties: false` to all object types in a JSON schema.
    Also fixes the `required` array to ensure it only contains fields that are actually required.
    
    This ensures OpenAI's response_format accepts the schema.
    
    Args:
        schema: JSON schema dictionary (from Pydantic's model_json_schema())
        
    Returns:
        Fixed JSON schema with additionalProperties set for all objects
    """
    if not isinstance(schema, dict):
        return _sanitize_for_json(schema) if schema is not None else schema
    
    # Sanitize schema first to remove any type objects
    schema = _sanitize_for_json(schema) if isinstance(schema, dict) else schema
    if not isinstance(schema, dict):
        return schema
    
    # Create a copy to avoid mutating the original
    fixed_schema = schema.copy()
    
    # If this is an object type, add additionalProperties and fix required array
    if fixed_schema.get("type") == "object":
        if "additionalProperties" not in fixed_schema:
            fixed_schema["additionalProperties"] = False
        
        # Fix required array: ensure it only contains fields that are actually in properties
        # OpenAI requires that the required array only contains fields that exist in properties
        # and don't have defaults (fields with default_factory should not be in required)
        if "required" in fixed_schema and "properties" in fixed_schema:
            properties = fixed_schema["properties"]
            required = fixed_schema.get("required", [])
            
            # Filter required to only include fields that:
            # 1. Actually exist in properties
            # 2. Don't have a default value in their property schema
            valid_required = []
            for field_name in required:
                if field_name in properties:
                    prop_schema = properties[field_name]
                    # Check if property has a default - if it does, it shouldn't be in required
                    # Properties with default_factory will not have "default" in the JSON schema,
                    # but Pydantic should already handle this correctly
                    # However, OpenAI is strict: required array must only contain fields in properties
                    # and must match exactly what's in properties
                    has_default = prop_schema.get("default") is not None
                    if not has_default:
                        valid_required.append(field_name)
                # If field not in properties, don't include it in required
            
            # Ensure required array only contains fields that exist in properties
            fixed_schema["required"] = [r for r in valid_required if r in properties]
        
        # Recursively fix nested properties
        if "properties" in fixed_schema:
            for prop_name, prop_schema in fixed_schema["properties"].items():
                # Sanitize prop_schema first to remove type objects
                prop_schema = _sanitize_for_json(prop_schema) if prop_schema is not None else prop_schema
                fixed_prop = fix_json_schema_for_openai(prop_schema) if isinstance(prop_schema, dict) else prop_schema
                # Ensure all object types have additionalProperties: false
                if isinstance(fixed_prop, dict):
                    if fixed_prop.get("type") == "object":
                        # Always set additionalProperties to false for OpenAI compatibility
                        if "additionalProperties" not in fixed_prop:
                            fixed_prop["additionalProperties"] = False
                    # Also check if the property schema itself needs additionalProperties (for Dict fields)
                    # If it's an object type without explicit type key, it might still need additionalProperties
                    if "type" not in fixed_prop and "properties" in fixed_prop:
                        # This is an object type (has properties), ensure additionalProperties: false
                        fixed_prop["additionalProperties"] = False
                # Handle anyOf/oneOf/allOf at property level (e.g., Optional[List[Dict]] creates anyOf with array option)
                if isinstance(fixed_prop, dict):
                    for key in ["anyOf", "oneOf", "allOf"]:
                        if key in fixed_prop:
                            for i, sub_schema in enumerate(fixed_prop[key]):
                                if isinstance(sub_schema, dict):
                                    # If it's an array type, ensure items have proper additionalProperties
                                    if sub_schema.get("type") == "array" and "items" in sub_schema:
                                        items_schema = sub_schema["items"]
                                        if isinstance(items_schema, dict):
                                            # If items is an object, ensure additionalProperties: false
                                            if items_schema.get("type") == "object":
                                                if "additionalProperties" not in items_schema:
                                                    items_schema["additionalProperties"] = False
                                            # Recursively fix items schema (handles nested anyOf in items)
                                            sub_schema["items"] = fix_json_schema_for_openai(items_schema)
                                    # Recursively fix the sub_schema itself
                                    fixed_prop[key][i] = fix_json_schema_for_openai(sub_schema)
                # Sanitize again after processing to ensure no type objects
                fixed_schema["properties"][prop_name] = _sanitize_for_json(fixed_prop) if fixed_prop is not None else fixed_prop
        
        # Fix patternProperties if present
        if "patternProperties" in fixed_schema:
            for pattern, pattern_schema in fixed_schema["patternProperties"].items():
                pattern_schema_sanitized = _sanitize_for_json(pattern_schema) if pattern_schema is not None else pattern_schema
                fixed_schema["patternProperties"][pattern] = fix_json_schema_for_openai(pattern_schema_sanitized) if isinstance(pattern_schema_sanitized, dict) else pattern_schema_sanitized
                # Sanitize again after processing
                fixed_schema["patternProperties"][pattern] = _sanitize_for_json(fixed_schema["patternProperties"][pattern]) if fixed_schema["patternProperties"][pattern] is not None else fixed_schema["patternProperties"][pattern]
    
    # If this is an array type, fix the items schema
    elif fixed_schema.get("type") == "array":
        if "items" in fixed_schema:
            items_schema = _sanitize_for_json(fixed_schema["items"]) if fixed_schema["items"] is not None else fixed_schema["items"]
            # Recursively fix items schema (this will handle anyOf, oneOf, allOf within items)
            fixed_schema["items"] = fix_json_schema_for_openai(items_schema) if isinstance(items_schema, dict) else items_schema
            # If items has anyOf/oneOf/allOf, ensure all object types in those have additionalProperties: false
            if isinstance(fixed_schema["items"], dict):
                for key in ["anyOf", "oneOf", "allOf"]:
                    if key in fixed_schema["items"]:
                        for i, sub_schema in enumerate(fixed_schema["items"][key]):
                            if isinstance(sub_schema, dict):
                                # If it's an object type, ensure additionalProperties: false
                                if sub_schema.get("type") == "object":
                                    if "additionalProperties" not in sub_schema:
                                        sub_schema["additionalProperties"] = False
                                # Recursively fix nested schemas
                                fixed_schema["items"][key][i] = fix_json_schema_for_openai(sub_schema)
            # Sanitize again after processing
            fixed_schema["items"] = _sanitize_for_json(fixed_schema["items"]) if fixed_schema["items"] is not None else fixed_schema["items"]
    
    # Handle anyOf, oneOf, allOf
    for key in ["anyOf", "oneOf", "allOf"]:
        if key in fixed_schema:
            fixed_schema[key] = [
                fix_json_schema_for_openai(_sanitize_for_json(sub_schema) if isinstance(sub_schema, dict) else sub_schema)
                if isinstance(sub_schema, dict) else _sanitize_for_json(sub_schema)
                for sub_schema in fixed_schema[key]
            ]
            # Sanitize again after processing
            fixed_schema[key] = [_sanitize_for_json(sub_schema) if isinstance(sub_schema, dict) else sub_schema for sub_schema in fixed_schema[key]]
    
    # Handle definitions/$defs (for referenced schemas)
    for defs_key in ["definitions", "$defs"]:
        if defs_key in fixed_schema:
            for ref_name, ref_schema in fixed_schema[defs_key].items():
                ref_schema_sanitized = _sanitize_for_json(ref_schema) if ref_schema is not None else ref_schema
                fixed_schema[defs_key][ref_name] = fix_json_schema_for_openai(ref_schema_sanitized) if isinstance(ref_schema_sanitized, dict) else ref_schema_sanitized
                # Sanitize again after processing
                fixed_schema[defs_key][ref_name] = _sanitize_for_json(fixed_schema[defs_key][ref_name]) if fixed_schema[defs_key][ref_name] is not None else fixed_schema[defs_key][ref_name]
    
    # Final sanitization pass before returning
    return _sanitize_for_json(fixed_schema) if isinstance(fixed_schema, dict) else fixed_schema


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
    # Generate schema using the model's core schema
    core_schema = model.__pydantic_core_schema__
    schema = generator.generate(core_schema, mode="validation")
    # Post-process to fix required array issues based on actual model fields
    fixed_schema = fix_json_schema_for_openai(schema)
    
    # Additional fix: Use model fields to determine which fields should be in required
    # This ensures fields with default_factory are not in required
    if isinstance(fixed_schema, dict) and "properties" in fixed_schema:
        model_fields = model.model_fields
        properties = fixed_schema["properties"]
        
        # Build correct required array from model fields
        correct_required = []
        for field_name, field_info in model_fields.items():
            if field_name in properties:
                # Field is required if it doesn't have a default and is not Optional
                if field_info.is_required():
                    correct_required.append(field_name)
        
        # Update required array
        if "required" in fixed_schema:
            fixed_schema["required"] = correct_required
        elif correct_required:
            fixed_schema["required"] = correct_required
    
    # Also fix $defs/definitions recursively - need to find the actual model class
    # and use its fields to determine required array
    for defs_key in ["definitions", "$defs"]:
        if defs_key in fixed_schema:
            for ref_name, ref_schema in fixed_schema[defs_key].items():
                # Try to find the referenced model class
                ref_model = None
                try:
                    # Try to import and get the model class
                    if ref_name == "AttributeTypeInfo":
                        from NL2DATA.utils.data_types.type_assignment import AttributeTypeInfo
                        ref_model = AttributeTypeInfo
                    elif ref_name == "DataTypeAssignmentOutput":
                        from NL2DATA.utils.data_types.type_assignment import DataTypeAssignmentOutput
                        ref_model = DataTypeAssignmentOutput
                    # Add more model lookups as needed
                except ImportError:
                    pass
                
                if ref_model and isinstance(ref_schema, dict) and "properties" in ref_schema:
                    # Use model fields to determine correct required array
                    ref_model_fields = ref_model.model_fields
                    props = ref_schema["properties"]
                    
                    # Build correct required array from model fields
                    correct_ref_required = []
                    for field_name, field_info in ref_model_fields.items():
                        if field_name in props and field_info.is_required():
                            correct_ref_required.append(field_name)
                    
                    ref_schema["required"] = correct_ref_required
                elif isinstance(ref_schema, dict) and "properties" in ref_schema:
                    # Fallback: just ensure required array only contains fields in properties
                    props = ref_schema["properties"]
                    if "required" in ref_schema:
                        ref_required = ref_schema["required"]
                        # Filter to only include fields that exist in properties
                        ref_schema["required"] = [r for r in ref_required if r in props]
    
    # Final sanitization pass to ensure no type objects remain (after all processing)
    # This ensures any type objects that might have been introduced during processing are removed
    fixed_schema = _sanitize_for_json(fixed_schema)
    
    return fixed_schema

