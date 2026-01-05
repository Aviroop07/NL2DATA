"""Phase 5, Step 5.2: Independent Attribute Data Type Assignment.

Assigns SQL data types to independent attributes (attributes with no dependencies).
These are processed first in the topological ordering.
"""

from typing import Dict, Any, List, Optional, Tuple
import asyncio
from pydantic import BaseModel, Field, ConfigDict, ValidationError

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.phases.phase5.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.data_types.type_assignment import (
    DataTypeAssignmentOutput,
    AttributeTypeInfo,
    _deterministic_type_assignment,
    _infer_type_from_name_and_hint,
)
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_type_hint,
)

logger = get_logger(__name__)


class AttributeTypeAssignment(BaseModel):
    """Single attribute type assignment."""
    attribute_key: str = Field(description="Attribute key in format 'Entity.attribute'")
    type_info: AttributeTypeInfo = Field(description="Type information for this attribute")

    model_config = ConfigDict(extra="forbid")


def _create_type_assignment(attribute_key: str, type_info_dict: Dict[str, Any]) -> AttributeTypeAssignment:
    """Helper to create AttributeTypeAssignment from dict."""
    return AttributeTypeAssignment(
        attribute_key=attribute_key,
        type_info=AttributeTypeInfo(
            type=type_info_dict.get("type", "VARCHAR"),
            size=type_info_dict.get("size"),
            precision=type_info_dict.get("precision"),
            scale=type_info_dict.get("scale"),
            reasoning=type_info_dict.get("reasoning", "")
        )
    )


class IndependentAttributeDataTypesOutput(BaseModel):
    """Output structure for independent attribute data type assignment."""
    data_types: List[AttributeTypeAssignment] = Field(
        description="List of attribute type assignments"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("5.2", phase=5, tags=["phase_5_step_2"])
async def step_5_2_independent_attribute_data_types(
    entity_name: str,
    attribute_name: str,
    attributes: Dict[str, List[Dict[str, Any]]],  # All entity attributes
    primary_keys: Optional[Dict[str, List[str]]] = None,
    domain: Optional[str] = None,
    entity_descriptions: Optional[Dict[str, str]] = None,
    nl_description: Optional[str] = None,
) -> IndependentAttributeDataTypesOutput:
    """
    Step 5.2 (per-attribute): Assign SQL data type to an independent attribute.
    
    Independent attributes have no dependencies (not FKs, not derived).
    This step uses LLM to infer appropriate SQL types based on attribute name,
    description, type hints, and domain context.
    
    Args:
        entity_name: Name of the entity
        attribute_name: Name of the attribute to assign type to
        attributes: Dictionary mapping entity names to their attribute lists
        primary_keys: Optional dictionary mapping entity names to primary keys
        domain: Optional domain context
        entity_descriptions: Optional dictionary mapping entity names to descriptions
        nl_description: Optional natural language description
        
    Returns:
        dict: Type assignment result with attribute_types dict
    """
    logger.debug(f"Assigning type to independent attribute: {entity_name}.{attribute_name}")
    
    # Get attribute info
    entity_attrs = attributes.get(entity_name, [])
    attr_info = None
    for attr in entity_attrs:
        if extract_attribute_name(attr) == attribute_name:
            attr_info = attr
            break
    
    if not attr_info:
        logger.warning(f"Attribute {entity_name}.{attribute_name} not found in attributes dict")
        # Fallback to deterministic assignment
        pk = primary_keys.get(entity_name, []) if primary_keys else []
        fallback_result = _deterministic_type_assignment(
            entity_name=entity_name,
            attributes=[{"name": attribute_name}],
            primary_key=pk,
        )
        fallback_types = fallback_result.get("attribute_types", {})
        if attribute_name in fallback_types:
            attr_key = f"{entity_name}.{attribute_name}"
            return IndependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, fallback_types[attribute_name])]
            )
        else:
            attr_key = f"{entity_name}.{attribute_name}"
            type_info_dict = {
                "type": "VARCHAR",
                "size": 255,
                "precision": None,
                "scale": None,
                "reasoning": "Deterministic fallback: default VARCHAR(255)"
            }
            return IndependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, type_info_dict)]
            )
    
    attr_name = extract_attribute_name(attr_info)
    attr_desc = attr_info.get("description", "") if isinstance(attr_info, dict) else ""
    type_hint = extract_attribute_type_hint(attr_info)
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_descriptions and entity_name in entity_descriptions:
        context_parts.append(f"Entity description: {entity_descriptions[entity_name]}")
    if primary_keys and entity_name in primary_keys:
        pk = primary_keys[entity_name]
        context_parts.append(f"Primary key: {', '.join(pk)}")
        if attr_name in pk:
            context_parts.append(f"Note: This attribute is part of the primary key")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
        # Escape braces for template formatting
        context_msg = context_msg.replace("{", "{{").replace("}", "}}")
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=DataTypeAssignmentOutput,
        additional_requirements=[
            f"CRITICAL: The \"attribute_types\" dictionary MUST contain exactly ONE entry with the key \"{attribute_name}\" (the attribute name provided)",
            f"The key in attribute_types MUST be \"{attribute_name}\" - use this exact string as the dictionary key",
            "The \"reasoning\" field in each AttributeTypeInfo is REQUIRED and must explain why this type was chosen",
            "For VARCHAR types, provide a reasonable size (e.g., 255 for names, 50 for codes, 500 for descriptions)",
            "For DECIMAL types, provide precision and scale (e.g., DECIMAL(12,2) for money, DECIMAL(6,4) for percentages)",
            "Consider the attribute name, description, and type_hint when choosing the type",
            "Primary key attributes should typically be BIGINT or VARCHAR",
        ]
    )
    
    # System prompt
    system_prompt = f"""You are a database schema design expert. Your task is to assign appropriate SQL data types to attributes.

CRITICAL OUTPUT FORMAT REQUIREMENT:
You will be given a SINGLE attribute name. You MUST return a JSON object with this EXACT structure:
{{
  "attribute_types": {{
    "{attribute_name}": {{
      "type": "VARCHAR",
      "size": 255,
      "precision": null,
      "scale": null,
      "reasoning": "Your explanation here"
    }}
  }}
}}

REQUIREMENTS:
1. The "attribute_types" field MUST be a dictionary (object) - NOT an empty dictionary
2. The dictionary MUST contain exactly ONE entry
3. The key MUST be the exact attribute name: "{attribute_name}"
4. The value MUST be an AttributeTypeInfo object with: type, size, precision, scale, and reasoning
5. DO NOT return an empty "attribute_types" dictionary - this will cause an error
6. DO NOT omit the "attribute_types" field - this will cause an error

Example for attribute "customer_name":
{{
  "attribute_types": {{
    "customer_name": {{
      "type": "VARCHAR",
      "size": 255,
      "precision": null,
      "scale": null,
      "reasoning": "VARCHAR(255) is appropriate for customer names which are variable-length strings"
    }}
  }}
}}

CRITICAL: If you return an empty "attribute_types" dictionary, your response will be rejected. 
You MUST include the attribute "{attribute_name}" as a key in the "attribute_types" dictionary.

SQL DATA TYPES:
- **VARCHAR(n)**: Variable-length strings (names, descriptions, codes). Provide size parameter.
- **TEXT**: Very long strings (unlimited length descriptions, comments)
- **INT**: 32-bit integers (quantities, counts, small IDs)
- **BIGINT**: 64-bit integers (large IDs, timestamps as integers)
- **DECIMAL(p,s)**: Fixed-point numbers (money, percentages, precise measurements). Provide precision (total digits) and scale (decimal places).
- **DOUBLE**: Floating-point numbers (scientific measurements, approximate values)
- **BOOLEAN**: True/false values (flags, enabled/disabled)
- **DATE**: Date only (year-month-day)
- **TIMESTAMP**: Date and time (year-month-day hour:minute:second)
- **JSON**: JSON documents (structured data)

TYPE SELECTION GUIDELINES:
1. **IDs and Keys**: Use BIGINT for numeric IDs, VARCHAR for string IDs
2. **Names**: VARCHAR(255) for person/entity names, VARCHAR(100) for short names
3. **Descriptions**: TEXT for long descriptions, VARCHAR(500) for medium descriptions
4. **Money/Amounts**: DECIMAL(12,2) for currency, DECIMAL(10,2) for smaller amounts
5. **Percentages**: DECIMAL(6,4) or DECIMAL(5,2) depending on precision needed
6. **Quantities/Counts**: INT for small numbers, BIGINT for large numbers
7. **Flags/Booleans**: BOOLEAN for true/false attributes
8. **Dates**: DATE for date-only, TIMESTAMP for date+time
9. **Codes**: VARCHAR(50) or VARCHAR(20) for short codes
10. **Emails/URLs**: VARCHAR(255) for emails, VARCHAR(500) for URLs

IMPORTANT:
- Consider the attribute name patterns (e.g., names ending in _id suggest BIGINT, _flag suggests BOOLEAN)
- Consider the type_hint if provided (e.g., "integer" -> INT or BIGINT, "string" -> VARCHAR)
- Consider the description context (e.g., "email address" -> VARCHAR(255))
- For primary keys, prefer BIGINT or VARCHAR depending on the use case
- Be conservative with VARCHAR sizes - choose reasonable defaults but allow for growth

""" + output_structure_section
    
    # Human prompt
    human_prompt = f"""Entity: {entity_name}
Attribute: {attribute_name}
Description: {attr_desc or 'No description provided'}
Type hint: {type_hint or 'No type hint provided'}
{context_msg}

CRITICAL: You MUST return a dictionary with the attribute name "{attribute_name}" as the key in the "attribute_types" field.

Example output structure:
{{
  "attribute_types": {{
    "{attribute_name}": {{
      "type": "VARCHAR",
      "size": 255,
      "precision": null,
      "scale": null,
      "reasoning": "Explanation of why this type was chosen"
    }}
  }}
}}

Assign an appropriate SQL data type for the attribute "{attribute_name}". 

CRITICAL: The attribute_types dictionary MUST contain exactly one entry with the key "{attribute_name}".
DO NOT return an empty attribute_types dictionary. 
DO NOT omit the attribute_types field.
The attribute_types dictionary MUST have "{attribute_name}" as a key with a valid AttributeTypeInfo object as the value."""
    
    # Get model
    llm = get_model_for_step("5.2")
    
    try:
        config = get_trace_config("5.2", phase=5, tags=["independent_attribute_types"])
        
        # Try LLM assignment
        try:
            result: DataTypeAssignmentOutput = await standardized_llm_call(
                llm=llm,
                output_schema=DataTypeAssignmentOutput,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt,
                input_data={},
                tools=None,
                use_agent_executor=False,
                decouple_tools=False,
                config=config,
            )
        except ValidationError as validation_error:
            # Check if it's an empty dict error (missing attribute_types field)
            error_str = str(validation_error)
            if "attribute_types" in error_str and "Field required" in error_str:
                logger.warning(
                    f"LLM returned empty/invalid response for {entity_name}.{attribute_name} (missing attribute_types). "
                    f"Using deterministic fallback. Error: {error_str[:200]}"
                )
            else:
                logger.warning(
                    f"LLM validation error for {entity_name}.{attribute_name}: {validation_error}. Using deterministic fallback"
                )
            # Fall through to fallback logic
            result = None
        except Exception as llm_error:
            logger.warning(
                f"LLM call failed for {entity_name}.{attribute_name}: {llm_error}. Using deterministic fallback"
            )
            pk = primary_keys.get(entity_name, []) if primary_keys else []
            fallback_result = _deterministic_type_assignment(
                entity_name=entity_name,
                attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                primary_key=pk,
            )
            # Convert fallback result to expected format
            fallback_types = fallback_result.get("attribute_types", {})
            if attribute_name in fallback_types:
                attr_key = f"{entity_name}.{attribute_name}"
                return IndependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, fallback_types[attribute_name])]
                )
            else:
                # If attribute not in fallback, create a basic type
                attr_key = f"{entity_name}.{attribute_name}"
                type_info_dict = {
                    "type": "VARCHAR",
                    "size": 255,
                    "precision": None,
                    "scale": None,
                    "reasoning": "Deterministic fallback: default VARCHAR(255)"
                }
                return IndependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, type_info_dict)]
                )
        
        # Validate that the attribute is in the result
        # If result is None (from exception handling) or empty dict, use fallback
        if result is None or not hasattr(result, 'attribute_types') or not result.attribute_types:
            logger.warning(
                f"LLM returned empty attribute_types dict for {entity_name}.{attribute_name}, using deterministic fallback. "
                f"Result object: {result.model_dump() if hasattr(result, 'model_dump') else result}"
            )
            pk = primary_keys.get(entity_name, []) if primary_keys else []
            fallback_result = _deterministic_type_assignment(
                entity_name=entity_name,
                attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                primary_key=pk,
            )
            # Convert fallback result to expected format
            fallback_types = fallback_result.get("attribute_types", {})
            if attribute_name in fallback_types:
                attr_key = f"{entity_name}.{attribute_name}"
                return IndependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, fallback_types[attribute_name])]
                )
            else:
                # If attribute not in fallback, create a basic type
                attr_key = f"{entity_name}.{attribute_name}"
                type_info_dict = {
                    "type": "VARCHAR",
                    "size": 255,
                    "precision": None,
                    "scale": None,
                    "reasoning": "Deterministic fallback: default VARCHAR(255)"
                }
                return IndependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, type_info_dict)]
                )
        
        # Check if attribute_name is in result, if not, check if there's only one entry (might be wrong key)
        if attribute_name not in result.attribute_types:
            # Try to fix: if there's exactly one entry, use it (LLM might have used wrong key)
            if len(result.attribute_types) == 1:
                wrong_key = list(result.attribute_types.keys())[0]
                logger.warning(
                    f"LLM used wrong key '{wrong_key}' instead of '{attribute_name}' for {entity_name}.{attribute_name}. "
                    f"Fixing by using the returned value."
                )
                # Use the value that was returned (even with wrong key)
                type_info = result.attribute_types[wrong_key]
            else:
                logger.warning(
                    f"LLM did not return type for {entity_name}.{attribute_name} "
                    f"(got keys: {list(result.attribute_types.keys())}), using deterministic fallback"
                )
                # Fallback to deterministic
                pk = primary_keys.get(entity_name, []) if primary_keys else []
                fallback_result = _deterministic_type_assignment(
                    entity_name=entity_name,
                    attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                    primary_key=pk,
                )
                fallback_types = fallback_result.get("attribute_types", {})
                if attribute_name in fallback_types:
                    attr_key = f"{entity_name}.{attribute_name}"
                    return IndependentAttributeDataTypesOutput(
                        data_types=[_create_type_assignment(attr_key, fallback_types[attribute_name])]
                    )
                else:
                    attr_key = f"{entity_name}.{attribute_name}"
                    type_info_dict = {
                        "type": "VARCHAR",
                        "size": 255,
                        "precision": None,
                        "scale": None,
                        "reasoning": "Deterministic fallback: default VARCHAR(255)"
                    }
                    return IndependentAttributeDataTypesOutput(
                        data_types=[_create_type_assignment(attr_key, type_info_dict)]
                    )
        else:
            # Normal case: attribute_name is in result
            type_info = result.attribute_types[attribute_name]
        attr_key = f"{entity_name}.{attribute_name}"
        return IndependentAttributeDataTypesOutput(
            data_types=[AttributeTypeAssignment(
                attribute_key=attr_key,
                type_info=AttributeTypeInfo(
                    type=type_info.type,
                    size=type_info.size,
                    precision=type_info.precision,
                    scale=type_info.scale,
                    reasoning=type_info.reasoning,
                )
            )]
        )
        
    except Exception as e:
        logger.warning(
            f"Error in LLM type assignment for {entity_name}.{attribute_name}: {e}. "
            f"Using deterministic fallback",
            exc_info=True
        )
        # Fallback to deterministic assignment
        pk = primary_keys.get(entity_name, []) if primary_keys else []
        fallback_result = _deterministic_type_assignment(
            entity_name=entity_name,
            attributes=[attr_info] if attr_info else [{"name": attribute_name}],
            primary_key=pk,
        )
        # Convert fallback result to expected format
        fallback_types = fallback_result.get("attribute_types", {})
        if attribute_name in fallback_types:
            attr_key = f"{entity_name}.{attribute_name}"
            type_info_dict = fallback_types[attribute_name]
            return IndependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, type_info_dict)]
            )
        else:
            # If attribute not in fallback, create a basic type
            attr_key = f"{entity_name}.{attribute_name}"
            type_info_dict = {
                "type": "VARCHAR",
                "size": 255,
                "precision": None,
                "scale": None,
                "reasoning": "Deterministic fallback: default VARCHAR(255)"
            }
            return IndependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, type_info_dict)]
            )


class IndependentAttributeDataTypesBatchOutput(BaseModel):
    """Batch output structure for independent attribute data type assignment."""
    data_types: List[AttributeTypeAssignment] = Field(
        description="List of all attribute type assignments"
    )

    model_config = ConfigDict(extra="forbid")


async def step_5_2_independent_attribute_data_types_batch(
    independent_attributes: List[Tuple[str, str]],  # List of (entity_name, attribute_name) tuples
    attributes: Dict[str, List[Dict[str, Any]]],  # All entity attributes
    primary_keys: Optional[Dict[str, List[str]]] = None,
    domain: Optional[str] = None,
    entity_descriptions: Optional[Dict[str, str]] = None,
    nl_description: Optional[str] = None,
) -> IndependentAttributeDataTypesBatchOutput:
    """
    Step 5.2 (batch): Assign SQL data types to all independent attributes (parallel execution).
    
    Args:
        independent_attributes: List of (entity_name, attribute_name) tuples
        attributes: Dictionary mapping entity names to their attribute lists
        primary_keys: Optional dictionary mapping entity names to primary keys
        domain: Optional domain context
        entity_descriptions: Optional dictionary mapping entity names to descriptions
        nl_description: Optional natural language description
        
    Returns:
        dict: Combined data_types dict mapping "entity.attribute" -> type_info
    """
    logger.info(f"Starting Step 5.2: Independent Attribute Data Types for {len(independent_attributes)} attributes")
    
    if not independent_attributes:
        logger.warning("No independent attributes provided for type assignment")
        return IndependentAttributeDataTypesBatchOutput(data_types=[])
    
    # Execute in parallel for all independent attributes
    tasks = []
    for entity_name, attribute_name in independent_attributes:
        task = step_5_2_independent_attribute_data_types(
            entity_name=entity_name,
            attribute_name=attribute_name,
            attributes=attributes,
            primary_keys=primary_keys,
            domain=domain,
            entity_descriptions=entity_descriptions,
            nl_description=nl_description,
        )
        tasks.append((entity_name, attribute_name, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, _, task in tasks],
        return_exceptions=True
    )
    
    # Combine results
    all_data_types_list = []
    for i, ((entity_name, attribute_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(
                f"Error assigning type to {entity_name}.{attribute_name}: {result}",
                exc_info=True
            )
            # Use deterministic fallback
            entity_attrs = attributes.get(entity_name, [])
            attr_info = None
            for attr in entity_attrs:
                if extract_attribute_name(attr) == attribute_name:
                    attr_info = attr
                    break
            
            pk = primary_keys.get(entity_name, []) if primary_keys else []
            fallback_result = _deterministic_type_assignment(
                entity_name=entity_name,
                attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                primary_key=pk,
            )
            fallback_types = fallback_result.get("attribute_types", {})
            if attribute_name in fallback_types:
                attr_key = f"{entity_name}.{attribute_name}"
                all_data_types_list.append(_create_type_assignment(attr_key, fallback_types[attribute_name]))
            continue
        
        # Extract data_types from result (now a list)
        if hasattr(result, 'data_types'):
            all_data_types_list.extend(result.data_types)
        elif isinstance(result, dict):
            # Handle old dict format for backward compatibility
            result_data_types = result.get("data_types", {})
            for attr_key, type_info_dict in result_data_types.items():
                all_data_types_list.append(_create_type_assignment(attr_key, type_info_dict))
    
    logger.info(f"Assigned types to {len(all_data_types_list)} independent attributes")
    
    return IndependentAttributeDataTypesBatchOutput(data_types=all_data_types_list)
