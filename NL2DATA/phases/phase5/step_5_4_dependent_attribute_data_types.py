"""Phase 5, Step 5.4: Dependent Attribute Data Type Assignment.

Assigns SQL data types to dependent attributes (attributes that depend on other attributes).
These are processed after independent attributes and FKs.
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
)
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_type_hint,
)
# Import AttributeTypeAssignment from step_5_2
from NL2DATA.phases.phase5.step_5_2_independent_attribute_data_types import (
    AttributeTypeAssignment,
    _create_type_assignment
)

logger = get_logger(__name__)


class DependentAttributeDataTypesOutput(BaseModel):
    """Output structure for dependent attribute data type assignment."""
    data_types: List[AttributeTypeAssignment] = Field(
        description="List of attribute type assignments"
    )

    model_config = ConfigDict(extra="forbid")


class DependentAttributeDataTypesBatchOutput(BaseModel):
    """Batch output structure for dependent attribute data type assignment."""
    data_types: List[AttributeTypeAssignment] = Field(
        description="List of all attribute type assignments"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("5.4", phase=5, tags=["phase_5_step_4"])
async def step_5_4_dependent_attribute_data_types(
    entity_name: str,
    attribute_name: str,
    attributes: Dict[str, List[Dict[str, Any]]],  # All entity attributes
    dependency_graph: Dict[str, List[str]],  # "entity.attribute" -> list of dependency keys
    fk_dependencies,  # Can be dict or list of FkDependencyInfo
    derived_dependencies,  # Can be dict or list of DerivedDependencyInfo
    independent_types,  # Can be dict or IndependentAttributeDataTypesBatchOutput
    fk_types,  # Can be dict or FkDataTypesOutput
    primary_keys: Optional[Dict[str, List[str]]] = None,
    derived_formulas: Optional[Dict[str, Dict[str, Any]]] = None,  # "entity.attribute" -> formula info
    domain: Optional[str] = None,
    nl_description: Optional[str] = None,
) -> DependentAttributeDataTypesOutput:
    """
    Step 5.4 (per-attribute): Assign SQL data type to a dependent attribute.
    
    Dependent attributes depend on other attributes (FKs or derived from formulas).
    This step uses LLM to infer appropriate SQL types, considering:
    - The attribute's dependencies
    - Derived formulas (if applicable)
    - Context from independent and FK types
    
    Args:
        entity_name: Name of the entity
        attribute_name: Name of the attribute to assign type to
        attributes: Dictionary mapping entity names to their attribute lists
        dependency_graph: Dependency graph from Step 5.1
        fk_dependencies: FK dependency mapping
        derived_dependencies: Derived attribute dependency mapping
        independent_types: Type assignments from Step 5.2
        fk_types: Type assignments from Step 5.3
        primary_keys: Optional dictionary mapping entity names to primary keys
        derived_formulas: Optional dictionary mapping "entity.attribute" -> formula info
        domain: Optional domain context
        nl_description: Optional natural language description
        
    Returns:
        dict: Type assignment result with attribute_types dict
    """
    logger.debug(f"Assigning type to dependent attribute: {entity_name}.{attribute_name}")
    
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
            return DependentAttributeDataTypesOutput(
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
            return DependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, type_info_dict)]
            )
    
    attr_name = extract_attribute_name(attr_info)
    attr_desc = attr_info.get("description", "") if isinstance(attr_info, dict) else ""
    type_hint = extract_attribute_type_hint(attr_info)
    
    # Convert input parameters to dict format for easier handling
    # Convert fk_dependencies
    if hasattr(fk_dependencies, '__iter__') and not isinstance(fk_dependencies, dict):
        # It's a list of FkDependencyInfo
        fk_dependencies_dict = {
            dep.attribute_key: {"entity": dep.referenced_entity, "attribute": dep.referenced_attribute}
            for dep in fk_dependencies
        }
    else:
        fk_dependencies_dict = fk_dependencies
    
    # Convert independent_types
    if hasattr(independent_types, 'data_types'):
        # It's IndependentAttributeDataTypesBatchOutput
        independent_types_dict = {
            assignment.attribute_key: assignment.type_info.model_dump()
            for assignment in independent_types.data_types
        }
    elif hasattr(independent_types, 'model_dump'):
        independent_types_dict = independent_types.model_dump().get("data_types", {})
        if isinstance(independent_types_dict, list):
            independent_types_dict = {
                assignment.get("attribute_key"): assignment.get("type_info", {})
                if isinstance(assignment, dict) else assignment.type_info.model_dump()
                for assignment in independent_types_dict
            }
    else:
        independent_types_dict = independent_types
    
    # Convert fk_types
    if hasattr(fk_types, 'fk_data_types'):
        # It's FkDataTypesOutput
        fk_types_dict = {
            assignment.attribute_key: assignment.type_info.model_dump()
            for assignment in fk_types.fk_data_types
        }
    elif hasattr(fk_types, 'model_dump'):
        fk_types_dict = fk_types.model_dump().get("fk_data_types", {})
        if isinstance(fk_types_dict, list):
            fk_types_dict = {
                assignment.get("attribute_key"): assignment.get("type_info", {})
                if isinstance(assignment, dict) else assignment.type_info.model_dump()
                for assignment in fk_types_dict
            }
    else:
        fk_types_dict = fk_types
    
    # Get dependencies
    attr_key = f"{entity_name}.{attribute_name}"
    dependencies = dependency_graph.get(attr_key, [])
    
    # Build dependency context
    dependency_context = []
    
    # Check if it's an FK (FKs should already be handled in step 5.3, skip LLM call)
    is_fk = attr_key in fk_dependencies_dict
    if is_fk:
        # FK types are already assigned in step 5.3, skip LLM call
        logger.debug(f"Skipping {attr_key} - already handled as FK in step 5.3")
        fk_dep = fk_dependencies_dict[attr_key]
        ref_entity = fk_dep.get("entity", "")
        ref_attr = fk_dep.get("attribute", "")
        ref_key = f"{ref_entity}.{ref_attr}"
        ref_type = fk_types_dict.get(ref_key, {})
        if ref_type:
            # Return FK type directly (should already be in fk_types)
            return DependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, ref_type)]
            )
        else:
            # Fallback: use independent_types if available
            ref_type = independent_types_dict.get(ref_key, {})
            if ref_type:
                return DependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, ref_type)]
                )
            else:
                logger.warning(f"FK {attr_key} references {ref_key} but type not found, using fallback")
                pk = primary_keys.get(entity_name, []) if primary_keys else []
                fallback_result = _deterministic_type_assignment(
                    entity_name=entity_name,
                    attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                    primary_key=pk,
                )
                fallback_types = fallback_result.get("attribute_types", {})
                if attribute_name in fallback_types:
                    return DependentAttributeDataTypesOutput(
                        data_types=[_create_type_assignment(attr_key, fallback_types[attribute_name])]
                    )
                else:
                    type_info_dict = {
                        "type": "VARCHAR",
                        "size": 255,
                        "precision": None,
                        "scale": None,
                        "reasoning": "Deterministic fallback: default VARCHAR(255)"
                    }
                    return DependentAttributeDataTypesOutput(
                        data_types=[_create_type_assignment(attr_key, type_info_dict)]
                    )
    
    # Check if it's a derived attribute - handle deterministically
    is_derived = False
    derived_formula = None
    derived_deps = []
    if derived_formulas and attr_key in derived_formulas:
        is_derived = True
        formula_info = derived_formulas[attr_key]
        derived_formula = formula_info.get("formula", "")
        derived_deps = formula_info.get("dependencies", [])
        
        # For derived attributes, determine type deterministically from formula
        logger.debug(f"Deterministically inferring type for derived attribute {attr_key} from formula: {derived_formula}")
        from NL2DATA.utils.data_types.derived_type_inference import infer_derived_attribute_type
        
        # Get dependency types
        all_types = {**independent_types_dict, **fk_types_dict}
        inferred_type = infer_derived_attribute_type(
            entity_name=entity_name,
            attribute_name=attribute_name,
            formula=derived_formula,
            dependencies=derived_deps,
            all_data_types=all_types,
        )
        
        return DependentAttributeDataTypesOutput(
            data_types=[_create_type_assignment(attr_key, inferred_type)]
        )
    
    # Get types of dependencies (for non-FK, non-derived dependent attributes that need LLM)
    if dependencies:
        dep_types = []
        for dep_key in dependencies:
            # Check independent types first
            dep_type = independent_types_dict.get(dep_key)
            if not dep_type:
                # Check FK types
                dep_type = fk_types_dict.get(dep_key)
            
            if dep_type:
                dep_type_str = dep_type.get("type", "UNKNOWN")
                dep_entity_attr = dep_key.split(".", 1) if "." in dep_key else ("", dep_key)
                dep_types.append(f"{dep_key}: {dep_type_str}")
        
        if dep_types:
            dependency_context.append(f"Dependencies: {', '.join(dep_types)}")
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if primary_keys and entity_name in primary_keys:
        pk = primary_keys[entity_name]
        context_parts.append(f"Primary key: {', '.join(pk)}")
        if attr_name in pk:
            context_parts.append(f"Note: This attribute is part of the primary key")
    
    if dependency_context:
        context_parts.extend(dependency_context)
    
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
            "For derived attributes, consider the formula and dependency types when choosing the result type",
            "For VARCHAR types, provide a reasonable size (e.g., 255 for names, 50 for codes, 500 for descriptions)",
            "For DECIMAL types, provide precision and scale (e.g., DECIMAL(12,2) for money, DECIMAL(6,4) for percentages)",
            "Consider the attribute name, description, type_hint, and dependencies when choosing the type",
        ]
    )
    
    # System prompt
    system_prompt = f"""You are a database schema design expert. Your task is to assign appropriate SQL data types to dependent attributes.

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

Example for attribute "total_amount":
{{
  "attribute_types": {{
    "total_amount": {{
      "type": "DECIMAL",
      "size": null,
      "precision": 12,
      "scale": 2,
      "reasoning": "DECIMAL(12,2) is appropriate for monetary amounts derived from price * quantity"
    }}
  }}
}}

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

DEPENDENT ATTRIBUTES:
Dependent attributes may be:
1. **Derived attributes**: Computed from other attributes using formulas (e.g., total = price * quantity)
2. **Foreign keys**: References to other entities (already handled in Step 5.3, but may appear here)
3. **Attributes with dependencies**: Attributes that logically depend on other attributes

TYPE SELECTION GUIDELINES:
1. **Derived attributes**: The result type should match the formula's output type
   - Sum/product of numbers -> DECIMAL or DOUBLE
   - Concatenation of strings -> VARCHAR or TEXT
   - Date arithmetic -> DATE or TIMESTAMP
   - Boolean operations -> BOOLEAN
2. **Foreign keys**: Should match the referenced PK type (usually already handled)
3. **Regular dependent attributes**: Follow same guidelines as independent attributes
4. Consider dependency types when choosing the result type
5. For derived numeric calculations, prefer DECIMAL for precision (money, percentages) or DOUBLE for scientific values

IMPORTANT:
- Consider the attribute name patterns (e.g., names ending in _id suggest BIGINT, _flag suggests BOOLEAN)
- Consider the type_hint if provided
- Consider the description context
- For derived attributes, analyze the formula to determine the appropriate result type
- Consider the types of dependencies when choosing the result type

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

Assign an appropriate SQL data type for the dependent attribute "{attribute_name}". The attribute_types dictionary MUST contain exactly one entry with the key "{attribute_name}". Consider the attribute name, description, type hint, dependencies, and any derived formulas."""
    
    # Get model
    llm = get_model_for_step("5.4")
    
    try:
        config = get_trace_config("5.4", phase=5, tags=["dependent_attribute_types"])
        
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
            fallback_types = fallback_result.get("attribute_types", {})
            if attribute_name in fallback_types:
                attr_key = f"{entity_name}.{attribute_name}"
                return DependentAttributeDataTypesOutput(
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
                return DependentAttributeDataTypesOutput(
                    data_types=[_create_type_assignment(attr_key, type_info_dict)]
                )
        
        # Validate that the attribute is in the result
        # If result is None (from exception handling) or empty dict, use fallback
        if result is None or not hasattr(result, 'attribute_types') or not result.attribute_types:
            logger.warning(
                f"LLM returned empty attribute_types dict for {entity_name}.{attribute_name}, using deterministic fallback"
            )
            pk = primary_keys.get(entity_name, []) if primary_keys else []
            fallback_result = _deterministic_type_assignment(
                entity_name=entity_name,
                attributes=[attr_info] if attr_info else [{"name": attribute_name}],
                primary_key=pk,
            )
            fallback_types = fallback_result.get("attribute_types", {})
            if attribute_name in fallback_types:
                attr_key = f"{entity_name}.{attribute_name}"
                return DependentAttributeDataTypesOutput(
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
                return DependentAttributeDataTypesOutput(
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
                    return DependentAttributeDataTypesOutput(
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
                    return DependentAttributeDataTypesOutput(
                        data_types=[_create_type_assignment(attr_key, type_info_dict)]
                    )
        else:
            # Normal case: attribute_name is in result
            type_info = result.attribute_types[attribute_name]
        attr_key = f"{entity_name}.{attribute_name}"
        return DependentAttributeDataTypesOutput(
            data_types=[AttributeTypeAssignment(
                attribute_key=attr_key,
                type_info=AttributeTypeInfo(
                    type=type_info.type,
                    size=type_info.size,
                    precision=type_info.precision,
                    scale=type_info.scale,
                    # Constraints are handled in Phase 8, not here
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
        fallback_types = fallback_result.get("attribute_types", {})
        if attribute_name in fallback_types:
            attr_key = f"{entity_name}.{attribute_name}"
            return DependentAttributeDataTypesOutput(
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
            return DependentAttributeDataTypesOutput(
                data_types=[_create_type_assignment(attr_key, type_info_dict)]
            )


async def step_5_4_dependent_attribute_data_types_batch(
    dependent_attributes: List[Tuple[str, str]],  # List of (entity_name, attribute_name) tuples
    attributes: Dict[str, List[Dict[str, Any]]],  # All entity attributes
        dependency_graph: Dict[str, List[str]],  # Dependency graph from Step 5.1
        fk_dependencies: Dict[str, Dict[str, str]],  # FK dependency mapping
        derived_dependencies: Dict[str, List[str]],  # Derived attribute dependency mapping
        independent_types: Dict[str, Dict[str, Any]],  # Type assignments from Step 5.2
        fk_types: Dict[str, Dict[str, Any]],  # Type assignments from Step 5.3
    primary_keys: Optional[Dict[str, List[str]]] = None,
    derived_formulas: Optional[Dict[str, Dict[str, Any]]] = None,
    domain: Optional[str] = None,
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 5.4 (batch): Assign SQL data types to all dependent attributes (parallel execution).
    
    Args:
        dependent_attributes: List of (entity_name, attribute_name) tuples
        attributes: Dictionary mapping entity names to their attribute lists
        dependency_graph: Dependency graph from Step 5.1
        fk_dependencies: FK dependency mapping
        derived_dependencies: Derived attribute dependency mapping
        independent_types: Type assignments from Step 5.2
        fk_types: Type assignments from Step 5.3
        primary_keys: Optional dictionary mapping entity names to primary keys
        derived_formulas: Optional dictionary mapping "entity.attribute" -> formula info
        domain: Optional domain context
        nl_description: Optional natural language description
        
    Returns:
        dict: Combined data_types dict mapping "entity.attribute" -> type_info
    """
    logger.info(f"Starting Step 5.4: Dependent Attribute Data Types for {len(dependent_attributes)} attributes")
    
    if not dependent_attributes:
        logger.warning("No dependent attributes provided for type assignment")
        return DependentAttributeDataTypesBatchOutput(data_types=[])
    
    # Execute in parallel for all dependent attributes
    tasks = []
    for entity_name, attribute_name in dependent_attributes:
        task = step_5_4_dependent_attribute_data_types(
            entity_name=entity_name,
            attribute_name=attribute_name,
            attributes=attributes,
            dependency_graph=dependency_graph,
            fk_dependencies=fk_dependencies,
            derived_dependencies=derived_dependencies,
            independent_types=independent_types,
            fk_types=fk_types,
            primary_keys=primary_keys,
            derived_formulas=derived_formulas,
            domain=domain,
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
    
    logger.info(f"Assigned types to {len(all_data_types_list)} dependent attributes")
    
    return DependentAttributeDataTypesBatchOutput(data_types=all_data_types_list)
