"""Phase 4, Step 4.5: Check Constraint Detection.

For categorical attributes, determine if they should be enforced as CHECK constraints in SQL.
Uses CHECK only (not ENUM) for database portability across all relational database engines.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase4.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
)

logger = get_logger(__name__)


class CheckConstraintDetectionOutput(BaseModel):
    """Output structure for check constraint detection."""
    check_constraint_attributes: List[str] = Field(
        default_factory=list,
        description="List of categorical attribute names that should have CHECK constraints"
    )
    check_constraint_definitions: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to their allowed values (for CHECK constraint)"
    )
    reasoning: Dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary mapping each attribute name to explanation of why CHECK constraint is needed (every attribute in check_constraint_attributes should have an entry)"
    )


@traceable_step("4.5", phase=4, tags=['phase_4_step_5'])
async def step_4_5_check_constraint_detection(
    entity_name: str,
    categorical_attribute: str,  # Name of the categorical attribute
    attribute_description: Optional[str] = None,  # Description of the attribute
    attribute_type: Optional[str] = None,  # SQL type from Step 4.3
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.5 (per-categorical-attribute): Determine if a categorical attribute should have a CHECK constraint.
    
    This is designed to be called in parallel for multiple categorical attributes.
    
    Args:
        entity_name: Name of the entity
        categorical_attribute: Name of the categorical attribute
        attribute_description: Optional description of the attribute
        attribute_type: Optional SQL type from Step 4.3
        nl_description: Optional original NL description
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Check constraint detection result with check_constraint_attributes, definitions, and reasoning
        
    Example:
        >>> result = await step_4_5_check_constraint_detection(
        ...     entity_name="Order",
        ...     categorical_attribute="status"
        ... )
        >>> "status" in result["check_constraint_attributes"]
        True
    """
    logger.debug(f"Detecting CHECK constraint for {entity_name}.{categorical_attribute}")
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"Attribute: {categorical_attribute}")
    if attribute_description:
        context_parts.append(f"Attribute description: {attribute_description}")
    if attribute_type:
        context_parts.append(f"SQL type: {attribute_type}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to determine if a categorical attribute should be enforced with a CHECK constraint in SQL.

CHECK CONSTRAINTS:
- CHECK constraints enforce that a column value must be one of a specific set of allowed values
- Syntax: CHECK (column_name IN ('value1', 'value2', 'value3'))
- Example: CHECK (status IN ('active', 'inactive', 'pending'))
- Example: CHECK (category IN ('electronics', 'clothing', 'food'))

WHEN TO USE CHECK CONSTRAINTS:
- **Categorical attributes** with a fixed set of values should use CHECK constraints
- This ensures data integrity at the database level
- Prevents invalid values from being inserted
- More portable than ENUM (works across PostgreSQL, MySQL, SQLite, SQL Server)

WHEN NOT TO USE CHECK CONSTRAINTS:
- **Boolean columns (BOOLEAN type)**: Boolean columns already enforce only TRUE and FALSE values by default. No CHECK constraint is needed to restrict them to true/false. Only use CHECK constraints for boolean columns if you need to enforce additional business rules (e.g., CHECK (is_active = true OR deleted_at IS NOT NULL)).
- Attributes with too many possible values (e.g., >20 values) - consider application-level validation instead
- Attributes where values may change frequently - CHECK constraints require ALTER TABLE to modify
- Free-text attributes - these don't have fixed value sets

IMPORTANT:
- Use CHECK constraints (not ENUM) for maximum database portability
- CHECK constraints work across all major SQL databases
- ENUM is database-specific and less portable

Return a JSON object with:
- check_constraint_attributes: List containing the attribute name if it should have a CHECK constraint, empty list otherwise
- check_constraint_definitions: Dictionary mapping attribute name to list of allowed values (e.g., {{"status": ["active", "inactive", "pending"]}})
- reasoning: REQUIRED - Dictionary mapping attribute name to explanation of why CHECK constraint is needed (every attribute in check_constraint_attributes MUST have an entry)

If the attribute should NOT have a CHECK constraint, return empty lists and empty dicts with reasoning explaining why."""

    # Human prompt template
    human_prompt_template = """Determine if the categorical attribute {categorical_attribute} in entity {entity_name} should have a CHECK constraint.

{context}

Natural Language Description:
{nl_description}

Return a JSON object indicating whether this attribute should have a CHECK constraint, what values should be allowed, and reasoning."""

    # Initialize model and create chain
    llm = get_model_for_step("4.5")  # Step 4.5 maps to "high_fanout" task type
    try:
        config = get_trace_config("4.5", phase=4, tags=["phase_4_step_5"])
        result: CheckConstraintDetectionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CheckConstraintDetectionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "categorical_attribute": categorical_attribute,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        output_dict = result.model_dump()
        
        # Validate that all check constraint attributes have reasoning and definitions
        check_attrs = output_dict.get("check_constraint_attributes", [])
        definitions = output_dict.get("check_constraint_definitions", {})
        reasoning = output_dict.get("reasoning", {})
        
        missing_reasoning = [attr for attr in check_attrs if attr not in reasoning]
        missing_definitions = [attr for attr in check_attrs if attr not in definitions]
        
        if missing_reasoning:
            logger.warning(
                f"Missing reasoning for CHECK constraint attributes {missing_reasoning} in entity {entity_name}. "
                f"Adding default reasoning."
            )
            for attr in missing_reasoning:
                reasoning[attr] = f"Attribute {attr} should have CHECK constraint but reasoning not provided"
        
        if missing_definitions:
            logger.warning(
                f"Missing definitions for CHECK constraint attributes {missing_definitions} in entity {entity_name}. "
                f"These attributes will be skipped."
            )
            # Remove attributes without definitions
            output_dict["check_constraint_attributes"] = [
                attr for attr in check_attrs if attr in definitions
            ]
        
        output_dict["reasoning"] = reasoning
        
        logger.debug(
            f"CHECK constraint detection completed for {entity_name}.{categorical_attribute}: "
            f"{len(output_dict.get('check_constraint_attributes', []))} constraints"
        )
        return output_dict
        
    except Exception as e:
        logger.error(
            f"Error detecting CHECK constraint for {entity_name}.{categorical_attribute}: {e}",
            exc_info=True
        )
        raise


async def step_4_5_check_constraint_detection_batch(
    entity_categorical_attributes: Dict[str, List[str]],  # entity_name -> list of categorical attribute names
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> all attributes with descriptions
    entity_attribute_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # entity_name -> {attr: type_info}
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.5: Detect CHECK constraints for all categorical attributes (parallel execution).
    
    Args:
        entity_categorical_attributes: Dictionary mapping entity names to their categorical attributes from Step 4.4
        entity_attributes: Dictionary mapping entity names to all their attributes with descriptions
        entity_attribute_types: Optional dictionary mapping entity names to their attribute types from Step 4.3
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: CHECK constraint detection results organized by entity, then by attribute
        
    Example:
        >>> result = await step_4_5_check_constraint_detection_batch(
        ...     entity_categorical_attributes={"Order": ["status"]},
        ...     entity_attributes={"Order": [{"name": "status", "description": "Order status"}]}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(
        f"Starting Step 4.5: CHECK Constraint Detection for categorical attributes"
    )
    
    if not entity_categorical_attributes:
        logger.warning("No categorical attributes provided for CHECK constraint detection")
        return {"entity_results": {}}
    
    # Execute in parallel for all categorical attributes across all entities
    import asyncio
    
    tasks = []
    for entity_name, categorical_attrs in entity_categorical_attributes.items():
        # Get entity attributes for descriptions
        all_attrs = entity_attributes.get(entity_name, [])
        attr_dict = {extract_attribute_name(attr): attr for attr in all_attrs}
        
        # Get attribute types if available
        attr_types = (entity_attribute_types or {}).get(entity_name, {})
        
        for cat_attr in categorical_attrs:
            attr_info = attr_dict.get(cat_attr, {})
            attr_desc = extract_attribute_description(attr_info) if attr_info else ""
            attr_type = attr_types.get(cat_attr, {}).get("type") if cat_attr in attr_types else None
            
            task = step_4_5_check_constraint_detection(
                entity_name=entity_name,
                categorical_attribute=cat_attr,
                attribute_description=attr_desc,
                attribute_type=attr_type,
                nl_description=nl_description,
                domain=domain,
            )
            tasks.append((entity_name, cat_attr, task))
    
    if not tasks:
        logger.warning("No categorical attributes to process for CHECK constraint detection")
        return {"entity_results": {}}
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, _, task in tasks],
        return_exceptions=True
    )
    
    # Process results - organize by entity, then by attribute
    entity_results = {}
    for i, ((entity_name, cat_attr, _), result) in enumerate(zip(tasks, results)):
        if entity_name not in entity_results:
            entity_results[entity_name] = {}
        
        if isinstance(result, Exception):
            logger.error(f"Error processing {entity_name}.{cat_attr}: {result}")
            entity_results[entity_name][cat_attr] = {
                "check_constraint_attributes": [],
                "check_constraint_definitions": {},
                "reasoning": {},
                "error": str(result)
            }
        else:
            entity_results[entity_name][cat_attr] = result
    
    # Aggregate results per entity
    aggregated_results = {}
    for entity_name, attr_results in entity_results.items():
        all_check_attrs = []
        all_definitions = {}
        all_reasoning = {}
        
        for attr_name, attr_result in attr_results.items():
            if attr_result.get("error"):
                continue
            check_attrs = attr_result.get("check_constraint_attributes", [])
            definitions = attr_result.get("check_constraint_definitions", {})
            reasoning = attr_result.get("reasoning", {})
            
            all_check_attrs.extend(check_attrs)
            all_definitions.update(definitions)
            all_reasoning.update(reasoning)
        
        aggregated_results[entity_name] = {
            "check_constraint_attributes": all_check_attrs,
            "check_constraint_definitions": all_definitions,
            "reasoning": all_reasoning,
            "per_attribute_results": attr_results  # Keep detailed results
        }
    
    total_constraints = sum(
        len(result.get("check_constraint_attributes", []))
        for result in aggregated_results.values()
    )
    logger.info(
        f"CHECK constraint detection completed: {len(aggregated_results)} entities, {total_constraints} total CHECK constraints"
    )
    
    return {"entity_results": aggregated_results}

