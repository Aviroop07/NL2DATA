"""Phase 4, Step 4.6: Categorical Value Extraction.

Extract the possible values for categorical attributes.
Needed to define the value space for categorical distribution generation.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase4.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.pipeline_config import get_phase4_config
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
)

logger = get_logger(__name__)


class CategoricalValueExtractionOutput(BaseModel):
    """Output structure for categorical value extraction."""
    values: List[str] = Field(
        description="List of possible values for this categorical attribute"
    )
    source: str = Field(
        description="Source of the values: 'explicit' (explicitly mentioned in NL), 'inferred' (inferred from context), or 'domain' (common domain values)"
    )
    reasoning: str = Field(description="REQUIRED - Explanation of why these values were extracted and their source (cannot be omitted)")


@traceable_step("4.6", phase=4, tags=['phase_4_step_6'])
async def step_4_6_categorical_value_extraction(
    entity_name: str,
    categorical_attribute: str,  # Name of the categorical attribute
    attribute_description: Optional[str] = None,  # Description of the attribute
    attribute_type: Optional[str] = None,  # SQL type from Step 4.3
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.6 (per-categorical-attribute): Extract possible values for a categorical attribute.
    
    This is designed to be called in parallel for multiple categorical attributes.
    
    Args:
        entity_name: Name of the entity
        categorical_attribute: Name of the categorical attribute
        attribute_description: Optional description of the attribute
        nl_description: Optional original NL description
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Categorical value extraction result with values list, source, and reasoning
        
    Example:
        >>> result = await step_4_6_categorical_value_extraction(
        ...     entity_name="Order",
        ...     categorical_attribute="status"
        ... )
        >>> len(result["values"]) > 0
        True
    """
    logger.debug(f"Extracting categorical values for {entity_name}.{categorical_attribute}")
    cfg = get_phase4_config()
    
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
    system_prompt = """You are a database schema design expert. Your task is to extract the possible values for a categorical attribute.

CATEGORICAL VALUES:
Categorical attributes have a fixed set of possible values. Your task is to identify all valid values for this attribute.

EXTRACTION STRATEGY:
1. **Explicit mentions**: If the NL description explicitly lists values (e.g., "status can be 'active', 'inactive', or 'pending'"), extract those
2. **Inferred from context**: If values are implied (e.g., "order status" implies 'pending', 'processing', 'shipped', 'delivered', 'cancelled')
3. **Domain knowledge**: Use common domain patterns (e.g., status fields often have 'active'/'inactive', order statuses have lifecycle stages)
4. **Attribute name**: The attribute name itself may suggest values (e.g., "priority" suggests 'low'/'medium'/'high')

VALUE FORMATTING:
- Use lowercase, snake_case, or the exact format mentioned in the description
- Be consistent with naming conventions
- Include all reasonable values (don't be too restrictive, but don't include everything)
- Typically 2-10 values for most categorical attributes

Return a JSON object with:
- values: List of possible values for this categorical attribute (e.g., ["active", "inactive", "pending"])
- source: One of "explicit" (explicitly mentioned), "inferred" (inferred from context), or "domain" (common domain values)
- reasoning: REQUIRED - Explanation of why these values were extracted and their source (cannot be omitted)

Important:
- Extract ALL reasonable values, not just the most common ones
- Be comprehensive but realistic (don't include every possible value if there are too many)
- If the description doesn't provide enough information, use domain knowledge to infer reasonable values"""

    # Human prompt template
    human_prompt_template = """Extract possible values for the categorical attribute {categorical_attribute} in entity {entity_name}.

{context}

Return a JSON object with the list of possible values, their source, and reasoning.

IMPORTANT:
- Do NOT use the original NL description here; decide based on the attribute + its SQL type."""

    # Initialize model and create chain
    llm = get_model_for_step("4.6")  # Step 4.6 maps to "high_fanout" task type
    try:
        config = get_trace_config("4.6", phase=4, tags=["phase_4_step_6"])
        result: CategoricalValueExtractionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CategoricalValueExtractionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "categorical_attribute": categorical_attribute,
                "context": context_msg,
                "nl_section": "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        output_dict = result.model_dump()
        
        # Validate that values list is not empty
        values = output_dict.get("values", [])

        # Deterministic type-safety:
        # - If SQL type is numeric, drop non-numeric categorical values.
        # - If SQL type is boolean, restrict to true/false (or 0/1).
        # - If attribute looks like an identifier (*_id), treat it as non-categorical.
        sql_t = (attribute_type or "").strip().upper()
        attr_lower = (categorical_attribute or "").lower()
        if attr_lower.endswith("_id"):
            output_dict["values"] = []
            output_dict["source"] = "inferred"
            output_dict["reasoning"] = "ID-like attribute (*_id) is treated as identifier; categorical values skipped."
            return output_dict

        def _is_numeric_type(t: str) -> bool:
            return any(x in t for x in ["INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "BIGINT", "SMALLINT"])

        def _is_boolean_type(t: str) -> bool:
            return "BOOL" in t

        if sql_t:
            if _is_numeric_type(sql_t):
                kept: List[str] = []
                for v in values:
                    try:
                        float(str(v))
                        kept.append(v)
                    except Exception:
                        continue
                output_dict["values"] = kept
                if not kept and values:
                    output_dict["reasoning"] = (
                        (output_dict.get("reasoning") or "")
                        + " (All suggested values were non-numeric but SQL type is numeric; dropped.)"
                    ).strip()
            elif _is_boolean_type(sql_t):
                allowed = {"true", "false", "0", "1"}
                kept = [v for v in values if str(v).strip().lower() in allowed]
                output_dict["values"] = kept
                if not kept and values:
                    output_dict["reasoning"] = (
                        (output_dict.get("reasoning") or "")
                        + " (SQL type is BOOLEAN; non-boolean values dropped.)"
                    ).strip()
        if not values:
            logger.warning(
                f"No values extracted for {entity_name}.{categorical_attribute}. "
                f"This may indicate the attribute is not truly categorical."
            )
        
        logger.debug(
            f"Categorical value extraction completed for {entity_name}.{categorical_attribute}: "
            f"{len(values)} values from {output_dict.get('source', 'unknown')}"
        )
        return output_dict
        
    except Exception as e:
        logger.error(
            f"Error extracting categorical values for {entity_name}.{categorical_attribute}: {e}",
            exc_info=True
        )
        raise


async def step_4_6_categorical_value_extraction_batch(
    entity_categorical_attributes: Dict[str, List[str]],  # entity_name -> list of categorical attribute names
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> all attributes with descriptions
    entity_attribute_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # entity_name -> {attr: type_info}
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.6: Extract categorical values for all categorical attributes (parallel execution).
    
    Args:
        entity_categorical_attributes: Dictionary mapping entity names to their categorical attributes from Step 4.4
        entity_attributes: Dictionary mapping entity names to all their attributes with descriptions
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Categorical value extraction results organized by entity, then by attribute
        
    Example:
        >>> result = await step_4_6_categorical_value_extraction_batch(
        ...     entity_categorical_attributes={"Order": ["status"]},
        ...     entity_attributes={"Order": [{"name": "status", "description": "Order status"}]}
        ... )
        >>> "Order" in result["entity_results"]
        True
    """
    logger.info(
        f"Starting Step 4.6: Categorical Value Extraction for categorical attributes"
    )
    
    if not entity_categorical_attributes:
        logger.warning("No categorical attributes provided for value extraction")
        return {"entity_results": {}}
    
    # Execute in parallel for all categorical attributes across all entities
    import asyncio
    
    tasks = []
    for entity_name, categorical_attrs in entity_categorical_attributes.items():
        # Get entity attributes for descriptions
        all_attrs = entity_attributes.get(entity_name, [])
        attr_dict = {extract_attribute_name(attr): attr for attr in all_attrs}
        attr_types = (entity_attribute_types or {}).get(entity_name, {})
        
        for cat_attr in categorical_attrs:
            attr_info = attr_dict.get(cat_attr, {})
            attr_desc = extract_attribute_description(attr_info) if attr_info else ""
            attr_type = attr_types.get(cat_attr, {}).get("type") if cat_attr in attr_types else None
            
            task = step_4_6_categorical_value_extraction(
                entity_name=entity_name,
                categorical_attribute=cat_attr,
                attribute_description=attr_desc,
                attribute_type=attr_type,
                nl_description=nl_description,
                domain=domain,
            )
            tasks.append((entity_name, cat_attr, task))
    
    if not tasks:
        logger.warning("No categorical attributes to process for value extraction")
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
                "values": [],
                "source": "error",
                "reasoning": f"Error during extraction: {str(result)}",
                "error": str(result)
            }
        else:
            entity_results[entity_name][cat_attr] = result
    
    total_values = sum(
        len(result.get("values", []))
        for entity_results_dict in entity_results.values()
        for result in entity_results_dict.values()
        if not result.get("error")
    )
    logger.info(
        f"Categorical value extraction completed: {len(entity_results)} entities, {total_values} total values extracted"
    )
    
    return {"entity_results": entity_results}

