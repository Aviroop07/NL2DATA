"""Phase 8, Step 8.2: Categorical Column Identification.

Identify categorical/enum columns from the relational schema.
This step runs before constraint detection to identify columns that may need categorical constraints.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.phases.phase8.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class EntityCategoricalResult(BaseModel):
    """Result for a single entity."""
    categorical_attributes: List[str] = Field(
        default_factory=list,
        description="List of categorical attribute names"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Reasoning for the identification"
    )

    model_config = ConfigDict(extra="forbid")


class CategoricalIdentificationOutput(BaseModel):
    """Overall output structure."""
    entity_results: Dict[str, EntityCategoricalResult] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their categorical attribute results"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("8.2", phase=8, tags=['phase_8_step_2'])
async def step_8_2_categorical_column_identification_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],
    entity_attribute_types: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    relational_schema: Optional[Dict[str, Any]] = None,
    nl_description: str = "",
    domain: Optional[str] = None,
    derived_formulas: Optional[Dict[str, Any]] = None,
    multivalued_derived: Optional[Dict[str, Any]] = None,
) -> CategoricalIdentificationOutput:
    """
    Step 8.2 (batch, LLM): Identify categorical/enum columns from schema.
    
    This step identifies columns that represent categorical/enum values (e.g., status, type, category)
    which may need categorical constraints or value lists.
    
    Args:
        entities: List of entity dictionaries
        entity_attributes: Dictionary mapping entity names to their attribute lists
        entity_attribute_types: Optional dictionary mapping entity names to attribute type info
        relational_schema: Optional relational schema (tables/columns structure)
        nl_description: Natural language description
        domain: Optional domain context
        
    Returns:
        dict: Categorical column identification result with entity_results:
            {
                "entity_results": {
                    "EntityName": {
                        "categorical_attributes": ["attr1", "attr2", ...],
                        "reasoning": "..."
                    }
                }
            }
    """
    logger.info("Starting Step 8.2: Categorical Column Identification")
    
    # Get derived columns to exclude
    derived_columns = set()
    if derived_formulas:
        for key in derived_formulas.keys():
            if isinstance(key, str) and "." in key:
                derived_columns.add(key)
    if multivalued_derived:
        for entity_name, mv_result in multivalued_derived.items():
            if isinstance(mv_result, dict):
                derived_attrs = mv_result.get("derived", [])
                for attr_name in derived_attrs:
                    if isinstance(attr_name, str):
                        derived_columns.add(f"{entity_name}.{attr_name}")
    
    if derived_columns:
        logger.info(f"Excluding {len(derived_columns)} derived columns from categorical identification")
    
    if not entities:
        logger.warning("No entities provided for categorical column identification")
        return CategoricalIdentificationOutput(entity_results={})
    
    # Build entity summary for prompt (excluding derived columns)
    entity_summaries = []
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else ""
        attrs = entity_attributes.get(entity_name, [])
        
        # Get attribute types if available
        attr_types = entity_attribute_types.get(entity_name, {}) if entity_attribute_types else {}
        attr_type_info = attr_types.get("attribute_types", {}) if isinstance(attr_types, dict) else {}
        
        attr_list = []
        for attr in attrs:
            attr_name = attr.get("name", "") if isinstance(attr, dict) else str(attr)
            
            # Skip derived columns
            column_key = f"{entity_name}.{attr_name}"
            if column_key in derived_columns:
                logger.debug(f"Skipping derived column {column_key} from categorical identification")
                continue
            
            attr_desc = attr.get("description", "") if isinstance(attr, dict) else ""
            type_info = attr_type_info.get(attr_name, {})
            sql_type = type_info.get("type", "UNKNOWN") if isinstance(type_info, dict) else "UNKNOWN"
            attr_list.append(f"  - {attr_name} ({sql_type}): {attr_desc}")
        
        entity_summaries.append(f"{entity_name}: {entity_desc}\nAttributes:\n" + "\n".join(attr_list))
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=CategoricalIdentificationOutput,
        additional_requirements=[
            "The 'entity_results' dictionary must have entity names as keys",
            "Each entity's 'categorical_attributes' must be a list of attribute name strings (not objects with 'attribute' and 'reasoning' fields)",
            "Each entity result must include 'categorical_attributes' (list of strings) and optionally 'reasoning' (string)",
            "Do NOT nest 'categorical_attributes' inside objects - it should be a direct list of attribute name strings"
        ]
    )
    
    # Build prompt
    system_prompt = f"""You are a database schema analyst. Your task is to identify categorical/enum columns.

Categorical columns are attributes that:
- Have a limited, discrete set of possible values (e.g., status: 'active', 'inactive', 'pending')
- Represent categories, types, or classifications
- Are typically stored as VARCHAR/TEXT but have enum-like behavior
- Examples: status, type, category, state, priority, role, level, grade

Do NOT identify:
- Primary keys or foreign keys (unless they're also categorical)
- Numeric identifiers (IDs, codes that are just numbers)
- Free-form text fields (descriptions, names, comments)
- Date/timestamp fields
- Boolean fields (they're already categorical but handled separately)

For each entity, identify which attributes are categorical and provide reasoning.

{output_structure_section}"""
    
    human_prompt = f"""Analyze the following entities and identify categorical/enum columns:

{chr(10).join(entity_summaries)}

Domain: {domain or 'Not specified'}
Context: {nl_description[:500] if nl_description else 'No additional context'}

Identify categorical attributes for each entity."""
    
    
    # Get model
    llm = get_model_for_step("8.2")
    
    # Make LLM call
    trace_config = get_trace_config("8.2", phase=8, tags=["phase_8_step_2"])
    
    try:
        result = await standardized_llm_call(
            llm=llm,
            output_schema=CategoricalIdentificationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},
        )
        
        # Ensure all entities have results (even if empty)
        entity_results = result.entity_results if hasattr(result, 'entity_results') else result.model_dump().get("entity_results", {})
        for entity in entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
            if entity_name not in entity_results:
                entity_results[entity_name] = EntityCategoricalResult(
                    categorical_attributes=[],
                    reasoning="No categorical attributes identified"
                )
        
        # Filter out boolean columns deterministically
        # Get data types to check column types
        filtered_entity_results = {}
        for entity_name, cat_result in entity_results.items():
            # Get categorical attributes
            if hasattr(cat_result, 'categorical_attributes'):
                cat_attrs = cat_result.categorical_attributes
            elif isinstance(cat_result, dict):
                cat_attrs = cat_result.get("categorical_attributes", [])
            else:
                cat_attrs = []
            
            # Get data types for this entity
            entity_data_types = entity_attribute_types.get(entity_name, {}) if entity_attribute_types else {}
            attr_types = entity_data_types.get("attribute_types", {}) if isinstance(entity_data_types, dict) else {}
            
            # Filter out boolean columns and derived columns
            filtered_cat_attrs = []
            for attr_name in cat_attrs:
                # Skip derived columns
                column_key = f"{entity_name}.{attr_name}"
                if column_key in derived_columns:
                    logger.debug(f"Filtering out {column_key} - derived columns are not treated as categorical")
                    continue
                
                type_info = attr_types.get(attr_name, {}) if isinstance(attr_types, dict) else {}
                sql_type = type_info.get("type", "VARCHAR(255)") if isinstance(type_info, dict) else "VARCHAR(255)"
                base_type = sql_type.upper().strip().split("(")[0].strip()
                
                if base_type in ("BOOLEAN", "BOOL"):
                    logger.debug(f"Filtering out {entity_name}.{attr_name} - boolean columns are not treated as categorical")
                    continue
                
                filtered_cat_attrs.append(attr_name)
            
            # Create filtered result
            if hasattr(cat_result, 'reasoning'):
                reasoning = cat_result.reasoning
            elif isinstance(cat_result, dict):
                reasoning = cat_result.get("reasoning", "")
            else:
                reasoning = ""
            
            filtered_entity_results[entity_name] = EntityCategoricalResult(
                categorical_attributes=filtered_cat_attrs,
                reasoning=reasoning
            )
        
        total_categorical = sum(
            len(r.categorical_attributes if hasattr(r, 'categorical_attributes') else r.get("categorical_attributes", []))
            for r in filtered_entity_results.values()
        )
        
        logger.info(f"Step 8.2 completed: Identified {total_categorical} categorical attributes across {len(filtered_entity_results)} entities (boolean columns filtered out)")
        
        return CategoricalIdentificationOutput(entity_results=filtered_entity_results)
        
    except Exception as e:
        logger.error(f"Error in Step 8.2: {e}", exc_info=True)
        # Return empty results on error
        return CategoricalIdentificationOutput(
            entity_results={
                entity.get("name", "") if isinstance(entity, dict) else str(entity): EntityCategoricalResult(
                    categorical_attributes=[],
                    reasoning=f"Error during identification: {str(e)}"
                )
                for entity in entities
            }
        )
