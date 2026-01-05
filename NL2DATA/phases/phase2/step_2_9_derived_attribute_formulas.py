"""Phase 2, Step 2.9: Derived Attribute Formulas.

Generate formulas (DSL expressions) for derived attributes.
Formulas can ONLY reference attributes within the same entity (no cross-entity dependencies).
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class DerivedFormulaOutput(BaseModel):
    """Output structure for derived attribute formula."""
    formula: str = Field(
        description="DSL expression for calculating the derived attribute (e.g., 'quantity * unit_price'). Can ONLY reference attributes from the same entity."
    )
    dependencies: List[str] = Field(
        description="List of attribute names that this derived attribute depends on (must be from the same entity)"
    )
    formula_type: str = Field(
        description="Type of formula: 'arithmetic', 'aggregation', 'string_operation', 'date_operation', 'conditional'"
    )
    reasoning: str = Field(
        description="Reasoning for the formula"
    )


class DerivedFormulaBatchOutput(BaseModel):
    """Output structure for Step 2.9 batch processing."""
    entity_results: List[DerivedFormulaOutput] = Field(
        description="List of derived formula results, one per derived attribute"
    )
    total_derived_attributes: int = Field(
        description="Total number of derived attributes processed"
    )


@traceable_step("2.9", phase=2, tags=['phase_2_step_9'])
async def step_2_9_derived_attribute_formulas(
    entity_name: str,
    attribute_name: str,
    entity_attributes: List,  # All attributes for this entity (with descriptions)
    entity_description: Optional[str],
    derivation_rules: Optional[dict] = None,  # attribute_name -> formula hint
    nl_description: Optional[str] = None,
) -> DerivedFormulaOutput:
    """
    Step 2.9 (per-derived attribute, LLM): Generate formula for a derived attribute.
    
    IMPORTANT:
    - Formula can ONLY reference attributes from the same entity
    - NO cross-entity dependencies allowed
    - All dependencies must be within entity_attributes list
    
    Args:
        entity_name: Name of the entity
        attribute_name: Name of the derived attribute
        entity_attributes: List of all attributes for this entity (with descriptions)
        entity_description: Description of the entity
        derivation_rules: Optional dictionary mapping attribute names to formula hints from NL
        nl_description: Original natural language description
        
    Returns:
        dict: Formula result with formula, dependencies, formula_type, reasoning
        
    Example:
        >>> result = await step_2_9_derived_attribute_formulas("Order", "total_price", [...], "...")
        >>> "formula" in result
        True
    """
    logger.debug(f"Generating formula for derived attribute: {entity_name}.{attribute_name}")
    
    # Build attribute summary (only same-entity attributes)
    attr_summary = []
    attr_names = []
    for attr in entity_attributes:
        attr_name = attr.get("name", "") if isinstance(attr, dict) else str(attr)
        attr_desc = attr.get("description", "") if isinstance(attr, dict) else ""
        attr_type = attr.get("type", "") if isinstance(attr, dict) else ""
        attr_summary.append(f"- {attr_name}: {attr_desc} (type: {attr_type})")
        attr_names.append(attr_name)
    
    # Check for derivation rule hint
    derivation_hint = ""
    if derivation_rules and attribute_name in derivation_rules:
        derivation_hint = f"\n\nHint from natural language: {derivation_rules[attribute_name]}"
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=DerivedFormulaOutput,
        additional_requirements=[
            "Derived attributes can ONLY reference attributes from the SAME entity",
            "NO cross-entity dependencies are allowed",
            "All dependencies must be within the provided entity attributes list",
        ]
    )
    
    system_prompt = f"""You are a database formula generator. Create DSL expressions for derived attributes.

CRITICAL CONSTRAINT:
- Derived attributes can ONLY reference attributes from the SAME entity
- NO cross-entity dependencies are allowed
- All dependencies must be within the provided entity attributes list

The formula should:
- Use attribute names from the same entity only
- Support arithmetic operations (+, -, *, /)
- Support aggregations (SUM, AVG, COUNT, etc.) - but only on same-entity attributes
- Support string operations (CONCAT, SUBSTRING, etc.)
- Support date operations (DATEDIFF, DATEADD, etc.)
- Support conditional logic (IF, CASE, etc.)

Return a clear DSL expression that can be used to calculate the derived attribute using ONLY same-entity attributes.

{output_structure_section}"""
    
    human_prompt = f"""Entity: {entity_name}
Description: {entity_description or "Not provided"}

Derived Attribute: {attribute_name}

Available Attributes (SAME ENTITY ONLY):
{chr(10).join(attr_summary)}
{derivation_hint}

Natural Language Description:
{nl_description or "Not provided"}

Generate a DSL formula for calculating {attribute_name} from the available attributes.
REMEMBER: The formula can ONLY reference attributes from {entity_name} (same entity)."""
    
    llm = get_model_for_step("2.9")
    trace_config = get_trace_config("2.9", phase=2, tags=["phase_2_step_9"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=DerivedFormulaOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    # Validate dependencies are within same entity
    invalid_deps = [dep for dep in result.dependencies if dep not in attr_names]
    if invalid_deps:
        logger.warning(
            f"Step 2.9 for {entity_name}.{attribute_name}: Formula dependencies include attributes not in entity: {invalid_deps}. "
            f"Removing invalid dependencies."
        )
        result.dependencies = [dep for dep in result.dependencies if dep in attr_names]
    
    return result


class EntityDerivedFormulaResult(BaseModel):
    """Result for a single derived attribute in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    attribute_name: str = Field(description="Name of the derived attribute")
    formula: str = Field(description="DSL expression for calculating the derived attribute")
    dependencies: List[str] = Field(description="List of attribute names that this derived attribute depends on")
    formula_type: str = Field(description="Type of formula")
    reasoning: str = Field(description="Reasoning for the formula")


class DerivedFormulaBatchOutput(BaseModel):
    """Output structure for Step 2.9 batch processing."""
    entity_results: List[EntityDerivedFormulaResult] = Field(
        description="List of derived formula results, one per derived attribute"
    )
    total_derived_attributes: int = Field(
        description="Total number of derived attributes processed"
    )


async def step_2_9_derived_attribute_formulas_batch(
    entity_derived_attributes: dict,  # entity_name -> list of derived attribute names
    entity_attributes: dict,  # entity_name -> list of attributes
    entity_descriptions: Optional[dict] = None,  # entity_name -> description
    derivation_rules: Optional[dict] = None,  # entity_name -> {attr_name: formula_hint}
    nl_description: Optional[str] = None,
) -> DerivedFormulaBatchOutput:
    """
    Step 2.9: Generate formulas for all derived attributes (parallel execution).
    
    Args:
        entity_derived_attributes: Dictionary mapping entity names to their derived attribute names
        entity_attributes: Dictionary mapping entity names to their full attribute lists
        entity_descriptions: Optional dictionary mapping entity names to descriptions
        derivation_rules: Optional nested dictionary: entity_name -> {attr_name: formula_hint}
        nl_description: Original natural language description
        
    Returns:
        DerivedFormulaBatchOutput: Batch formula results
    """
    logger.info(f"Starting Step 2.9: Derived Attribute Formulas for {sum(len(attrs) for attrs in entity_derived_attributes.values())} derived attributes")
    
    import asyncio
    
    tasks = []
    task_metadata = []  # Store (entity_name, attribute_name) for each task
    for entity_name, derived_attrs in entity_derived_attributes.items():
        entity_desc = (entity_descriptions or {}).get(entity_name)
        attrs = entity_attributes.get(entity_name, [])
        entity_rules = (derivation_rules or {}).get(entity_name, {})
        
        for attr_name in derived_attrs:
            tasks.append(
                step_2_9_derived_attribute_formulas(
                    entity_name=entity_name,
                    attribute_name=attr_name,
                    entity_attributes=attrs,
                    entity_description=entity_desc,
                    derivation_rules=entity_rules,
                    nl_description=nl_description,
                )
            )
            task_metadata.append((entity_name, attr_name))
    
    results = await asyncio.gather(*tasks)
    
    # Build batch output with entity and attribute names
    entity_results_list = []
    for (entity_name, attr_name), result in zip(task_metadata, results):
        entity_results_list.append(
            EntityDerivedFormulaResult(
                entity_name=entity_name,
                attribute_name=attr_name,
                formula=result.formula,
                dependencies=result.dependencies,
                formula_type=result.formula_type,
                reasoning=result.reasoning,
            )
        )
    
    return DerivedFormulaBatchOutput(
        entity_results=entity_results_list,
        total_derived_attributes=len(results),
    )
