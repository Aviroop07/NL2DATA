"""Phase 7, Step 7.3: Boolean Dependency Analysis.

Determine if boolean attributes should be random or dependent on other attributes.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class BooleanDependencyAnalysisOutput(BaseModel):
    """Output structure for boolean dependency analysis."""
    is_random: bool = Field(description="Whether the boolean attribute should be random (True) or dependent (False)")
    dependency_dsl: Optional[str] = Field(
        default=None,
        description="DSL expression for dependency if is_random=False (e.g., 'IF subscription_type = \"premium\" THEN true ELSE false')"
    )
    reasoning: str = Field(description="Reasoning for the dependency analysis")


@traceable_step("7.3", phase=7, tags=['phase_7_step_3'])
async def step_7_3_boolean_dependency_analysis(
    attribute_name: str,
    attribute_description: Optional[str],
    entity_name: str,
    related_attributes: List[Dict[str, Any]],  # All related attributes (same entity and related entities)
    dsl_grammar: Optional[str] = None,  # DSL grammar specification
    entity_description: Optional[str] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
    constraints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Step 7.3 (per-boolean attribute, LLM): Analyze boolean dependencies.
    
    Args:
        attribute_name: Name of the boolean attribute
        attribute_description: Optional description
        entity_name: Name of the entity/table
        related_attributes: All related attributes with descriptions and types
        dsl_grammar: Optional DSL grammar specification
        entity_description: Optional entity description
        relations: Optional relations
        constraints: Optional constraints
        
    Returns:
        dict: Dependency analysis with is_random, dependency_dsl, reasoning
    """
    logger.debug(f"Analyzing boolean dependency for {entity_name}.{attribute_name}")
    
    # Get model
    model = get_model_for_step("7.3")
    
    # Create prompt
    system_prompt = """You are a data generation expert. Your task is to determine if boolean attributes should be random or dependent on other attributes.

DEPENDENCY ANALYSIS:
- **Random**: Attribute has no correlation with other attributes (e.g., random flags)
- **Dependent**: Attribute depends on other attributes (e.g., is_premium depends on subscription_type)

If dependent, create a DSL expression using IF-THEN-ELSE or conditional logic.
Use the provided DSL grammar to create valid expressions."""
    
    related_attrs_context = "\n".join(
        f"- {attr.get('entity_name', '')}.{attr.get('attribute_name', '')}: {attr.get('attribute_type', '')} - {attr.get('attribute_description', '')}"
        for attr in related_attributes[:20]
    )
    
    dsl_context = f"\n\nDSL Grammar:\n{dsl_grammar}" if dsl_grammar else ""
    
    human_prompt = f"""Boolean Attribute: {entity_name}.{attribute_name}
Description: {attribute_description or 'No description'}
Entity: {entity_description or 'No description'}

Related Attributes:
{related_attrs_context}
{dsl_context}

Determine if this boolean attribute should be random or dependent on other attributes. If dependent, provide a DSL expression."""
    
    # Create structured chain
    # Invoke standardized LLM call
    try:
        result: BooleanDependencyAnalysisOutput = await standardized_llm_call(
            llm=model,
            output_schema=BooleanDependencyAnalysisOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
        )
        
        # Work with Pydantic model directly
        # Convert to dict only at return boundary
        return {
            "is_random": result.is_random,
            "dependency_dsl": result.dependency_dsl,
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"Boolean dependency analysis failed: {e}")
        raise


async def step_7_3_boolean_dependency_analysis_batch(
    boolean_attributes: List[Dict[str, Any]],  # List of boolean attributes with metadata
    related_attributes_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,  # attribute -> related attributes
    dsl_grammar: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Analyze dependencies for multiple boolean attributes in parallel."""
    import asyncio
    
    tasks = []
    attribute_keys = []
    
    for attr in boolean_attributes:
        key = f"{attr.get('entity_name', '')}.{attr.get('attribute_name', '')}"
        attribute_keys.append(key)
        
        related = []
        if related_attributes_map and key in related_attributes_map:
            related = related_attributes_map[key]
        
        tasks.append(
            step_7_3_boolean_dependency_analysis(
                attribute_name=attr.get("attribute_name", ""),
                attribute_description=attr.get("attribute_description"),
                entity_name=attr.get("entity_name", ""),
                related_attributes=related,
                dsl_grammar=dsl_grammar,
                entity_description=attr.get("entity_description"),
                relations=attr.get("relations"),
                constraints=attr.get("constraints"),
            )
        )
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = {}
    for key, result in zip(attribute_keys, results):
        if isinstance(result, Exception):
            logger.error(f"Boolean dependency analysis failed for {key}: {result}")
            output[key] = {
                "is_random": True,
                "dependency_dsl": None,
                "reasoning": f"Analysis failed: {str(result)}"
            }
        else:
            output[key] = result
    
    return output

