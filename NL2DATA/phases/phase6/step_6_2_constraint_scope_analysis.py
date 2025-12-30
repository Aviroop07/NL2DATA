"""Phase 6, Step 6.2: Constraint Scope Analysis.

Identify which entities and attributes are affected by each constraint.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase6.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _check_schema_component_exists_impl

logger = get_logger(__name__)


class ConstraintScopeAnalysisOutput(BaseModel):
    """Output structure for constraint scope analysis."""
    affected_entities: List[str] = Field(description="List of entity/table names affected by this constraint")
    affected_attributes: List[str] = Field(description="List of attribute/column names affected (format: 'entity.attribute')")
    constraint_category: str = Field(description="Category of constraint: 'statistical', 'structural', 'distribution', 'other'")
    reasoning: str = Field(description="Reasoning for the scope analysis")


@traceable_step("6.2", phase=6, tags=['phase_6_step_2'])
async def step_6_2_constraint_scope_analysis(
    constraint_description: str,  # Constraint description from Step 6.1
    constraint_category: str,  # Category: statistical, structural, distribution, other
    normalized_schema: Dict[str, Any],  # Full normalized schema from Phase 4
    phase2_constraints: Optional[List[Dict[str, Any]]] = None,  # Constraints from Phase 2 (Step 2.13)
) -> Dict[str, Any]:
    """
    Step 6.2 (per-constraint, LLM): Identify which entities and attributes are affected.
    
    Args:
        constraint_description: Constraint description from Step 6.1
        constraint_category: Category of the constraint
        normalized_schema: Full normalized schema from Phase 4
        phase2_constraints: Optional constraints from Phase 2
        
    Returns:
        dict: Scope analysis result with affected_entities, affected_attributes, constraint_category, reasoning
    """
    logger.debug(f"Analyzing scope for constraint: {constraint_description}")
    
    # Build schema context
    schema_summary = []
    tables = normalized_schema.get("normalized_tables", [])
    for table in tables:
        table_name = table.get("name", "")
        columns = [col.get("name", "") for col in table.get("columns", [])]
        schema_summary.append(f"Table {table_name}: {columns}")
    
    # Get model
    model = get_model_for_step("6.2")
    
    # Create prompt
    system_prompt = """You are a database constraint analysis expert. Your task is to identify which entities and attributes are affected by a constraint.

SCOPE ANALYSIS:
- Identify all tables (entities) that are affected by the constraint
- Identify all columns (attributes) that are affected (format: 'table.column')
- Determine the constraint category if not already specified
- Provide clear reasoning for your analysis

You have access to a validation tool: check_schema_component_exists_bound. Use this tool to verify that entities and attributes exist in the schema before finalizing your response.

CRITICAL: When calling check_schema_component_exists_bound, you MUST provide arguments as a JSON object:
- CORRECT: {{"component_type": "table", "name": "Customer"}}
- WRONG: ["component_type", "name"] or [] or "component_type"

CRITICAL OUTPUT FORMAT:
After using tools (if needed), you MUST return ONLY a valid JSON object with this exact structure:
{
  "affected_entities": ["Entity1", "Entity2"],
  "affected_attributes": ["Entity1.attribute1", "Entity2.attribute2"],
  "constraint_category": "structural",
  "reasoning": "Your reasoning here"
}

DO NOT return markdown, explanations, or any text outside the JSON object. Return ONLY the JSON object."""
    
    human_prompt = f"""Constraint:
Description: {constraint_description}
Category: {constraint_category}

Schema:
{chr(10).join(schema_summary[:20])}  # Limit for context

Identify which entities and attributes are affected by this constraint."""
    
    # Create bound version of check_schema_component_exists with normalized_schema
    def check_schema_component_exists_bound(component_type: str, name: str) -> bool:
        """Bound version of check_schema_component_exists with normalized_schema."""
        schema_state = {
            "entities": [
                {"name": table.get("name", "")} 
                for table in normalized_schema.get("normalized_tables", [])
            ],
            "attributes": {
                table.get("name", ""): [
                    {"name": col.get("name", "")} 
                    for col in table.get("columns", [])
                ]
                for table in normalized_schema.get("normalized_tables", [])
            }
        }
        # NOTE: check_schema_component_exists is a LangChain @tool (StructuredTool) and is not callable.
        return _check_schema_component_exists_impl(component_type, name, schema_state)
    
    # Invoke standardized LLM call with schema validation tools
    try:
        config = get_trace_config("6.2", phase=6, tags=["constraint_scope"])
        result: ConstraintScopeAnalysisOutput = await standardized_llm_call(
            llm=model,
            output_schema=ConstraintScopeAnalysisOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
            tools=[check_schema_component_exists_bound],
            use_agent_executor=True,  # Use agent executor for tool calls
            decouple_tools=True,  # Decouple tool calling from JSON generation
            config=config,
        )
        
        # Work with Pydantic model directly
        # Convert to dict only at return boundary
        return {
            "affected_entities": result.affected_entities,
            "affected_attributes": result.affected_attributes,
            "constraint_category": result.constraint_category,
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"Constraint scope analysis failed: {e}")
        raise


async def step_6_2_constraint_scope_analysis_batch(
    constraints: List[Dict[str, Any]],  # List of constraints from Step 6.1
    normalized_schema: Dict[str, Any],
    phase2_constraints: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Analyze scope for multiple constraints in parallel."""
    import asyncio
    
    tasks = [
        step_6_2_constraint_scope_analysis(
            constraint_description=constraint.get("description", ""),
            constraint_category=constraint.get("category", "other"),
            normalized_schema=normalized_schema,
            phase2_constraints=phase2_constraints,
        )
        for constraint in constraints
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = []
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Scope analysis failed for constraint {idx}: {result}")
            output.append({
                "affected_entities": [],
                "affected_attributes": [],
                "constraint_category": "error",
                "reasoning": f"Analysis failed: {str(result)}"
            })
        else:
            output.append(result)
    
    return output

