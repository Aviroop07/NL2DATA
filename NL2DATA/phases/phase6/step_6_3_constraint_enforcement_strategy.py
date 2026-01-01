"""Phase 6, Step 6.3: Constraint Enforcement Strategy.

Determine how to enforce each constraint using DSL expressions.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase6.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _validate_dsl_expression_impl

logger = get_logger(__name__)


class ConstraintEnforcementStrategyOutput(BaseModel):
    """Output structure for constraint enforcement strategy."""
    enforcement_type: str = Field(description="Type of enforcement: 'generation_strategy', 'check_constraint', 'application_logic'")
    dsl_expression: str = Field(description="DSL expression for enforcing the constraint (see DSL grammar)")
    reasoning: str = Field(description="Reasoning for the enforcement strategy")


@traceable_step("6.3", phase=6, tags=['phase_6_step_3'])
async def step_6_3_constraint_enforcement_strategy(
    constraint_description: str,  # Constraint description from Step 6.1
    constraint_category: str,  # Category from Step 6.2
    affected_components: Dict[str, Any],  # Output from Step 6.2
    normalized_schema: Dict[str, Any],  # Full normalized schema
    dsl_grammar: Optional[str] = None,  # DSL grammar specification (external context)
) -> Dict[str, Any]:
    """
    Step 6.3 (per-constraint, LLM): Determine enforcement strategy using DSL.
    
    Args:
        constraint_description: Constraint description
        constraint_category: Category of constraint
        affected_components: Output from Step 6.2 with affected entities/attributes
        normalized_schema: Full normalized schema
        dsl_grammar: Optional DSL grammar specification
        
    Returns:
        dict: Enforcement strategy with enforcement_type, dsl_expression, reasoning
    """
    logger.debug(f"Determining enforcement strategy for constraint: {constraint_description}")
    
    # Get model
    model = get_model_for_step("6.3")
    
    # Create prompt
    system_prompt = """You are a database constraint enforcement expert. Your task is to determine how to enforce constraints using DSL expressions.

ENFORCEMENT TYPES:
1. **generation_strategy**: Constraint enforced during data generation (e.g., distributions, patterns)
2. **check_constraint**: Constraint enforced as SQL CHECK constraint
3. **application_logic**: Constraint enforced in application code (for complex cases)

DSL EXPRESSIONS:
- Use the provided DSL grammar to create valid expressions
- Examples: "amount > 0", "fraud ~ Bernoulli(0.05)", "timestamp ~ Seasonal(month_day=[1,15])"
- Ensure expressions are parseable and executable

You have access to a validation tool: validate_dsl_expression. Use this tool to validate your DSL expression syntax before finalizing your response.

CRITICAL OUTPUT FORMAT:
After using the validation tool (if needed), you MUST return ONLY a valid JSON object with this exact structure:
{
  "enforcement_type": "check_constraint",
  "dsl_expression": "amount > 0",
  "reasoning": "Your reasoning here"
}

DO NOT return markdown, explanations, or any text outside the JSON object. Return ONLY the JSON object."""
    
    dsl_context = f"\n\nDSL Grammar:\n{dsl_grammar}" if dsl_grammar else ""
    
    human_prompt = f"""Constraint:
Description: {constraint_description}
Category: {constraint_category}
Affected: {affected_components.get('affected_entities', [])}, {affected_components.get('affected_attributes', [])}
{dsl_context}

Determine the enforcement strategy and create a DSL expression for this constraint."""
    
    # Create bound version of validate_dsl_expression with dsl_grammar
    # NOTE: validate_dsl_expression is a LangChain @tool (StructuredTool) and is not callable like a function.
    # Use the pure implementation to avoid "'StructuredTool' object is not callable".
    def validate_dsl_expression_bound(dsl: str) -> Dict[str, Any]:
        """Bound version of validate_dsl_expression with dsl_grammar.
        
        Args:
            dsl: DSL expression to validate. Must be a valid DSL expression string.
                 Example: "amount > 0"
        
        Returns:
            Dictionary with validation results
        
        IMPORTANT: When calling this tool, provide arguments as a JSON object:
        {"dsl": "amount > 0"}
        NOT as a list: ["dsl"] (WRONG)
        """
        # Use the pure implementation (grammar is optional and not used in current implementation)
        return _validate_dsl_expression_impl(dsl, grammar=dsl_grammar)
    
    # Invoke standardized LLM call with DSL validation tools
    try:
        config = get_trace_config("6.3", phase=6, tags=["constraint_enforcement"])
        result: ConstraintEnforcementStrategyOutput = await standardized_llm_call(
            llm=model,
            output_schema=ConstraintEnforcementStrategyOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
            tools=[validate_dsl_expression_bound],
            use_agent_executor=True,  # Use agent executor for tool calls
            decouple_tools=True,  # Decouple tool calling from JSON generation
            config=config,
        )
        
        # Work with Pydantic model directly
        # Convert to dict only at return boundary
        return {
            "enforcement_type": result.enforcement_type,
            "dsl_expression": result.dsl_expression,
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"Constraint enforcement strategy failed: {e}")
        raise


async def step_6_3_constraint_enforcement_strategy_batch(
    constraints_with_scope: List[Dict[str, Any]],  # Constraints with scope analysis from Step 6.2
    normalized_schema: Dict[str, Any],
    dsl_grammar: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Determine enforcement strategy for multiple constraints in parallel."""
    import asyncio
    
    tasks = [
        step_6_3_constraint_enforcement_strategy(
            constraint_description=constraint.get("description", ""),
            constraint_category=constraint.get("category", "other"),
            affected_components=constraint.get("scope", {}),
            normalized_schema=normalized_schema,
            dsl_grammar=dsl_grammar,
        )
        for constraint in constraints_with_scope
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = []
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Enforcement strategy failed for constraint {idx}: {result}")
            output.append({
                "enforcement_type": "error",
                "dsl_expression": "",
                "reasoning": f"Strategy determination failed: {str(result)}"
            })
        else:
            output.append(result)
    
    return output

