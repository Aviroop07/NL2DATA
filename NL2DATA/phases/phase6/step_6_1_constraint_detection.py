"""Phase 6, Step 6.1: Constraint Detection.

Extract all constraints mentioned in the description (statistical, structural, distributional).
Iterative loop continues until LLM suggests no more additions or deletions.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase6.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig

logger = get_logger(__name__)


class ConstraintDetectionOutput(BaseModel):
    """Output structure for constraint detection."""
    statistical_constraints: List[str] = Field(
        description="List of statistical constraints (e.g., 'fraud majority: confirmed_fraud ~ 5%')"
    )
    structural_constraints: List[str] = Field(
        description="List of structural constraints (e.g., 'amount > 0', 'age >= 18')"
    )
    distribution_constraints: List[str] = Field(
        description="List of distribution constraints (e.g., 'amount follows log-normal', 'seasonal patterns')"
    )
    other_constraints: List[str] = Field(
        default_factory=list,
        description="List of other constraints that don't fit the above categories"
    )
    additions: List[str] = Field(
        default_factory=list,
        description="List of newly added constraint descriptions (for tracking changes)"
    )
    deletions: List[str] = Field(
        default_factory=list,
        description="List of constraint descriptions that should be removed (for tracking changes)"
    )
    no_more_changes: bool = Field(
        description="Whether the LLM suggests no more additions or deletions (termination condition for loop)"
    )
    reasoning: Optional[Dict[str, str]] = Field(
        default=None,
        description="Reasoning for each constraint category and the termination decision"
    )


@traceable_step("6.1", phase=6, tags=['phase_6_step_1'])
async def step_6_1_constraint_detection(
    nl_description: str,
    normalized_schema: Dict[str, Any],  # Full normalized schema from Phase 4
    previous_constraints: Optional[Dict[str, List[str]]] = None,  # Previous constraints from loop iterations
) -> Dict[str, Any]:
    """
    Step 6.1 (loop, LLM): Extract all constraints mentioned in the description.
    
    This step iteratively identifies constraints until no more additions or deletions
    are suggested. Designed to be called in a loop until no_more_changes is True.
    
    Args:
        nl_description: Original natural language description
        normalized_schema: Full normalized schema from Phase 4
        previous_constraints: Optional previous constraints from loop iterations
        
    Returns:
        dict: Constraint detection result with all constraint categories, additions, deletions, no_more_changes
        
    Example:
        >>> result = await step_6_1_constraint_detection("Amounts must be positive", {...})
        >>> len(result["structural_constraints"]) > 0
        True
    """
    logger.debug("Detecting constraints from description")
    
    # Build schema summary
    schema_summary = []
    tables = normalized_schema.get("normalized_tables", [])
    for table in tables[:10]:  # Limit for context
        table_name = table.get("name", "")
        columns = [col.get("name", "") for col in table.get("columns", [])[:10]]
        schema_summary.append(f"Table {table_name}: {columns}")
    
    # Build previous constraints context
    prev_context = ""
    if previous_constraints:
        prev_context = "\n\nPrevious constraints identified:\n"
        for category, constraints in previous_constraints.items():
            if constraints:
                prev_context += f"{category}: {', '.join(constraints[:5])}\n"  # Show first 5
    
    # Get model
    model = get_model_for_step("6.1")
    
    # Create prompt
    system_prompt = """You are a database constraint analysis expert. Your task is to identify all constraints mentioned in the description.

CONSTRAINT CATEGORIES:
1. **Statistical Constraints**: Constraints about distributions, percentages, frequencies
   - Examples: "fraud majority: confirmed_fraud ~ 5%", "pay-day spikes around 1st and 15th"
2. **Structural Constraints**: Constraints about value ranges, relationships, data integrity
   - Examples: "amount > 0", "age >= 18", "order_date must be after customer registration_date"
3. **Distribution Constraints**: Constraints about data distributions and patterns
   - Examples: "amount follows log-normal", "weekly seasonality", "heavy tail distribution"
4. **Other Constraints**: Any constraints that don't fit the above categories

ITERATIVE REFINEMENT:
- Review previously identified constraints
- Add new constraints that were missed
- Remove constraints that are incorrect or redundant
- Continue until you're confident all important constraints are identified

Return a comprehensive list of all constraints, categorized appropriately."""
    
    human_prompt = f"""Natural Language Description:
{nl_description}

Schema Summary:
{chr(10).join(schema_summary)}
{prev_context}

Identify all constraints mentioned in the description. Categorize them appropriately and indicate if there are any additions or deletions from previous iterations."""
    
    # Invoke standardized LLM call
    try:
        result: ConstraintDetectionOutput = await standardized_llm_call(
            llm=model,
            output_schema=ConstraintDetectionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
        )
        
        # Work with Pydantic model directly
        logger.debug(
            f"Constraint detection completed: {len(result.statistical_constraints)} statistical, "
            f"{len(result.structural_constraints)} structural, {len(result.distribution_constraints)} distribution"
        )
        
        # Convert to dict only at return boundary
        return {
            "statistical_constraints": result.statistical_constraints,
            "structural_constraints": result.structural_constraints,
            "distribution_constraints": result.distribution_constraints,
            "other_constraints": result.other_constraints,
            "additions": result.additions,
            "deletions": result.deletions,
            "no_more_changes": result.no_more_changes,
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"Constraint detection failed: {e}")
        raise


async def step_6_1_constraint_detection_with_loop(
    nl_description: str,
    normalized_schema: Dict[str, Any],
    max_iterations: int = 10,
    max_time_sec: int = 300,
) -> Dict[str, Any]:
    """
    Run constraint detection in a loop until no more changes.
    
    Args:
        nl_description: Original natural language description
        normalized_schema: Full normalized schema from Phase 4
        max_iterations: Maximum number of loop iterations
        max_time_sec: Maximum time in seconds
        
    Returns:
        Final constraint detection result after loop completes
    """
    config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
        enable_cycle_detection=True,
    )
    
    executor = SafeLoopExecutor()
    
    previous_constraints = None
    
    def termination_check(result: Dict[str, Any]) -> bool:
        return result.get("no_more_changes", False)
    
    async def step_func(previous_result=None, **kwargs):
        nonlocal previous_constraints
        result = await step_6_1_constraint_detection(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            previous_constraints=previous_constraints,
        )
        # Update previous_constraints for next iteration
        previous_constraints = {
            "statistical_constraints": result.get("statistical_constraints", []),
            "structural_constraints": result.get("structural_constraints", []),
            "distribution_constraints": result.get("distribution_constraints", []),
            "other_constraints": result.get("other_constraints", []),
        }
        return result
    
    result = await executor.run_loop(
        step_func=step_func,
        termination_check=termination_check,
        config=config,
    )
    
    # Handle case where loop timed out with 0 iterations (result["result"] is None)
    final_result = result.get("result")
    if final_result is None:
        logger.warning(
            f"Loop terminated with no results (iterations={result.get('iterations', 0)}, "
            f"terminated_by={result.get('terminated_by', 'unknown')}). "
            f"Returning empty constraint structure."
        )
        # Return empty constraint structure
        return {
            "statistical_constraints": [],
            "structural_constraints": [],
            "distribution_constraints": [],
            "other_constraints": [],
            "additions": [],
            "deletions": [],
            "no_more_changes": True,  # Set to True to indicate we're done (even if not ideal)
            "reasoning": {
                "note": f"Loop terminated early: {result.get('terminated_by', 'unknown')} "
                        f"after {result.get('iterations', 0)} iterations"
            }
        }
    
    return final_result

