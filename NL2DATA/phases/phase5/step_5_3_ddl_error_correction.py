"""Phase 5, Step 5.3: DDL Error Correction.

If DDL validation fails, use LLM to analyze errors and generate IR patches.
Only invoked when Step 5.2 reports validation failures.
Outputs patches to LogicalIR, then DDL is regenerated deterministically.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase5.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class Patch(BaseModel):
    """A patch to apply to LogicalIR."""
    operation: str = Field(description="Operation type: 'rename', 'change_type', 'fix_constraint', 'add_column', 'remove_column'")
    target: str = Field(description="Target table or column (e.g., 'Customer.customer_id')")
    changes: Dict[str, Any] = Field(description="Changes to apply")
    reasoning: str = Field(description="Reasoning for this patch")


class DDLCorrection(BaseModel):
    """A single DDL correction."""
    original: str = Field(description="Original problematic DDL or schema element")
    corrected: str = Field(description="Corrected version")
    reasoning: str = Field(description="Explanation of the correction")


class DDLErrorCorrectionOutput(BaseModel):
    """Output structure for DDL error correction."""
    ir_patches: List[Patch] = Field(description="List of patches to apply to LogicalIR")
    corrections: List[DDLCorrection] = Field(description="List of corrections made")
    reasoning: str = Field(description="Overall reasoning for the corrections")


@traceable_step("5.3", phase=5, tags=['phase_5_step_3'])
async def step_5_3_ddl_error_correction(
    validation_errors: Dict[str, Any],  # Output from Step 5.2
    original_ddl: List[str],  # Original DDL statements
    normalized_schema: Dict[str, Any],  # Full normalized schema from Phase 4
    relational_schema: Optional[Dict[str, Any]] = None,  # Relational schema from Step 3.5
) -> Dict[str, Any]:
    """
    Step 5.3 (conditional, LLM): Correct DDL errors by generating IR patches.
    
    This step is only invoked when Step 5.2 reports validation failures.
    It analyzes errors and generates patches to LogicalIR (not direct DDL edits),
    maintaining single source of truth.
    
    Args:
        validation_errors: Validation result from Step 5.2 with syntax_errors, naming_conflicts
        original_ddl: Original DDL statements that failed validation
        normalized_schema: Full normalized schema from Phase 4
        relational_schema: Optional relational schema from Step 3.5 for context
        
    Returns:
        dict: Error correction result with ir_patches, corrections, reasoning
        
    Example:
        >>> errors = {"syntax_errors": [{"error": "syntax error", "statement": "..."}]}
        >>> result = await step_5_3_ddl_error_correction(errors, ["..."], {...})
        >>> len(result["ir_patches"]) > 0
        True
    """
    logger.info("Starting Step 5.3: DDL Error Correction (LLM, conditional)")
    
    # Build error context
    error_summary = []
    if validation_errors.get("syntax_errors"):
        for err in validation_errors["syntax_errors"]:
            error_summary.append(f"Syntax error: {err.get('error', 'Unknown')} in statement: {err.get('statement', '')[:100]}")
    
    if validation_errors.get("naming_conflicts"):
        for conflict in validation_errors["naming_conflicts"]:
            error_summary.append(f"Naming conflict: {conflict}")
    
    if not error_summary:
        logger.warning("No errors found in validation_errors, returning empty patches")
        return {
            "ir_patches": [],
            "corrections": [],
            "reasoning": "No errors to correct"
        }
    
    # Build comprehensive schema context
    schema_context = []
    schema_context.append(f"Normalized tables: {len(normalized_schema.get('normalized_tables', []))}")
    
    # Get model
    model = get_model_for_step("5.3")
    
    # Create prompt
    system_prompt = """You are a database schema expert. Your task is to analyze DDL validation errors and generate patches to fix the LogicalIR schema.

CRITICAL: You must output IR patches (changes to LogicalIR), NOT direct DDL edits. The DDL will be regenerated deterministically from the patched IR.

PATCH OPERATIONS:
1. **rename**: Rename a table or column (e.g., fix reserved keyword conflicts)
2. **change_type**: Change a column's data type (e.g., fix type mismatches)
3. **fix_constraint**: Fix constraint definitions (e.g., fix CHECK constraint syntax)
4. **add_column**: Add a missing column
5. **remove_column**: Remove an invalid column

For each patch:
- operation: Type of operation
- target: Table or column identifier (e.g., "Customer.customer_id")
- changes: Dictionary with specific changes (e.g., {"new_name": "customer_id_new"}, {"new_type": "INTEGER"})
- reasoning: Clear explanation of why this patch is needed

Return patches that will fix all validation errors when applied to LogicalIR."""
    
    human_prompt = f"""DDL Validation Errors:
{chr(10).join(error_summary)}

Original DDL Statements:
{chr(10).join(original_ddl[:3])}  # Show first 3 for context

Normalized Schema Summary:
- Tables: {len(normalized_schema.get('normalized_tables', []))}
- Schema structure available for reference

Analyze the errors and generate IR patches to fix them. Remember: output patches to LogicalIR, not direct DDL edits."""
    
    # Invoke standardized LLM call
    try:
        result: DDLErrorCorrectionOutput = await standardized_llm_call(
            llm=model,
            output_schema=DDLErrorCorrectionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
        )
        
        # Work with Pydantic model directly
        logger.info(f"DDL error correction completed: {len(result.ir_patches)} patches generated")
        
        # Convert to dict only at return boundary
        return {
            "ir_patches": [patch.model_dump() for patch in result.ir_patches],
            "corrections": [corr.model_dump() for corr in result.corrections],
            "reasoning": result.reasoning
        }
    except Exception as e:
        logger.error(f"DDL error correction failed: {e}")
        raise

