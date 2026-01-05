"""Phase 8, Step 8.4: Constraint Detection.

Detect constraints from natural language description and schema.
Excludes structural constraints (NOT NULL, NULL, referential integrity, unique) - those are already handled.
Focuses on statistical, distribution, and other business rule constraints.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.phases.phase8.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema

logger = get_logger(__name__)


def _sanitize_for_json(obj: Any, _visited: Optional[set] = None) -> Any:
    """
    Recursively sanitize objects to ensure they're JSON serializable.
    Converts type objects to strings.
    Uses _visited set to prevent infinite recursion on circular references.
    """
    if _visited is None:
        _visited = set()
    
    # Handle circular references
    obj_id = id(obj)
    if obj_id in _visited:
        return "<circular_reference>"
    _visited.add(obj_id)
    
    try:
        if isinstance(obj, type):
            # Convert Python type objects to their name
            return obj.__name__
        elif isinstance(obj, dict):
            return {k: _sanitize_for_json(v, _visited) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [_sanitize_for_json(item, _visited) for item in obj]
        elif isinstance(obj, set):
            # Convert sets to lists
            return [_sanitize_for_json(item, _visited) for item in obj]
        elif hasattr(obj, 'model_dump'):
            # Handle Pydantic BaseModel instances
            try:
                return _sanitize_for_json(obj.model_dump(), _visited)
            except:
                return str(obj)
        elif hasattr(obj, '__dict__'):
            # Handle objects with __dict__ (but not BaseModel instances which have model_dump)
            try:
                return _sanitize_for_json(obj.__dict__, _visited)
            except:
                return str(obj)
        elif obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        else:
            # For any other type, try to convert to string
            # But first check if it's a type-like object
            if hasattr(obj, '__class__') and obj.__class__ == type:
                return obj.__name__
            return str(obj)
    finally:
        _visited.discard(obj_id)


class DetectedConstraint(BaseModel):
    """A single detected constraint with substring justification."""
    constraint_id: str = Field(description="Unique identifier for this constraint")
    constraint_type: str = Field(description="Type: 'statistical', 'distribution', or 'other'")
    table: str = Field(description="Table name this constraint applies to")
    column: Optional[str] = Field(default=None, description="Column name (if column-specific, otherwise None)")
    description: str = Field(description="Natural language description of the constraint")
    justification_substring: str = Field(
        description="Exact substring(s) from the NL description that justify this constraint. Must be traceable."
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Optional reasoning for why this constraint was identified"
    )

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


class ConstraintDetectionOutput(BaseModel):
    """Output structure for constraint detection."""
    statistical_constraints: List[DetectedConstraint] = Field(
        default_factory=list,
        description="List of statistical constraints (ranges, min/max, averages, percentiles)"
    )
    distribution_constraints: List[DetectedConstraint] = Field(
        default_factory=list,
        description="List of distribution constraints (how data should be distributed)"
    )
    other_constraints: List[DetectedConstraint] = Field(
        default_factory=list,
        description="List of other business rule constraints"
    )
    no_more_changes: bool = Field(
        description="Whether the LLM suggests no further constraints should be added (termination condition)"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Reasoning for the constraints identified and termination decision"
    )

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


def _format_schema_for_llm(
    normalized_schema: Dict[str, Any],
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Format relational schema with FKs and FDs for LLM context."""
    schema_parts = []
    
    tables = normalized_schema.get("tables", []) or normalized_schema.get("normalized_tables", [])
    
    for table in tables:
        table_name = table.get("name", "")
        if not table_name:
            continue
        
        schema_parts.append(f"\nTable: {table_name}")
        
        # Columns
        columns = table.get("columns", [])
        if columns:
            schema_parts.append("  Columns:")
            for col in columns:
                col_name = col.get("name", "")
                col_type_raw = col.get("type", col.get("datatype", "UNKNOWN"))
                # Convert Python type objects to strings (e.g., str -> "str", int -> "int")
                if isinstance(col_type_raw, type):
                    col_type = col_type_raw.__name__
                else:
                    col_type = str(col_type_raw) if col_type_raw is not None else "UNKNOWN"
                col_desc = col.get("description", "")
                is_pk = col.get("is_primary_key", False)
                is_fk = col.get("is_foreign_key", False)
                is_nullable = col.get("is_nullable", True)
                
                col_info = f"    - {col_name}: {col_type}"
                if is_pk:
                    col_info += " (PRIMARY KEY)"
                if is_fk:
                    col_info += " (FOREIGN KEY)"
                if not is_nullable:
                    col_info += " (NOT NULL)"
                if col_desc:
                    col_info += f" - {col_desc}"
                schema_parts.append(col_info)
        
        # Primary Key
        pk = table.get("primary_key", [])
        if pk:
            schema_parts.append(f"  Primary Key: {', '.join(pk)}")
        
        # Foreign Keys
        fks = table.get("foreign_keys", [])
        if fks:
            schema_parts.append("  Foreign Keys:")
            for fk in fks:
                if isinstance(fk, dict):
                    fk_cols = fk.get("columns", [])
                    ref_table = fk.get("references_table", "")
                    ref_cols = fk.get("references_columns", [])
                    if fk_cols and ref_table:
                        schema_parts.append(f"    - {', '.join(fk_cols)} -> {ref_table}({', '.join(ref_cols) if isinstance(ref_cols, list) else ref_cols})")
                elif isinstance(fk, str):
                    schema_parts.append(f"    - {fk}")
    
    # Functional Dependencies
    if functional_dependencies:
        schema_parts.append("\nFunctional Dependencies:")
        for fd in functional_dependencies:
            if isinstance(fd, dict):
                lhs = fd.get("lhs", [])
                rhs = fd.get("rhs", [])
                reasoning = fd.get("reasoning", "")
                if lhs and rhs:
                    fd_str = f"  {', '.join(lhs)} -> {', '.join(rhs)}"
                    if reasoning:
                        fd_str += f" ({reasoning})"
                    schema_parts.append(fd_str)
            elif hasattr(fd, 'lhs') and hasattr(fd, 'rhs'):
                lhs = fd.lhs if isinstance(fd.lhs, list) else [fd.lhs]
                rhs = fd.rhs if isinstance(fd.rhs, list) else [fd.rhs]
                reasoning = getattr(fd, 'reasoning', '')
                if lhs and rhs:
                    fd_str = f"  {', '.join(lhs)} -> {', '.join(rhs)}"
                    if reasoning:
                        fd_str += f" ({reasoning})"
                    schema_parts.append(fd_str)
    
    return "\n".join(schema_parts)


def _format_categorical_values_for_llm(
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Format categorical values for LLM context."""
    if not categorical_values:
        return ""
    
    parts = ["\nCategorical Columns and Their Values:"]
    
    for entity_name, entity_cats in categorical_values.items():
        if not isinstance(entity_cats, dict):
            continue
        
        for attr_name, cat_output in entity_cats.items():
            if not isinstance(cat_output, dict):
                continue
            
            # Extract categorical values
            cat_values = cat_output.get("categorical_values", [])
            if not cat_values:
                continue
            
            value_list = []
            for cv in cat_values:
                if isinstance(cv, dict):
                    value_list.append(cv.get("value", ""))
                elif hasattr(cv, 'value'):
                    value_list.append(cv.value)
                else:
                    value_list.append(str(cv))
            
            if value_list:
                parts.append(f"  {entity_name}.{attr_name}: {', '.join(value_list)}")
    
    return "\n".join(parts) if len(parts) > 1 else ""


@traceable_step("8.4", phase=8, tags=['phase_8_step_4'])
async def step_8_4_constraint_detection_single(
    nl_description: str,
    normalized_schema: Dict[str, Any],
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
    derived_columns: Optional[set] = None,
    previous_constraints: Optional[List[Dict[str, Any]]] = None,
) -> ConstraintDetectionOutput:
    """
    Step 8.4 (single iteration): Detect constraints from NL description and schema.
    
    Args:
        nl_description: Natural language description
        normalized_schema: Relational schema with tables, columns, FKs
        categorical_values: Optional dictionary mapping entity_name -> attribute_name -> CategoricalValueIdentificationOutput
        functional_dependencies: Optional list of functional dependencies from step 8.1
        derived_columns: Optional set of derived column identifiers (format: "table.column") to exclude
        previous_constraints: Optional list of previously identified constraints (for loop)
        
    Returns:
        ConstraintDetectionOutput with detected constraints
    """
    derived_columns = derived_columns or set()
    
    # Sanitize all inputs to remove any type objects that can't be JSON serialized
    sanitized_schema = _sanitize_for_json(normalized_schema)
    sanitized_functional_dependencies = _sanitize_for_json(functional_dependencies) if functional_dependencies else None
    sanitized_categorical_values = _sanitize_for_json(categorical_values) if categorical_values else None
    
    # Format schema for LLM (use sanitized versions)
    schema_text = _format_schema_for_llm(sanitized_schema, sanitized_functional_dependencies)
    
    # Format categorical values (use sanitized version)
    categorical_text = _format_categorical_values_for_llm(sanitized_categorical_values)
    
    # Format previous constraints for context
    previous_constraints_text = ""
    if previous_constraints:
        previous_constraints_text = "\n\nPreviously Identified Constraints:\n"
        for i, prev_const in enumerate(previous_constraints, 1):
            const_type = prev_const.get("constraint_type", "unknown")
            table = prev_const.get("table", "")
            column = prev_const.get("column", "")
            desc = prev_const.get("description", "")
            prev_const_str = f"{i}. [{const_type}] {table}"
            if column:
                prev_const_str += f".{column}"
            prev_const_str += f": {desc}"
            previous_constraints_text += prev_const_str + "\n"
    
    # Build excluded columns text
    excluded_text = ""
    if derived_columns:
        excluded_text = f"\n\nIMPORTANT: Exclude the following derived columns from constraint detection: {', '.join(sorted(derived_columns))}"
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=ConstraintDetectionOutput,
        additional_requirements=[
            "Every constraint MUST include a 'justification_substring' field with the exact substring(s) from the NL description that justify it.",
            "Do NOT include structural constraints (NOT NULL, NULL, referential integrity, unique) - those are already handled.",
            "Focus on statistical constraints (ranges, min/max, averages), distribution constraints, and other business rules.",
            "For categorical columns, mention the specific categorical values in the constraint description.",
            "The 'no_more_changes' field indicates whether you believe all constraints have been identified.",
        ]
    )
    
    # Build prompt
    system_prompt = f"""You are a database constraint analyst. Your task is to identify constraints from natural language descriptions.

CRITICAL REQUIREMENTS:
1. Every constraint MUST be backed by an actual substring from the NL description. No speculative constraints.
2. The 'justification_substring' field must contain the exact substring(s) from the NL that justify the constraint.
3. Do NOT include structural constraints (NOT NULL, NULL, referential integrity, unique) - those are already defined.
4. Focus on:
   - Statistical constraints: ranges (e.g., "amount must be between 0 and 1000"), min/max values, averages, percentiles
   - Distribution constraints: how data should be distributed (e.g., "80% active, 20% inactive")
   - Other business rules: any other constraints mentioned in the NL
5. For categorical columns, use the specific categorical values provided in the constraint description.
6. Exclude derived columns from constraint detection.

{output_structure_section}"""

    human_prompt = f"""Analyze the following natural language description and schema to identify constraints:

Natural Language Description:
{nl_description}

Relational Schema:
{schema_text}
{categorical_text}
{previous_constraints_text}
{excluded_text}

Identify all constraints that are:
- Statistical (ranges, min/max, averages, percentiles)
- Distribution-related (how data should be distributed)
- Other business rules

For each constraint, provide:
1. The constraint description in natural language
2. The exact substring(s) from the NL that justify it (justification_substring)
3. The table and column it applies to
4. The constraint type

Remember: Every constraint must be traceable to the NL description. No BS constraints."""

    # Get model
    llm = get_model_for_step("8.4")
    
    # Make LLM call
    trace_config = get_trace_config("8.4", phase=8, tags=["phase_8_step_4"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=ConstraintDetectionOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,  # Keep original config (RunnableConfig should be fine)
    )
    
    return result


async def step_8_4_constraint_detection_with_loop(
    nl_description: str,
    normalized_schema: Dict[str, Any],
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
    functional_dependencies: Optional[List[Dict[str, Any]]] = None,
    derived_formulas: Optional[Dict[str, Any]] = None,
    multivalued_derived: Optional[Dict[str, Any]] = None,
    max_iterations: int = 10,
    max_time_sec: int = 300,
) -> Dict[str, Any]:
    """
    Step 8.4 (loop, LLM): Detect constraints from NL description and schema.
    
    IMPORTANT: Derived columns (from step 2.9) are excluded from constraint detection.
    
    Args:
        nl_description: Natural language description
        normalized_schema: Relational schema with tables, columns, FKs
        categorical_values: Optional dictionary mapping entity_name -> attribute_name -> CategoricalValueIdentificationOutput
                           Contains the identified categorical values/labels for categorical columns (from step 8.3)
        functional_dependencies: Optional list of functional dependencies from step 8.1
        derived_formulas: Optional dictionary from step 2.9 (keys are "entity.attribute")
        multivalued_derived: Optional dictionary from step 2.8 (entity_name -> {derived: [attr_names]})
        max_iterations: Maximum loop iterations
        max_time_sec: Maximum time in seconds
        
    Returns:
        dict: Constraint detection result with categories:
            - statistical_constraints
            - distribution_constraints
            - other_constraints
            - final_result (ConstraintDetectionOutput)
    """
    # Sanitize inputs to remove any type objects that can't be JSON serialized
    sanitized_normalized_schema = _sanitize_for_json(normalized_schema)
    sanitized_categorical_values = _sanitize_for_json(categorical_values) if categorical_values else None
    sanitized_functional_dependencies = _sanitize_for_json(functional_dependencies) if functional_dependencies else None
    
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
        logger.info(f"Excluding {len(derived_columns)} derived columns from constraint detection")
    
    previous_constraints = None
    
    async def constraint_detection_step(previous_result=None):
        """Single iteration of constraint detection."""
        nonlocal previous_constraints
        
        if previous_result:
            # Extract constraints from previous result
            if hasattr(previous_result, 'statistical_constraints'):
                prev_stats = [c.model_dump() if hasattr(c, 'model_dump') else c for c in previous_result.statistical_constraints]
                prev_dist = [c.model_dump() if hasattr(c, 'model_dump') else c for c in previous_result.distribution_constraints]
                prev_other = [c.model_dump() if hasattr(c, 'model_dump') else c for c in previous_result.other_constraints]
                previous_constraints = prev_stats + prev_dist + prev_other
            elif isinstance(previous_result, dict):
                prev_stats = previous_result.get("statistical_constraints", [])
                prev_dist = previous_result.get("distribution_constraints", [])
                prev_other = previous_result.get("other_constraints", [])
                previous_constraints = prev_stats + prev_dist + prev_other
        
        result = await step_8_4_constraint_detection_single(
            nl_description=nl_description,
            normalized_schema=sanitized_normalized_schema,
            categorical_values=sanitized_categorical_values,
            functional_dependencies=sanitized_functional_dependencies,
            derived_columns=derived_columns,
            previous_constraints=previous_constraints,
        )
        return result
    
    # Termination check: no_more_changes must be True
    def termination_check(result) -> bool:
        if hasattr(result, 'no_more_changes'):
            return result.no_more_changes
        return result.get("no_more_changes", False)
    
    # Configure loop
    loop_config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
        oscillation_window=3,
        enable_cycle_detection=True,
    )
    
    # Execute loop
    executor = SafeLoopExecutor()
    loop_result = await executor.run_loop(
        step_func=constraint_detection_step,
        termination_check=termination_check,
        config=loop_config,
    )
    
    final_result = loop_result["result"]
    iterations = loop_result["iterations"]
    terminated_by = loop_result["terminated_by"]
    
    logger.info(
        f"Step 8.4 constraint detection loop completed: {iterations} iterations, "
        f"terminated by {terminated_by}, no_more_changes={final_result.no_more_changes if hasattr(final_result, 'no_more_changes') else 'N/A'}"
    )
    
    # Convert to dict format for backward compatibility
    if hasattr(final_result, 'model_dump'):
        result_dict = final_result.model_dump()
    else:
        result_dict = final_result if isinstance(final_result, dict) else {}
    
    # Flatten constraints for backward compatibility
    all_constraints = []
    for cat in ["statistical_constraints", "distribution_constraints", "other_constraints"]:
        constraints = result_dict.get(cat, [])
        for constraint in constraints:
            if isinstance(constraint, dict):
                constraint["constraint_category"] = cat.replace("_constraints", "")
            all_constraints.append(constraint)
    
    return {
        "statistical_constraints": result_dict.get("statistical_constraints", []),
        "distribution_constraints": result_dict.get("distribution_constraints", []),
        "other_constraints": result_dict.get("other_constraints", []),
        "constraints": all_constraints,  # Flattened list for backward compatibility
        "final_result": result_dict,
        "loop_metadata": {
            "iterations": iterations,
            "terminated_by": terminated_by,
        }
    }
