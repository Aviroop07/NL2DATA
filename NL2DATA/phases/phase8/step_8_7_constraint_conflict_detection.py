"""Phase 8, Step 8.7: Constraint Conflict Detection.

Deterministic check for constraint conflicts:
1. Each column should only have one constraint
2. Categorical constraints must match the categorical value list
3. Derived columns are excluded from conflict checking

If conflicts are detected, LLM is called to resolve them by either:
- Removing one of the conflicting constraints, OR
- Reconciling the constraints
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from pydantic import BaseModel, Field, ConfigDict
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase8.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema

logger = get_logger(__name__)


class ConstraintConflict(BaseModel):
    """A conflict between constraints."""
    constraint1_id: str = Field(description="ID or identifier of first constraint")
    constraint2_id: str = Field(description="ID or identifier of second constraint")
    conflict_type: str = Field(description="Type of conflict: 'multiple_constraints', 'categorical_mismatch', etc.")
    description: str = Field(description="Description of the conflict")
    affected_column: str = Field(description="Column affected by the conflict (format: table.column)")

    model_config = ConfigDict(extra="forbid")


class ResolvedConstraint(BaseModel):
    """A resolved constraint with validated structure."""
    constraint_id: Optional[str] = Field(
        default=None,
        description="Unique identifier for the constraint"
    )
    constraint_type: str = Field(description="Type of constraint (e.g., 'range', 'categorical', 'not_null', 'foreign_key')")
    table: str = Field(description="Table name the constraint applies to")
    column: str = Field(description="Column name the constraint applies to")
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the constraint"
    )
    affected_attributes: Optional[List[str]] = Field(
        default=None,
        description="List of affected attributes (format: 'table.column' or 'column')"
    )
    affected_tables: Optional[List[str]] = Field(
        default=None,
        description="List of affected table names"
    )
    
    # Constraint-specific fields (optional, depending on constraint_type)
    min_value: Optional[Any] = Field(default=None, description="Minimum value (for range constraints)")
    max_value: Optional[Any] = Field(default=None, description="Maximum value (for range constraints)")
    allowed_values: Optional[List[str]] = Field(default=None, description="Allowed values (for categorical constraints)")
    references: Optional[str] = Field(default=None, description="Referenced table.column (for foreign key constraints)")
    not_null: Optional[bool] = Field(default=None, description="Whether column is NOT NULL")
    default_value: Optional[Any] = Field(default=None, description="Default value")
    dsl_expression: Optional[str] = Field(default=None, description="DSL expression for the constraint")
    
    model_config = ConfigDict(
        extra="allow",  # Allow extra fields for constraint-specific properties
        json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema}
    )


class ConstraintResolution(BaseModel):
    """Resolution for a single conflict."""
    conflict_id: str = Field(description="Identifier for the conflict being resolved")
    resolution_type: str = Field(description="Type of resolution: 'remove_constraint1', 'remove_constraint2', 'reconcile', or 'keep_both'")
    constraint_to_remove: Optional[str] = Field(
        default=None,
        description="ID of constraint to remove (if resolution_type is 'remove_constraint1' or 'remove_constraint2')"
    )
    reconciled_constraint: Optional[ResolvedConstraint] = Field(
        default=None,
        description="Reconciled constraint (if resolution_type is 'reconcile')"
    )
    reasoning: str = Field(description="Reasoning for the resolution decision")

    model_config = ConfigDict(extra="forbid")


class ConstraintConflictResolutionOutput(BaseModel):
    """Output structure for conflict resolution."""
    resolutions: List[ConstraintResolution] = Field(
        default_factory=list,
        description="List of conflict resolutions"
    )
    resolved_constraints: List[ResolvedConstraint] = Field(
        default_factory=list,
        description="List of constraints after resolution (conflicts removed or reconciled)"
    )

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema}
    )


class ConstraintConflictDetectionOutput(BaseModel):
    """Output structure for constraint conflict detection."""
    conflicts: List[ConstraintConflict] = Field(
        default_factory=list,
        description="List of detected conflicts"
    )
    conflict_count: int = Field(
        default=0,
        description="Total number of conflicts detected"
    )
    resolved_constraints: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="List of constraints after conflict resolution (if conflicts were resolved)"
    )
    resolution_applied: bool = Field(
        default=False,
        description="Whether conflict resolution was applied"
    )

    model_config = ConfigDict(extra="forbid")


def _get_derived_columns(
    derived_formulas: Optional[Dict[str, Any]] = None,
    multivalued_derived: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    """
    Get set of derived column keys in format "entity.attribute" or "table.column".
    
    Args:
        derived_formulas: Dictionary from step 2.9 (keys are "entity.attribute")
        multivalued_derived: Dictionary from step 2.8 (entity_name -> {derived: [attr_names]})
        
    Returns:
        Set of column keys in format "entity.attribute"
    """
    derived_columns = set()
    
    # From Phase 2.9 (derived formulas)
    if derived_formulas:
        for key in derived_formulas.keys():
            if isinstance(key, str) and "." in key:
                derived_columns.add(key)
    
    # From Phase 2.8 (multivalued/derived detection)
    if multivalued_derived:
        for entity_name, mv_result in multivalued_derived.items():
            if isinstance(mv_result, dict):
                derived_attrs = mv_result.get("derived", [])
                for attr_name in derived_attrs:
                    if isinstance(attr_name, str):
                        derived_columns.add(f"{entity_name}.{attr_name}")
    
    return derived_columns


def _get_column_key(constraint: Dict[str, Any]) -> Optional[str]:
    """
    Extract column key from constraint in format "table.column".
    
    Args:
        constraint: Constraint dictionary
        
    Returns:
        Column key in format "table.column" or None if not available
    """
    table = constraint.get("table", "")
    column = constraint.get("column", "")
    
    if table and column:
        return f"{table}.{column}"
    
    # Try to get from affected_attributes
    affected_attrs = constraint.get("affected_attributes", [])
    if affected_attrs and isinstance(affected_attrs, list) and len(affected_attrs) > 0:
        # Take first affected attribute
        attr = affected_attrs[0]
        if isinstance(attr, str):
            if "." in attr:
                return attr  # Already in "table.column" format
            elif table:
                return f"{table}.{attr}"
    
    return None


def _get_categorical_allowed_values(
    entity_name: str,
    attribute_name: str,
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[List[str]]:
    """
    Get allowed categorical values for a column.
    
    Args:
        entity_name: Entity/table name
        attribute_name: Attribute/column name
        categorical_values: Dictionary from step 8.3 (entity_name -> attribute_name -> CategoricalValueIdentificationOutput)
        
    Returns:
        List of allowed values as strings, or None if not categorical
    """
    if not categorical_values:
        return None
    
    entity_cat_values = categorical_values.get(entity_name, {})
    if not entity_cat_values:
        return None
    
    attr_cat_values = entity_cat_values.get(attribute_name, {})
    if not attr_cat_values:
        return None
    
    # Extract values from CategoricalValueIdentificationOutput
    if hasattr(attr_cat_values, 'categorical_values'):
        cat_values_list = attr_cat_values.categorical_values
    elif isinstance(attr_cat_values, dict):
        cat_values_list = attr_cat_values.get("categorical_values", [])
    else:
        return None
    
    # Extract value strings
    allowed_values = []
    for cv in cat_values_list:
        if hasattr(cv, 'value'):
            allowed_values.append(str(cv.value))
        elif isinstance(cv, dict):
            val = cv.get("value")
            if val is not None:
                allowed_values.append(str(val))
        elif isinstance(cv, str):
            allowed_values.append(cv)
    
    return allowed_values if allowed_values else None


async def step_8_7_constraint_conflict_detection(
    constraints: List[Dict[str, Any]],
    categorical_values: Optional[Dict[str, Dict[str, Any]]] = None,
    derived_formulas: Optional[Dict[str, Any]] = None,
    multivalued_derived: Optional[Dict[str, Any]] = None,
) -> ConstraintConflictDetectionOutput:
    """
    Step 8.7 (deterministic): Detect conflicts between constraints.
    
    Checks:
    1. Each column should only have one constraint
    2. Categorical constraints must match the categorical value list
    3. Derived columns are excluded from conflict checking
    
    Args:
        constraints: List of constraint dictionaries
        categorical_values: Optional dictionary from step 8.3 (entity_name -> attribute_name -> CategoricalValueIdentificationOutput)
        derived_formulas: Optional dictionary from step 2.9 (keys are "entity.attribute")
        multivalued_derived: Optional dictionary from step 2.8 (entity_name -> {derived: [attr_names]})
        
    Returns:
        ConstraintConflictDetectionOutput with detected conflicts
    """
    logger.info("Starting Step 8.7: Constraint Conflict Detection (deterministic)")
    
    conflicts = []
    
    if not constraints:
        logger.info("No constraints provided, no conflicts to detect")
        return ConstraintConflictDetectionOutput(
            conflicts=[],
            conflict_count=0,
        )
    
    # Get derived columns to exclude
    derived_columns = _get_derived_columns(derived_formulas, multivalued_derived)
    logger.debug(f"Excluding {len(derived_columns)} derived columns from conflict detection")
    
    # Track constraints per column
    column_constraints: Dict[str, List[Dict[str, Any]]] = {}
    
    # First pass: Group constraints by column
    for i, constraint in enumerate(constraints):
        column_key = _get_column_key(constraint)
        
        if not column_key:
            logger.warning(f"Constraint {i} does not have identifiable table.column, skipping conflict check")
            continue
        
        # Skip derived columns
        if column_key in derived_columns:
            logger.debug(f"Skipping conflict check for derived column: {column_key}")
            continue
        
        if column_key not in column_constraints:
            column_constraints[column_key] = []
        column_constraints[column_key].append({
            "constraint": constraint,
            "index": i,
        })
    
    # Second pass: Check for multiple constraints on same column
    for column_key, constraint_list in column_constraints.items():
        if len(constraint_list) > 1:
            # Multiple constraints on same column - conflict!
            for i in range(len(constraint_list)):
                for j in range(i + 1, len(constraint_list)):
                    constraint1_info = constraint_list[i]
                    constraint2_info = constraint_list[j]
                    constraint1 = constraint1_info["constraint"]
                    constraint2 = constraint2_info["constraint"]
                    
                    constraint1_id = constraint1.get("constraint_id", f"constraint_{constraint1_info['index']}")
                    constraint2_id = constraint2.get("constraint_id", f"constraint_{constraint2_info['index']}")
                    
                    conflicts.append(ConstraintConflict(
                        constraint1_id=constraint1_id,
                        constraint2_id=constraint2_id,
                        conflict_type="multiple_constraints",
                        description=f"Column {column_key} has multiple constraints (should only have one). Constraint 1: {constraint1.get('constraint_type', 'unknown')}, Constraint 2: {constraint2.get('constraint_type', 'unknown')}",
                        affected_column=column_key,
                    ))
                    logger.warning(f"Conflict detected: Multiple constraints on column {column_key}")
    
    # Third pass: Check categorical constraints match allowed values
    for column_key, constraint_list in column_constraints.items():
        # Parse entity and attribute from column_key
        if "." not in column_key:
            continue
        
        entity_name, attribute_name = column_key.split(".", 1)
        
        # Get allowed categorical values for this column
        allowed_values = _get_categorical_allowed_values(
            entity_name,
            attribute_name,
            categorical_values,
        )
        
        if allowed_values is None:
            # Not a categorical column, skip
            continue
        
        # Check each constraint on this column
        for constraint_info in constraint_list:
            constraint = constraint_info["constraint"]
            constraint_type = constraint.get("constraint_type", "")
            
            # Check if this is a categorical constraint
            if constraint_type == "categorical" or "categorical" in constraint_type.lower():
                constraint_allowed_values = constraint.get("allowed_values", [])
                
                if not constraint_allowed_values:
                    # Categorical constraint without allowed_values - might be an issue but not a conflict
                    continue
                
                # Check if constraint values are subset of categorical values
                constraint_values_set = set(str(v).lower() for v in constraint_allowed_values)
                allowed_values_set = set(str(v).lower() for v in allowed_values)
                
                # Check if any constraint value is not in allowed values
                mismatched_values = constraint_values_set - allowed_values_set
                
                if mismatched_values:
                    constraint_id = constraint.get("constraint_id", f"constraint_{constraint_info['index']}")
                    conflicts.append(ConstraintConflict(
                        constraint1_id=constraint_id,
                        constraint2_id="categorical_value_list",
                        conflict_type="categorical_mismatch",
                        description=f"Categorical constraint on {column_key} includes values not in categorical value list: {sorted(mismatched_values)}. Allowed values: {sorted(allowed_values_set)}",
                        affected_column=column_key,
                    ))
                    logger.warning(f"Conflict detected: Categorical constraint on {column_key} has values not in categorical value list: {mismatched_values}")
    
    conflict_count = len(conflicts)
    logger.info(f"Step 8.7: Detected {conflict_count} constraint conflicts")
    
    # If conflicts exist, resolve them using LLM
    resolved_constraints = None
    resolution_applied = False
    
    if conflict_count > 0:
        logger.info(f"Resolving {conflict_count} constraint conflicts using LLM")
        resolved_constraints, resolution_applied = await _resolve_conflicts_with_llm(
            constraints=constraints,
            conflicts=conflicts,
        )
    
    logger.info(f"Step 8.7 completed: Detected {conflict_count} conflicts, resolution applied: {resolution_applied}")
    
    return ConstraintConflictDetectionOutput(
        conflicts=conflicts,
        conflict_count=conflict_count,
        resolved_constraints=resolved_constraints,
        resolution_applied=resolution_applied,
    )


async def _resolve_conflicts_with_llm(
    constraints: List[Dict[str, Any]],
    conflicts: List[ConstraintConflict],
    ) -> Tuple[Optional[List[Dict[str, Any]]], bool]:
    """
    Resolve constraint conflicts using LLM.
    
    Args:
        constraints: Original list of constraints
        conflicts: List of detected conflicts
        
    Returns:
        Tuple of (resolved_constraints, resolution_applied)
        - resolved_constraints: List of constraints after resolution, or None if resolution failed
        - resolution_applied: Whether resolution was successfully applied
    """
    try:
        # Build conflict summary for LLM
        conflict_summaries = []
        for i, conflict in enumerate(conflicts):
            conflict_summaries.append(
                f"Conflict {i+1}:\n"
                f"  Type: {conflict.conflict_type}\n"
                f"  Affected Column: {conflict.affected_column}\n"
                f"  Constraint 1 ID: {conflict.constraint1_id}\n"
                f"  Constraint 2 ID: {conflict.constraint2_id}\n"
                f"  Description: {conflict.description}"
            )
        
        # Find the actual constraint objects for each conflict
        constraint_map = {}
        for i, constraint in enumerate(constraints):
            constraint_id = constraint.get("constraint_id", f"constraint_{i}")
            constraint_map[constraint_id] = constraint
            constraint_map[f"constraint_{i}"] = constraint  # Also map by index
        
        # Build constraint details for conflicts
        conflict_details = []
        for conflict in conflicts:
            constraint1 = constraint_map.get(conflict.constraint1_id, {})
            constraint2 = constraint_map.get(conflict.constraint2_id, {})
            
            conflict_details.append({
                "conflict": conflict,
                "constraint1": constraint1,
                "constraint2": constraint2,
            })
        
        # Generate output structure section
        output_structure_section = generate_output_structure_section_with_custom_requirements(
            output_schema=ConstraintConflictResolutionOutput,
            additional_requirements=[
                "For each conflict, provide a resolution that either removes one constraint or reconciles them",
                "If removing a constraint, set resolution_type to 'remove_constraint1' or 'remove_constraint2' and specify constraint_to_remove",
                "If reconciling, set resolution_type to 'reconcile' and provide a reconciled_constraint (ResolvedConstraint model) that combines both constraints appropriately",
                "The resolved_constraints list should contain all constraints after applying resolutions (removed constraints excluded, reconciled constraints included)",
                "Each constraint in resolved_constraints must be a ResolvedConstraint model with at least: constraint_type, table, column",
                "Include all relevant constraint fields (min_value, max_value, allowed_values, etc.) based on constraint_type"
            ]
        )
        
        # Build prompt
        system_prompt = f"""You are a database constraint conflict resolver. Your task is to resolve conflicts between constraints.

When constraints conflict, you have two options:
1. **Remove one constraint**: If one constraint is redundant, incorrect, or less important, remove it
2. **Reconcile constraints**: If both constraints are valid but conflicting, create a new reconciled constraint that satisfies both

**Resolution Types:**
- 'remove_constraint1': Remove the first constraint (constraint1_id)
- 'remove_constraint2': Remove the second constraint (constraint2_id)
- 'reconcile': Create a new reconciled constraint that combines both
- 'keep_both': Keep both constraints (only if they can coexist)

**Guidelines:**
- For multiple constraints on same column: Usually remove the less specific or redundant one
- For categorical mismatches: Update the constraint to match the categorical value list, or remove it if it's incorrect
- Prioritize constraints that are more specific or important
- When reconciling, ensure the new constraint is valid and doesn't conflict with others
- All constraints in resolved_constraints must be valid ResolvedConstraint objects with required fields: constraint_type, table, column

**ResolvedConstraint Model Requirements:**
- constraint_type (required): Type of constraint (e.g., 'range', 'categorical', 'not_null', 'foreign_key')
- table (required): Table name
- column (required): Column name
- constraint_id (optional): Unique identifier
- description (optional): Human-readable description
- For range constraints: include min_value and/or max_value
- For categorical constraints: include allowed_values list
- For foreign key constraints: include references field
- Include any other relevant fields from the original constraints

{output_structure_section}"""
        
        # Build human prompt with constraint details
        constraints_text = []
        for i, constraint in enumerate(constraints):
            constraint_id = constraint.get("constraint_id", f"constraint_{i}")
            constraint_type = constraint.get("constraint_type", "unknown")
            table = constraint.get("table", "")
            column = constraint.get("column", "")
            description = constraint.get("description", "")
            
            constraints_text.append(
                f"Constraint {constraint_id}:\n"
                f"  Type: {constraint_type}\n"
                f"  Table.Column: {table}.{column}\n"
                f"  Description: {description}\n"
                f"  Full constraint: {constraint}"
            )
        
        human_prompt = f"""Resolve the following constraint conflicts:

**Detected Conflicts:**
{chr(10).join(conflict_summaries)}

**All Constraints:**
{chr(10).join(constraints_text)}

Provide resolutions for each conflict. The resolved_constraints list should contain the final set of constraints after applying all resolutions."""
        
        # Get model
        llm = get_model_for_step("8.7")
        
        # Make LLM call
        trace_config = get_trace_config("8.7", phase=8, tags=["phase_8_step_7", "conflict_resolution"])
        
        result = await standardized_llm_call(
            llm=llm,
            output_schema=ConstraintConflictResolutionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},
        )
        
        # Extract resolved constraints and convert to dict format
        if hasattr(result, 'resolved_constraints'):
            resolved_pydantic = result.resolved_constraints
        elif isinstance(result, dict):
            resolved_pydantic = result.get("resolved_constraints", [])
        else:
            resolved_pydantic = []
        
        # Convert Pydantic models to dictionaries for compatibility
        resolved = []
        for constraint_model in resolved_pydantic:
            if hasattr(constraint_model, 'model_dump'):
                resolved.append(constraint_model.model_dump(exclude_none=False))
            elif isinstance(constraint_model, dict):
                resolved.append(constraint_model)
            else:
                # Try to convert to dict
                try:
                    resolved.append(dict(constraint_model))
                except Exception:
                    logger.warning(f"Could not convert constraint to dict: {constraint_model}")
        
        if resolved:
            logger.info(f"LLM resolved conflicts: {len(resolved)} constraints after resolution (from {len(constraints)} original)")
            return resolved, True
        else:
            logger.warning("LLM conflict resolution returned empty constraints list, keeping original constraints")
            return constraints, False
            
    except Exception as e:
        logger.error(f"Error during LLM conflict resolution: {e}", exc_info=True)
        logger.warning("Falling back to original constraints due to resolution error")
        return constraints, False
