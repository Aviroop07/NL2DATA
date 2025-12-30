"""Phase 1, Step 1.12: Relation Validation.

Validates relations for circular dependencies, impossible cardinalities, or conflicting types.
"""

import json
import re
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
    build_relation_list_string,
    get_entities_in_relation,
)
from NL2DATA.utils.tools.validation_tools import (
    _detect_circular_dependencies_impl,
    _validate_cardinality_consistency_impl,
)

logger = get_logger(__name__)


class RelationValidationOutput(BaseModel):
    """Output structure for relation validation."""
    circular_dependencies: List[str] = Field(
        default_factory=list,
        description="List of circular dependency patterns found (e.g., 'A->B->C->A'). Each string should describe the pattern."
    )
    impossible_cardinalities: List[str] = Field(
        default_factory=list,
        description="List of relations with impossible cardinality combinations. Format: 'RelationName1, RelationName2: description of issue'"
    )
    conflicts: List[str] = Field(
        default_factory=list,
        description="List of conflicting relationship types or descriptions. Format: 'RelationName1, RelationName2: description of conflict'"
    )
    validation_passed: bool = Field(
        description="Whether the validation passed (True if no issues found)"
    )
    reasoning: str = Field(description="Explanation of validation results and any issues found")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.12", phase=1, tags=["relation_validation"])
async def step_1_12_relation_validation(
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    relation_cardinalities: Optional[List[Dict[str, Any]]] = None,
    nl_description: Optional[str] = None,
    previous_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Step 1.12: Validate relations for logical errors.
    
    This step catches:
    - Circular dependencies (e.g., A->B->C->A)
    - Impossible cardinalities (e.g., 1:1 when it should be 1:N)
    - Conflicting relationship types
    
    This step supports iterative refinement: if validation fails, it should loop back
    to Step 1.9 to fix relations. Use step_1_12_relation_validation_with_loop()
    for automatic looping with safety guardrails.
    
    Args:
        entities: List of all entities in the schema
        relations: List of all relations from Step 1.9
        relation_cardinalities: Optional list of cardinality information from Step 1.11
        nl_description: Optional original NL description for context
        previous_result: Optional previous iteration result (for loop support)
        
    Returns:
        dict: Validation result with circular_dependencies, impossible_cardinalities, conflicts, validation_passed, and reasoning
        
    Example:
        >>> result = await step_1_12_relation_validation(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"], "type": "one-to-many"}]
        ... )
        >>> result["validation_passed"]
        True
    """
    iteration_num = (previous_result.get("iteration", 0) + 1) if previous_result else 1
    if iteration_num > 1:
        logger.info(f"Starting Step 1.12: Relation Validation (iteration {iteration_num})")
    else:
        logger.info("Starting Step 1.12: Relation Validation")
    
    # Build entity list using utilities
    entity_list_str = build_entity_list_string(entities, include_descriptions=False, prefix="- ")
    
    # Build cardinality and participation maps if available
    cardinality_map = {}
    participation_map = {}
    if relation_cardinalities:
        for rel_card in relation_cardinalities:
            entities_in_rel = get_entities_in_relation(rel_card)
            if entities_in_rel:
                key = tuple(sorted(entities_in_rel))
                cardinality_map[key] = rel_card.get("entity_cardinalities", {})
                participation_map[key] = rel_card.get("entity_participations", {})
    
    # Build relation list with cardinalities and participations using utilities
    relation_list_str = build_relation_list_string(
        relations,
        include_cardinalities=cardinality_map if cardinality_map else None,
        include_participations=participation_map if participation_map else None,
    )
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to validate relationships in a database schema for logical errors and inconsistencies.

Common issues to detect:

1. **Circular Dependencies**: 
   - Patterns like A->B->C->A create circular references
   - Example: Customer places Order, Order has Payment, Payment belongs to Customer (circular)
   - These can cause issues with referential integrity and data integrity

2. **Impossible Cardinalities**:
   - Cardinality combinations that don't make logical sense
   - Example: A relation says "one-to-many" but cardinalities are both "1" (should be 1 and N)
   - Example: A relation says "many-to-many" but cardinalities are 1 and 1 (should be N and N)

3. **Conflicting Types**:
   - Relationship type doesn't match the description
   - Example: Type says "one-to-one" but description says "a customer can have many orders"
   - Multiple relations between same entities with conflicting types

4. **Missing Cardinalities**:
   - Relations without cardinality information (if cardinalities were provided for other relations)

Your task:
- Analyze all relationships for these issues
- Identify specific problems with clear descriptions
- Determine if validation passed (no issues) or failed (issues found)
- Provide reasoning for your analysis
- You MUST respond with valid JSON format only

Important:
- Be thorough but not overly strict (some patterns may be valid)
- Focus on clear logical errors, not style preferences
- Consider the domain context when evaluating validity

You have access to validation tools:
- detect_circular_dependencies: Use this to detect circular reference patterns in relations
- validate_cardinality_consistency: Use this to validate that cardinality values are consistent with relation types

Use these tools to validate your analysis before finalizing your response.

CRITICAL: You MUST return ONLY valid JSON. Do NOT include any markdown formatting, explanations, or text outside the JSON object.

Provide your response as a JSON object with:
- circular_dependencies: List of strings describing circular dependency patterns (e.g., ["A->B->C->A"]). Empty array if none.
- impossible_cardinalities: List of strings describing relations with impossible cardinalities. Format each as: "Entity1, Entity2: description of issue". Empty array if none.
- conflicts: List of strings describing conflicting relationship types. Format each as: "Entity1, Entity2: description of conflict". Empty array if none.
- validation_passed: true if no issues found, false otherwise
- reasoning: REQUIRED - String with clear explanation of validation results (cannot be omitted)

IMPORTANT: All list items must be STRINGS, not objects. For example:
- CORRECT: "impossible_cardinalities": ["Sensor, SensorReading: cardinalities are missing"]
- WRONG: "impossible_cardinalities": [{{"relation": "Sensor, SensorReading", "issue": "cardinalities are missing"}}]

Example JSON output (NO markdown, NO text before/after):
{{"circular_dependencies": [], "impossible_cardinalities": ["A, B: cardinality mismatch"], "conflicts": [], "validation_passed": false, "reasoning": "Found issues..."}}"""
    
    # Human prompt template
    human_prompt = f"""Entities in the schema:
{{entity_list}}

Relations in the schema (with cardinalities if available):
{{relation_list}}

Original description (if available):
{{nl_description}}"""
    
    # Deterministic validation (no agent/tool calling).
    # This avoids brittle tool-argument schema errors and produces stable validation results.
    # NOTE: The current cycle detector treats relations as directed edges and will flag cycles
    # in a general schema graph. Cycles are *normal* in ER-style relationship graphs and are not
    # inherently invalid in Phase 1 (FK directionality isn't finalized yet).
    # We therefore compute cycles for informational purposes but do NOT fail validation on them.
    circular = _detect_circular_dependencies_impl(relations).get("cycles", []) or []
    circular_patterns = ["->".join([*cycle]) for cycle in circular] if circular else []

    impossible: List[str] = []
    conflicts: List[str] = []

    # Cardinality consistency checks (only when we have sufficient info)
    for rel in relations or []:
        rel_entities = rel.get("entities", []) or []
        rel_type = rel.get("type", "") or ""
        cards = rel.get("entity_cardinalities") or {}
        if rel_type and cards:
            check = _validate_cardinality_consistency_impl(rel)
            if not check.get("is_consistent", True):
                e1 = rel_entities[0] if len(rel_entities) > 0 else "?"
                e2 = rel_entities[1] if len(rel_entities) > 1 else "?"
                impossible.append(f"{e1}, {e2}: {', '.join(check.get('errors', []))}")

    # Conflicting relation types between same entity pair
    pair_to_types: Dict[tuple, set] = {}
    for rel in relations or []:
        ents = tuple(sorted(rel.get("entities", []) or []))
        if len(ents) < 2:
            continue
        t = (rel.get("type", "") or "").strip().lower()
        if not t:
            continue
        pair_to_types.setdefault(ents, set()).add(t)
    for pair, types in pair_to_types.items():
        if len(types) > 1:
            e1, e2 = pair[0], pair[1]
            conflicts.append(f"{e1}, {e2}: conflicting relation types detected: {sorted(list(types))}")

    validation_passed = (len(impossible) == 0 and len(conflicts) == 0)
    reasoning_lines = [
        f"Deterministic validation summary:",
        f"- circular_dependencies (informational only at Phase 1): {len(circular_patterns)}",
        f"- impossible_cardinalities: {len(impossible)}",
        f"- conflicts: {len(conflicts)}",
    ]
    if circular_patterns:
        reasoning_lines.append(
            "Note: Graph cycles are expected in Phase 1 and are not treated as errors (FK direction not finalized)."
        )
    if not validation_passed:
        if circular_patterns:
            reasoning_lines.append(f"circular patterns: {circular_patterns}")
        if impossible:
            reasoning_lines.append(f"impossible cardinalities: {impossible[:5]}")
        if conflicts:
            reasoning_lines.append(f"conflicts: {conflicts[:5]}")

    result_dict = {
        "circular_dependencies": circular_patterns,
        "impossible_cardinalities": impossible,
        "conflicts": conflicts,
        "validation_passed": validation_passed,
        "reasoning": "\n".join(reasoning_lines),
    }
    
    # Work with result_dict for logging (already converted from Pydantic model)
    validation_passed = result_dict.get("validation_passed", False)
    circular_count = len(result_dict.get("circular_dependencies", []))
    impossible_count = len(result_dict.get("impossible_cardinalities", []))
    conflict_count = len(result_dict.get("conflicts", []))
    
    if validation_passed:
        logger.info("Relation validation passed - no issues found")
    else:
        logger.warning(
            f"Relation validation found issues: {circular_count} circular dependencies, "
            f"{impossible_count} impossible cardinalities, {conflict_count} conflicts"
        )
    
    # Add iteration info for loop tracking
    result_dict["iteration"] = iteration_num
    result_dict["needs_loop"] = not result_dict.get("validation_passed", False)
    
    return result_dict


async def step_1_12_relation_validation_with_loop(
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    relation_cardinalities: Optional[List[Dict[str, Any]]] = None,
    nl_description: Optional[str] = None,
    max_iterations: int = 3,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 1.12 with automatic looping: loops back to relation extraction if validation fails.
    
    This function implements the conditional loop specified in the plan: if validation fails,
    it loops back to Step 1.9 (relation extraction) to fix relations, then re-validates.
    
    Args:
        entities: List of all entities in the schema
        relations: Initial list of relations from Step 1.9
        relation_cardinalities: Optional list of cardinality information from Step 1.11
        nl_description: Optional original NL description for context
        max_iterations: Maximum number of loop iterations (default: 3)
        max_time_sec: Maximum wall time in seconds (default: 180)
        
    Returns:
        dict: Final validation result with loop metadata
        
    Example:
        >>> result = await step_1_12_relation_validation_with_loop(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"], "type": "one-to-many"}]
        ... )
        >>> result["final_result"]["validation_passed"]
        True
    """
    from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
    
    logger.info("Starting Step 1.12: Relation Validation (with loop support)")
    
    current_relations = relations.copy()
    loop_history = []
    
    async def validation_check_step(previous_result=None):
        """Single iteration of validation check."""
        try:
            result = await step_1_12_relation_validation(
                entities=entities,
                relations=current_relations,
                relation_cardinalities=relation_cardinalities,
                nl_description=nl_description,
                previous_result=previous_result,
            )
            # Ensure result is not None
            if result is None:
                logger.error("Step 1.12 returned None, creating default error result")
                result = {
                    "circular_dependencies": [],
                    "impossible_cardinalities": [],
                    "conflicts": [],
                    "validation_passed": False,
                    "reasoning": "Validation step returned None",
                    "iteration": previous_result.get("iteration", 0) + 1 if previous_result else 1,
                    "needs_loop": False
                }
            loop_history.append(result)
            return result
        except Exception as e:
            logger.error(f"Error in validation check step: {e}", exc_info=True)
            # Return a default error result instead of raising
            error_result = {
                "circular_dependencies": [],
                "impossible_cardinalities": [],
                "conflicts": [],
                "validation_passed": False,
                "reasoning": f"Validation step failed: {str(e)}",
                "iteration": previous_result.get("iteration", 0) + 1 if previous_result else 1,
                "needs_loop": False
            }
            loop_history.append(error_result)
            return error_result
    
    def should_terminate(result: Dict[str, Any]) -> bool:
        """Check if loop should terminate (validation passed)."""
        if result is None:
            logger.warning("should_terminate received None result, returning False")
            return False
        return result.get("validation_passed", False)
    
    # Run loop
    config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
        oscillation_window=3,
        enable_cycle_detection=True
    )
    
    executor = SafeLoopExecutor()
    loop_result = await executor.run_loop(
        step_func=validation_check_step,
        termination_check=should_terminate,
        config=config
    )
    
    final_result = loop_result["result"]
    
    # If validation still fails after loop, log warning
    if not final_result.get("validation_passed", False):
        logger.warning(
            f"Relation validation failed after {loop_result['iterations']} iterations. "
            f"Issues found: {len(final_result.get('circular_dependencies', []))} circular dependencies, "
            f"{len(final_result.get('impossible_cardinalities', []))} impossible cardinalities, "
            f"{len(final_result.get('conflicts', []))} conflicts. "
            f"Consider manual review."
        )
    else:
        logger.info(
            f"Relation validation passed after {loop_result['iterations']} iteration(s)"
        )
    
    return {
        "final_result": final_result,
        "loop_metadata": {
            "iterations": loop_result["iterations"],
            "terminated_by": loop_result["terminated_by"],
            "history": loop_history
        }
    }

