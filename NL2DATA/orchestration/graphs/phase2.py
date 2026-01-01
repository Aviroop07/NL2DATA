"""Phase 2: Attribute Discovery & Schema Design Graph."""

from typing import Dict, Any, List, Literal
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _naming_validation_passed(state: IRGenerationState) -> Literal["passed", "failed"]:
    """Check if naming validation passed."""
    metadata = state.get("metadata", {})
    validation_passed = metadata.get("naming_validation_passed", False)
    
    if validation_passed:
        return "passed"
    return "failed"


def _cleanup_complete(state: IRGenerationState) -> Literal["complete", "incomplete"]:
    """Check if entity cleanup is complete."""
    metadata = state.get("metadata", {})
    cleanup_complete = metadata.get("cleanup_complete", False)
    
    if cleanup_complete:
        return "complete"
    return "incomplete"


def _attributes_to_name_lists(attributes: Dict[str, Any]) -> Dict[str, List[str]]:
    """Convert state["attributes"] (entity -> list[dict|model]) into entity -> list[str] names."""
    out: Dict[str, List[str]] = {}
    for entity_name, attrs in (attributes or {}).items():
        names: List[str] = []
        if isinstance(attrs, list):
            for a in attrs:
                if isinstance(a, dict):
                    n = a.get("name", "")
                else:
                    n = getattr(a, "name", "")
                if n:
                    names.append(n)
        out[entity_name] = names
    return out


def _wrap_step_2_1(step_func):
    """Wrap Step 2.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.1: Attribute Count Detection")
        result = await step_func(
            entities=state.get("entities", []),
            nl_description=state["nl_description"]
        )
        
        return {
            "current_step": "2.1",
            "previous_answers": {**state.get("previous_answers", {}), "2.1": result}
        }
    return node


def _wrap_step_2_2(step_func):
    """Wrap Step 2.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.2: Intrinsic Attributes")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            attribute_count_results=prev_answers.get("2.1"),
            domain=state.get("domain"),
            relations=state.get("relations", []),
            primary_keys=state.get("primary_keys", {})
        )
        
        # Normalize to entity -> attribute_list (not the wrapper payload)
        raw = result.get("entity_results", {}) or {}
        attributes = {k: (v.get("attributes", []) if isinstance(v, dict) else []) for k, v in raw.items()}
        
        return {
            "attributes": attributes,
            "current_step": "2.2",
            "previous_answers": {**prev_answers, "2.2": result}
        }
    return node


def _wrap_step_2_3(step_func):
    """Wrap Step 2.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.3: Attribute Synonym Detection")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            nl_description=state["nl_description"]
        )
        
        # Update attributes after synonym merging (entity -> list[dict])
        updated_attributes = result.get("updated_attributes", {}) or state.get("attributes", {})
        
        return {
            "attributes": updated_attributes,
            "current_step": "2.3",
            "previous_answers": {**prev_answers, "2.3": result}
        }
    return node


def _wrap_step_2_4(step_func):
    """Wrap Step 2.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.4: Composite Attribute Handling")
        prev_answers = state.get("previous_answers", {})
        attr_name_lists = _attributes_to_name_lists(state.get("attributes", {}))
        result = await step_func(
            entities=state.get("entities", []),
            entity_attributes=attr_name_lists,
            nl_description=state["nl_description"],
        )

        # Composite handling currently produces recommendations; keep attributes unchanged.
        return {
            "current_step": "2.4",
            "previous_answers": {**prev_answers, "2.4": result},
            "composite_decompositions": result.get("composite_decompositions", {}) if isinstance(result, dict) else {},
        }
    return node


def _wrap_step_2_5(step_func):
    """Wrap Step 2.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.5: Temporal Attributes Detection")
        prev_answers = state.get("previous_answers", {})
        attr_name_lists = _attributes_to_name_lists(state.get("attributes", {}))
        result = await step_func(
            entities=state.get("entities", []),
            entity_attributes=attr_name_lists,
            nl_description=state["nl_description"]
        )

        # Temporal step produces recommendations; keep attributes unchanged for now.
        return {
            "current_step": "2.5",
            "previous_answers": {**prev_answers, "2.5": result}
        }
    return node


def _wrap_step_2_6(step_func):
    """Wrap Step 2.6 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.6: Naming Convention Validation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=_attributes_to_name_lists(state.get("attributes", {})),
            domain=state.get("domain"),
            nl_description=state["nl_description"]
        )
        
        return {
            "current_step": "2.6",
            "previous_answers": {**prev_answers, "2.6": result},
            "metadata": {
                **state.get("metadata", {}),
                "naming_validation_passed": result.get("validation_passed", False)
            }
        }
    return node


def _wrap_step_2_7(step_func):
    """Wrap Step 2.7 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.7: Primary Key Identification")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=_attributes_to_name_lists(state.get("attributes", {}))
        )
        
        # Update primary keys
        primary_keys = result.get("entity_results", {})
        
        return {
            "primary_keys": primary_keys,
            "current_step": "2.7",
            "previous_answers": {**prev_answers, "2.7": result}
        }
    return node


def _wrap_step_2_8(step_func):
    """Wrap Step 2.8 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.8: Multivalued/Derived Detection")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            entity_attributes=_attributes_to_name_lists(state.get("attributes", {})),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )
        
        return {
            "current_step": "2.8",
            "previous_answers": {**prev_answers, "2.8": result}
        }
    return node


def _wrap_step_2_9(step_func):
    """Wrap Step 2.9 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.9: Derived Attribute Formulas")
        prev_answers = state.get("previous_answers", {})
        mv = (prev_answers.get("2.8") or {}).get("entity_results", {}) if isinstance(prev_answers.get("2.8"), dict) else {}
        entity_derived_attributes: Dict[str, List[str]] = {}
        derivation_rules: Dict[str, Dict[str, str]] = {}
        for entity_name, mv_out in (mv or {}).items():
            if not isinstance(mv_out, dict):
                continue
            derived = mv_out.get("derived", []) or []
            if isinstance(derived, list) and derived:
                entity_derived_attributes[entity_name] = [d for d in derived if isinstance(d, str) and d]
            dr = mv_out.get("derivation_rules", {}) or {}
            if isinstance(dr, dict) and dr:
                derivation_rules[entity_name] = {k: str(v) for k, v in dr.items() if isinstance(k, str) and k}

        entity_attr_lists = _attributes_to_name_lists(state.get("attributes", {}))
        entity_descriptions = {
            (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")): (
                e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
            )
            for e in (state.get("entities", []) or [])
            if (e.get("name") if isinstance(e, dict) else getattr(e, "name", ""))
        }

        result = await step_func(
            entity_derived_attributes=entity_derived_attributes,
            entity_attributes=entity_attr_lists,
            entity_descriptions=entity_descriptions,
            derivation_rules=derivation_rules,
            nl_description=state["nl_description"],
        )

        # Flatten into state["derived_formulas"]/["derived_metrics"] keyed by "Entity.attr"
        derived_formulas: Dict[str, Dict[str, Any]] = {}
        derived_metrics: Dict[str, Dict[str, Any]] = {}
        if isinstance(result, dict):
            row_level = result.get("entity_results", {}) or {}
            metrics = result.get("entity_metrics", {}) or {}
            if isinstance(row_level, dict):
                for ent, mp in row_level.items():
                    if not isinstance(mp, dict):
                        continue
                    for attr, info in mp.items():
                        if isinstance(attr, str) and attr and isinstance(info, dict):
                            derived_formulas[f"{ent}.{attr}"] = info
            if isinstance(metrics, dict):
                for ent, mp in metrics.items():
                    if not isinstance(mp, dict):
                        continue
                    for attr, info in mp.items():
                        if isinstance(attr, str) and attr and isinstance(info, dict):
                            derived_metrics[f"{ent}.{attr}"] = info
        
        return {
            "current_step": "2.9",
            "previous_answers": {**prev_answers, "2.9": result},
            "derived_formulas": derived_formulas,
            "derived_metrics": derived_metrics,
        }
    return node


def _wrap_step_2_10(step_func):
    """Wrap Step 2.10 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.10: Unique Constraints")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=_attributes_to_name_lists(state.get("attributes", {})),
            primary_keys=state.get("primary_keys", {})
        )
        
        # Update constraints
        constraints = result.get("entity_results", {})
        constraint_list = []
        for entity_name, entity_constraints in constraints.items():
            for constraint in entity_constraints.get("unique_attributes", []):
                constraint_list.append({
                    "type": "unique",
                    "entity": entity_name,
                    "attributes": [constraint] if isinstance(constraint, str) else constraint
                })
        
        return {
            "constraints": constraint_list,
            "current_step": "2.10",
            "previous_answers": {**prev_answers, "2.10": result}
        }
    return node


def _wrap_step_2_11(step_func):
    """Wrap Step 2.11 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.11: Nullability Constraints")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=_attributes_to_name_lists(state.get("attributes", {}))
        )
        
        return {
            "current_step": "2.11",
            "previous_answers": {**prev_answers, "2.11": result}
        }
    return node


def _wrap_step_2_12(step_func):
    """Wrap Step 2.12 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.12: Default Values")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=_attributes_to_name_lists(state.get("attributes", {})),
            nullability_results=prev_answers.get("2.11")
        )
        
        return {
            "current_step": "2.12",
            "previous_answers": {**prev_answers, "2.12": result}
        }
    return node


def _wrap_step_2_14(step_func):
    """Wrap Step 2.14 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.14: Entity Cleanup")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            relations=state.get("relations", []),
            nl_description=state["nl_description"],
            domain=state.get("domain")
        )
        
        # Update attributes after cleanup (entity -> list[dict])
        updated_attributes = result.get("updated_attributes", {}) or state.get("attributes", {})
        
        return {
            "attributes": updated_attributes,
            "current_step": "2.14",
            "previous_answers": {**prev_answers, "2.14": result},
            "metadata": {
                **state.get("metadata", {}),
                "cleanup_complete": result.get("all_complete", False)
            }
        }
    return node


def _wrap_step_2_16(step_func):
    """Wrap Step 2.16 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.16: Cross-Entity Attribute Reconciliation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            relations=state.get("relations", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )
        updated_attributes = result.get("updated_attributes", {}) or state.get("attributes", {})
        return {
            "attributes": updated_attributes,
            "current_step": "2.16",
            "previous_answers": {**prev_answers, "2.16": result},
        }
    return node


def _wrap_step_2_15(step_func):
    """Wrap Step 2.15 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.15: Relation Intrinsic Attributes")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            relations=state.get("relations", []),
            entity_intrinsic_attributes=state.get("attributes", {}),
            nl_description=state["nl_description"],
            domain=state.get("domain")
        )
        
        return {
            "current_step": "2.15",
            "previous_answers": {**prev_answers, "2.15": result}
        }
    return node


def create_phase_2_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 2 (Attribute Discovery & Schema Design).
    
    This graph orchestrates all Phase 2 steps:
    1. Attribute Count Detection (2.1) - parallel per entity
    2. Intrinsic Attributes (2.2) - parallel per entity
    3. Attribute Synonym Detection (2.3) - parallel per entity
    4. Composite Attribute Handling (2.4) - parallel per entity
    5. Temporal Attributes Detection (2.5) - parallel per entity
    6. Naming Convention Validation (2.6) - deterministic
    7. Primary Key Identification (2.7) - parallel per entity
    8. Multivalued/Derived Detection (2.8) - parallel per entity
    9. Derived Attribute Formulas (2.9) - parallel per derived attribute
    10. Unique Constraints (2.10) - parallel per entity
    11. Nullability Constraints (2.11) - parallel per entity
    12. Default Values (2.12) - parallel per entity
    13. (disabled) Check Constraints (2.13)
    14. Entity Cleanup (2.14) - parallel per entity (loop)
    15. Relation Intrinsic Attributes (2.15) - parallel per relation
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase2 import (
        step_2_1_attribute_count_detection_batch,
        step_2_2_intrinsic_attributes_batch,
        step_2_3_attribute_synonym_detection_batch,
        step_2_4_composite_attribute_handling_batch,
        step_2_5_temporal_attributes_detection_batch,
        step_2_6_naming_convention_validation,
        step_2_7_primary_key_identification_batch,
        step_2_8_multivalued_derived_detection_batch,
        step_2_9_derived_attribute_formulas_batch,
        step_2_10_unique_constraints_batch,
        step_2_11_nullability_constraints_batch,
        step_2_12_default_values_batch,
    )
    from NL2DATA.phases.phase2.step_2_14_entity_cleanup import step_2_14_entity_cleanup_batch
    from NL2DATA.phases.phase2.step_2_16_cross_entity_attribute_reconciliation import (
        step_2_16_cross_entity_attribute_reconciliation_batch,
    )
    from NL2DATA.phases.phase2.step_2_15_relation_intrinsic_attributes import step_2_15_relation_intrinsic_attributes_batch
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes (wrapped to work with LangGraph state)
    workflow.add_node("attribute_count", _wrap_step_2_1(step_2_1_attribute_count_detection_batch))
    workflow.add_node("intrinsic_attributes", _wrap_step_2_2(step_2_2_intrinsic_attributes_batch))
    workflow.add_node("synonym_detection", _wrap_step_2_3(step_2_3_attribute_synonym_detection_batch))
    workflow.add_node("cross_entity_reconcile", _wrap_step_2_16(step_2_16_cross_entity_attribute_reconciliation_batch))
    workflow.add_node("composite_handling", _wrap_step_2_4(step_2_4_composite_attribute_handling_batch))
    workflow.add_node("temporal_attributes", _wrap_step_2_5(step_2_5_temporal_attributes_detection_batch))
    workflow.add_node("naming_validation", _wrap_step_2_6(step_2_6_naming_convention_validation))
    workflow.add_node("primary_keys", _wrap_step_2_7(step_2_7_primary_key_identification_batch))
    workflow.add_node("multivalued_derived", _wrap_step_2_8(step_2_8_multivalued_derived_detection_batch))
    workflow.add_node("derived_formulas", _wrap_step_2_9(step_2_9_derived_attribute_formulas_batch))
    workflow.add_node("unique_constraints", _wrap_step_2_10(step_2_10_unique_constraints_batch))
    workflow.add_node("nullability", _wrap_step_2_11(step_2_11_nullability_constraints_batch))
    workflow.add_node("default_values", _wrap_step_2_12(step_2_12_default_values_batch))
    workflow.add_node("entity_cleanup", _wrap_step_2_14(step_2_14_entity_cleanup_batch))
    workflow.add_node("relation_attributes", _wrap_step_2_15(step_2_15_relation_intrinsic_attributes_batch))
    
    # Set entry point
    workflow.set_entry_point("attribute_count")
    
    # Add edges (sequential flow with parallel execution within nodes)
    workflow.add_edge("attribute_count", "intrinsic_attributes")
    workflow.add_edge("intrinsic_attributes", "synonym_detection")
    workflow.add_edge("synonym_detection", "cross_entity_reconcile")
    workflow.add_edge("cross_entity_reconcile", "composite_handling")
    workflow.add_edge("composite_handling", "temporal_attributes")
    workflow.add_edge("temporal_attributes", "naming_validation")
    
    # Conditional: Loop back if naming validation fails
    workflow.add_conditional_edges(
        "naming_validation",
        _naming_validation_passed,
        {
            "passed": "primary_keys",
            "failed": "synonym_detection"  # Loop back to fix naming issues
        }
    )
    
    workflow.add_edge("primary_keys", "multivalued_derived")
    workflow.add_edge("multivalued_derived", "derived_formulas")
    workflow.add_edge("derived_formulas", "unique_constraints")
    workflow.add_edge("unique_constraints", "nullability")
    workflow.add_edge("nullability", "default_values")
    workflow.add_edge("default_values", "entity_cleanup")
    
    # Conditional: Loop back if cleanup not complete
    workflow.add_conditional_edges(
        "entity_cleanup",
        _cleanup_complete,
        {
            "complete": "relation_attributes",
            "incomplete": "entity_cleanup"  # Loop back
        }
    )
    
    workflow.add_edge("relation_attributes", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

