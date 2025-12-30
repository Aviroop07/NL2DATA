"""Phase 1: Domain & Entity Discovery Graph."""

from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger


def _should_infer_domain(state: IRGenerationState) -> Literal["infer", "skip"]:
    """Determine if domain inference is needed."""
    if state.get("has_explicit_domain", False):
        return "skip"
    if state.get("domain"):
        return "skip"
    return "infer"


def _has_orphans(state: IRGenerationState) -> Literal["has_orphans", "no_orphans"]:
    """Check if there are orphan entities."""
    metadata = state.get("metadata", {})
    orphan_entities = metadata.get("orphan_entities", [])
    
    if orphan_entities:
        return "has_orphans"
    return "no_orphans"


def _validation_passed(state: IRGenerationState) -> Literal["passed", "failed"]:
    """Check if relation validation passed."""
    metadata = state.get("metadata", {})
    validation_passed = metadata.get("validation_passed", False)
    
    if validation_passed:
        return "passed"
    return "failed"


def _wrap_step_1_1(step_func):
    """Wrap Step 1.1 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.1: Domain Detection")
        result = await step_func(state["nl_description"])
        
        return {
            "domain": result.get("domain"),
            "has_explicit_domain": result.get("has_explicit_domain", False),
            "current_step": "1.1",
            "previous_answers": {**state.get("previous_answers", {}), "1.1": result}
        }
    return node


def _wrap_step_1_2(step_func):
    """Wrap Step 1.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.2: Entity Mention Detection")
        result = await step_func(state["nl_description"])
        
        return {
            "current_step": "1.2",
            "previous_answers": {**state.get("previous_answers", {}), "1.2": result}
        }
    return node


def _wrap_step_1_3(step_func):
    """Wrap Step 1.3 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.3: Domain Inference")
        result = await step_func(
            state["nl_description"],
            domain_detection_result=state.get("previous_answers", {}).get("1.1")
        )
        
        return {
            "domain": result.get("domain"),
            "current_step": "1.3",
            "previous_answers": {**state.get("previous_answers", {}), "1.3": result}
        }
    return node


def _wrap_step_1_4(step_func):
    """Wrap Step 1.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.4: Key Entity Extraction")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            state["nl_description"],
            domain=state.get("domain"),
            domain_detection_result=prev_answers.get("1.1"),
            entity_mention_result=prev_answers.get("1.2")
        )
        
        # Convert EntityInfo objects to dicts for state
        entities = result.get("entities", [])
        entity_dicts = [
            e.dict() if hasattr(e, "dict") else e
            for e in entities
        ]
        
        return {
            "entities": entity_dicts,
            "current_step": "1.4",
            "previous_answers": {**prev_answers, "1.4": result}
        }
    return node


def _wrap_step_1_5(step_func):
    """Wrap Step 1.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.5: Relation Mention Detection")
        result = await step_func(
            nl_description=state["nl_description"],
            entities=state.get("entities", [])
        )
        
        # Don't update current_step or previous_answers here - they're updated in consolidation step after parallel nodes merge
        # Store result in metadata temporarily so consolidation step can access it
        return {
            "metadata": {
                **state.get("metadata", {}),
                "step_1_5_result": result
            }
        }
    return node


def _wrap_step_1_6(step_func):
    """Wrap Step 1.6 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.6: Auxiliary Entity Suggestion")
        result = await step_func(
            nl_description=state["nl_description"],
            key_entities=state.get("entities", []),
            domain=state.get("domain")
        )
        
        # Add suggested entities to entities list
        suggested = result.get("suggested_entities", [])
        suggested_dicts = [
            {"name": e.get("name", ""), "description": e.get("reason", ""), "reasoning": e.get("reason", "")}
            if isinstance(e, dict) else {"name": getattr(e, "name", ""), "description": getattr(e, "reason", ""), "reasoning": getattr(e, "reason", "")}
            for e in suggested
        ]
        
        # Don't update current_step or previous_answers here - they're updated in consolidation step after parallel nodes merge
        # Store result in metadata temporarily so consolidation step can access it
        return {
            "entities": suggested_dicts,  # Will be merged with existing entities
            "metadata": {
                **state.get("metadata", {}),
                "step_1_6_result": result
            }
        }
    return node


def _wrap_step_1_7(step_func):
    """Wrap Step 1.7 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.7: Entity Consolidation")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        
        # Retrieve results from parallel nodes (1.5 and 1.6) from metadata
        step_1_5_result = metadata.get("step_1_5_result")
        step_1_6_result = metadata.get("step_1_6_result")
        
        result = await step_func(
            key_entities=state.get("entities", []),
            auxiliary_entities=state.get("entities", []),  # Already merged
            domain=state.get("domain"),
            nl_description=state["nl_description"]
        )
        
        # Update entities with consolidated list
        final_entities = result.get("final_entity_list", [])
        # Keep entity info for entities that remain
        existing_entities = state.get("entities", [])
        entity_dicts = [
            e for e in existing_entities
            if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) in final_entities
        ]
        
        # Merge previous_answers from parallel nodes and current step
        updated_prev_answers = {**prev_answers}
        if step_1_5_result:
            updated_prev_answers["1.5"] = step_1_5_result
        if step_1_6_result:
            updated_prev_answers["1.6"] = step_1_6_result
        updated_prev_answers["1.7"] = result
        
        # Clean up temporary metadata
        cleaned_metadata = {k: v for k, v in metadata.items() if k not in ["step_1_5_result", "step_1_6_result"]}
        
        return {
            "entities": entity_dicts,
            "current_step": "1.7",
            "previous_answers": updated_prev_answers,
            "metadata": cleaned_metadata
        }
    return node


def _wrap_step_1_75(step_func):
    """Wrap Step 1.75 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.75: Entity vs Relation Reclassification")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )

        # Update entities with filtered list; keep relation candidates in metadata for downstream prompts
        relation_candidates = result.get("relation_candidates", []) or []

        return {
            "entities": result.get("entities", []),
            "current_step": "1.75",
            "previous_answers": {**prev_answers, "1.75": result},
            "metadata": {
                **state.get("metadata", {}),
                "relation_candidates": relation_candidates,
            },
        }
    return node


def _wrap_step_1_8(step_func):
    """Wrap Step 1.8 to work as LangGraph node."""
    from NL2DATA.phases.phase1.step_1_8_entity_cardinality import step_1_8_entity_cardinality_single
    
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.8: Entity Cardinality")
        import asyncio
        
        entities = state.get("entities", [])
        tasks = []
        entity_names = []
        
        for entity in entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_names.append(entity_name)
            tasks.append(
                step_1_8_entity_cardinality_single(
                    entity_name=entity_name,
                    entity_description=entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", ""),
                    nl_description=state["nl_description"],
                    domain=state.get("domain")
                )
            )
        
        # Execute in parallel
        results = await asyncio.gather(*tasks)
        
        # Update entity cardinalities
        entity_cardinalities = {}
        for entity_name, result in zip(entity_names, results):
            entity_info = result.get("entity_info", [{}])[0] if result.get("entity_info") else {}
            entity_cardinalities[entity_name] = {
                "cardinality": entity_info.get("cardinality"),
                "table_type": entity_info.get("table_type"),
            }
        
        return {
            "entity_cardinalities": entity_cardinalities,
            "current_step": "1.8",
            "previous_answers": {**state.get("previous_answers", {}), "1.8": {"results": results}}
        }
    return node


def _wrap_step_1_9(step_func):
    """Wrap Step 1.9 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.9: Key Relations Extraction")
        prev_answers = state.get("previous_answers", {})
        mentioned_relations = None
        rel_mention = prev_answers.get("1.5") or {}
        if isinstance(rel_mention, dict):
            mentioned_relations = rel_mention.get("mentioned_relations")

        # Include Step 1.75 relation candidates as additional hints
        relation_candidates = []
        step_1_75 = prev_answers.get("1.75") or {}
        if isinstance(step_1_75, dict):
            relation_candidates = step_1_75.get("relation_candidates", []) or []
        if relation_candidates:
            mentioned_relations = (mentioned_relations or []) + relation_candidates

        result = await step_func(
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
            mentioned_relations=mentioned_relations,
        )
        
        # Convert RelationInfo objects to dicts
        relations = result.get("relations", [])
        relation_dicts = [
            r.dict() if hasattr(r, "dict") else r
            for r in relations
        ]
        
        return {
            "relations": relation_dicts,
            "current_step": "1.9",
            "previous_answers": {**prev_answers, "1.9": result}
        }
    return node


def _wrap_step_1_10(step_func):
    """Wrap Step 1.10 to work as LangGraph node."""
    from NL2DATA.phases.phase1.step_1_10_schema_connectivity import step_1_10_schema_connectivity
    
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.10: Schema Connectivity")
        prev_answers = state.get("previous_answers", {})
        result = await step_1_10_schema_connectivity(
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
            entity_extraction_result=prev_answers.get("1.4")
        )
        
        return {
            "current_step": "1.10",
            "previous_answers": {**prev_answers, "1.10": result},
            "metadata": {
                **state.get("metadata", {}),
                "orphan_entities": result.get("orphan_entities", []),
                "connectivity_status": result.get("connectivity_status", {})
            }
        }
    return node


def _wrap_step_1_11(step_func):
    """Wrap Step 1.11 to work as LangGraph node."""
    from NL2DATA.phases.phase1.step_1_11_relation_cardinality import step_1_11_relation_cardinality_single
    
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.11: Relation Cardinality")
        import asyncio
        
        relations = state.get("relations", [])
        tasks = []
        relation_ids = []
        
        for relation in relations:
            # Create relation ID
            entities = relation.get("entities", []) if isinstance(relation, dict) else getattr(relation, "entities", [])
            relation_id = "+".join(sorted(entities))
            relation_ids.append(relation_id)
            
            tasks.append(
                step_1_11_relation_cardinality_single(
                    relation=relation,
                    nl_description=state["nl_description"]
                )
            )
        
        # Execute in parallel
        results = await asyncio.gather(*tasks)
        
        # Update relation cardinalities
        relation_cardinalities = {}
        for relation_id, result in zip(relation_ids, results):
            relation_cardinalities[relation_id] = {
                "entity_cardinalities": result.get("entity_cardinalities", {}),
                "entity_participations": result.get("entity_participations", {}),
            }
        
        return {
            "relation_cardinalities": relation_cardinalities,
            "current_step": "1.11",
            "previous_answers": {**state.get("previous_answers", {}), "1.11": {"results": results}}
        }
    return node


def _wrap_step_1_12(step_func):
    """Wrap Step 1.12 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.12: Relation Validation")
        prev_answers = state.get("previous_answers", {})
        result = await step_func(
            relations=state.get("relations", []),
            entities=state.get("entities", []),
            relation_cardinalities=state.get("relation_cardinalities", {}),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
            entity_extraction_result=prev_answers.get("1.4"),
            relation_extraction_result=prev_answers.get("1.9"),
            cardinality_result=prev_answers.get("1.11")
        )
        
        return {
            "current_step": "1.12",
            "previous_answers": {**prev_answers, "1.12": result},
            "metadata": {
                **state.get("metadata", {}),
                "validation_passed": result.get("validation_passed", False)
            }
        }
    return node


def create_phase_1_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 1 (Domain & Entity Discovery).
    
    This graph orchestrates all Phase 1 steps:
    1. Domain Detection (1.1)
    2. Entity Mention Detection (1.2)
    3. Domain Inference (1.3) - conditional
    4. Key Entity Extraction (1.4)
    5. Relation Mention Detection (1.5) - parallel with 1.6
    6. Auxiliary Entity Suggestion (1.6) - parallel with 1.5
    7. Entity Consolidation (1.7)
    7b. Entity vs Relation Reclassification (1.75)
    8. Entity Cardinality (1.8) - parallel per entity
    9. Key Relations Extraction (1.9)
    10. Schema Connectivity (1.10) - loop if orphans found
    11. Relation Cardinality (1.11) - parallel per relation
    12. Relation Validation (1.12) - loop if validation fails
    
    Returns:
        Compiled StateGraph ready for execution
    """
    from NL2DATA.phases.phase1 import (
        step_1_1_domain_detection,
        step_1_2_entity_mention_detection,
        step_1_3_domain_inference,
        step_1_4_key_entity_extraction,
        step_1_5_relation_mention_detection,
        step_1_6_auxiliary_entity_suggestion,
        step_1_7_entity_consolidation,
        step_1_75_entity_relation_reclassification,
        step_1_8_entity_cardinality,
        step_1_9_key_relations_extraction,
        step_1_10_schema_connectivity,
        step_1_11_relation_cardinality,
        step_1_12_relation_validation,
    )
    
    # Create graph
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes (wrapped to work with LangGraph state)
    workflow.add_node("domain_detection", _wrap_step_1_1(step_1_1_domain_detection))
    workflow.add_node("entity_mention", _wrap_step_1_2(step_1_2_entity_mention_detection))
    workflow.add_node("domain_inference", _wrap_step_1_3(step_1_3_domain_inference))
    workflow.add_node("entity_extraction", _wrap_step_1_4(step_1_4_key_entity_extraction))
    workflow.add_node("relation_mention", _wrap_step_1_5(step_1_5_relation_mention_detection))
    workflow.add_node("auxiliary_entities", _wrap_step_1_6(step_1_6_auxiliary_entity_suggestion))
    workflow.add_node("entity_consolidation", _wrap_step_1_7(step_1_7_entity_consolidation))
    workflow.add_node("entity_reclassification", _wrap_step_1_75(step_1_75_entity_relation_reclassification))
    workflow.add_node("entity_cardinality", _wrap_step_1_8(step_1_8_entity_cardinality))
    workflow.add_node("relation_extraction", _wrap_step_1_9(step_1_9_key_relations_extraction))
    workflow.add_node("schema_connectivity", _wrap_step_1_10(step_1_10_schema_connectivity))
    workflow.add_node("relation_cardinality", _wrap_step_1_11(step_1_11_relation_cardinality))
    workflow.add_node("relation_validation", _wrap_step_1_12(step_1_12_relation_validation))
    
    # Set entry point
    workflow.set_entry_point("domain_detection")
    
    # Add edges
    workflow.add_edge("domain_detection", "entity_mention")
    
    # Conditional: Skip domain inference if domain already found
    workflow.add_conditional_edges(
        "entity_mention",
        _should_infer_domain,
        {
            "infer": "domain_inference",
            "skip": "entity_extraction"
        }
    )
    workflow.add_edge("domain_inference", "entity_extraction")
    
    # Parallel: Relation mention and auxiliary entities (both depend on entity extraction)
    workflow.add_edge("entity_extraction", "relation_mention")
    workflow.add_edge("entity_extraction", "auxiliary_entities")
    
    # Merge parallel results before consolidation
    workflow.add_edge("relation_mention", "entity_consolidation")
    workflow.add_edge("auxiliary_entities", "entity_consolidation")
    
    workflow.add_edge("entity_consolidation", "entity_reclassification")
    workflow.add_edge("entity_reclassification", "entity_cardinality")
    workflow.add_edge("entity_cardinality", "relation_extraction")
    workflow.add_edge("relation_extraction", "schema_connectivity")
    
    # Conditional: Loop back if orphans found
    workflow.add_conditional_edges(
        "schema_connectivity",
        _has_orphans,
        {
            "has_orphans": "relation_extraction",  # Loop back
            "no_orphans": "relation_cardinality"
        }
    )
    
    workflow.add_edge("relation_cardinality", "relation_validation")
    
    # Conditional: Loop back if validation fails
    workflow.add_conditional_edges(
        "relation_validation",
        _validation_passed,
        {
            "failed": "relation_extraction",  # Loop back to fix
            "passed": END
        }
    )
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

