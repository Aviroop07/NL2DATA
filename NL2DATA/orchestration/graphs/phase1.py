"""Phase 1: Domain & Entity Discovery Graph."""

from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


# Removed _should_infer_domain - step 1.1 now handles both detection and inference


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
        logger.info("[LangGraph] Executing Step 1.1: Domain Detection & Inference")
        result = await invoke_step_checked(step_func, state["nl_description"])
        
        # Handle Pydantic model
        if hasattr(result, 'model_dump'):
            result_dict = result.model_dump()
        else:
            result_dict = result
        
        return {
            "domain": result_dict.get("domain"),
            "has_explicit_domain": result_dict.get("has_explicit_domain", False),
            "current_step": "1.1",
            "previous_answers": {**state.get("previous_answers", {}), "1.1": result_dict}
        }
    return node


def _wrap_step_1_2(step_func):
    """Wrap Step 1.2 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.2: Entity Mention Detection")
        result = await invoke_step_checked(step_func, state["nl_description"])
        
        return {
            "current_step": "1.2",
            "previous_answers": {**state.get("previous_answers", {}), "1.2": result}
        }
    return node


# Removed _wrap_step_1_3 - step 1.1 now handles both detection and inference


def _wrap_step_1_4(step_func):
    """Wrap Step 1.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.4: Key Entity Extraction")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
            state["nl_description"],
            domain=state.get("domain"),
            domain_detection_result=prev_answers.get("1.1"),
            entity_mention_result=prev_answers.get("1.2")
        )
        
        # Handle Pydantic model
        if hasattr(result, 'entities'):
            entities = result.entities
        elif isinstance(result, dict):
            entities = result.get("entities", [])
        else:
            entities = []
        
        # Convert EntityInfo objects to dicts for state
        entity_dicts = [
            e.model_dump() if hasattr(e, "model_dump") else (e.dict() if hasattr(e, "dict") else e)
            for e in entities
        ]
        
        # Validate entity names before merging into state
        from NL2DATA.utils.validation.schema_anchored import validate_entity_names
        existing_entities = state.get("entities", [])
        existing_entity_names = {
            e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
            for e in existing_entities
        }
        existing_entity_names = {e for e in existing_entity_names if e}
        
        # Validate new entities against existing ones (if any)
        if existing_entity_names:
            validation_result = validate_entity_names(
                output={"entities": entity_dicts},
                allowed_entities=existing_entity_names,
                context="step_1_4_entity_extraction"
            )
            if not validation_result["valid"]:
                logger.warning(
                    f"Step 1.4: Entity name validation issues: {validation_result['errors']}. "
                    f"Suggestions: {validation_result['suggestions']}"
                )
        
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
        result = await invoke_step_checked(
            step_func,
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
        result = await invoke_step_checked(
            step_func,
            nl_description=state["nl_description"],
            key_entities=state.get("entities", []),
            domain=state.get("domain")
        )
        
        # CRITICAL: Do NOT return entities here - it would overwrite step 1.4's entities!
        # Step 1.6's suggested entities will be merged in step 1.7 (consolidation).
        # Store result in metadata temporarily so consolidation step can access it.
        return {
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
        
        # CRITICAL: Get key entities from step 1.4's result (not from state, which may have been overwritten)
        # and auxiliary entities from step 1.6's result.
        step_1_4_result = prev_answers.get("1.4", {})
        key_entities = step_1_4_result.get("entities", []) if isinstance(step_1_4_result, dict) else []
        # Convert EntityInfo objects to dicts if needed
        key_entity_dicts = [
            e.dict() if hasattr(e, "dict") else e
            for e in key_entities
        ]
        
        # Get auxiliary entities from step 1.6 result
        auxiliary_entities = []
        if step_1_6_result:
            if hasattr(step_1_6_result, 'suggested_entities'):
                suggested = step_1_6_result.suggested_entities
            elif isinstance(step_1_6_result, dict):
                suggested = step_1_6_result.get("suggested_entities", [])
            else:
                suggested = []
            
            for e in suggested:
                if hasattr(e, 'model_dump'):
                    e_dict = e.model_dump()
                elif isinstance(e, dict):
                    e_dict = e
                else:
                    e_dict = {"name": getattr(e, "name", ""), "description": getattr(e, "reasoning", ""), "reasoning": getattr(e, "reasoning", "")}
                auxiliary_entities.append({
                    "name": e_dict.get("name", ""),
                    "description": e_dict.get("reasoning", e_dict.get("reason", "")),
                    "reasoning": e_dict.get("reasoning", e_dict.get("reason", ""))
                })
        
        # If we don't have key entities from prev_answers, fall back to state (shouldn't happen, but defensive)
        if not key_entity_dicts:
            key_entity_dicts = state.get("entities", [])
        
        result = await invoke_step_checked(
            step_func,
            key_entities=key_entity_dicts,
            auxiliary_entities=auxiliary_entities,
            domain=state.get("domain"),
            nl_description=state["nl_description"]
        )
        
        # Handle Pydantic model
        if hasattr(result, 'final_entities'):
            final_entity_names = result.final_entities
        elif isinstance(result, dict):
            final_entity_names = result.get("final_entity_list", []) or result.get("final_entities", [])
        else:
            final_entity_names = []
        
        final_entity_names_set = {str(n).strip() for n in final_entity_names if n}
        
        # Build consolidated entity list: combine key and auxiliary, then filter by final names
        all_candidate_entities = key_entity_dicts + auxiliary_entities
        
        # CRITICAL: Ensure all key entities are included even if consolidation didn't explicitly list them
        # The consolidation step's final_entities may only include entities that were merged,
        # but we need ALL entities (key + auxiliary, after deduplication)
        key_entity_names = {e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "") for e in key_entity_dicts}
        key_entity_names = {n for n in key_entity_names if n}
        
        # If final_entity_names doesn't include all key entities, add them
        if key_entity_names and not key_entity_names.issubset(final_entity_names_set):
            missing_key_entities = key_entity_names - final_entity_names_set
            logger.warning(
                f"Step 1.7: final_entity_list missing {len(missing_key_entities)} key entities: {missing_key_entities}. "
                f"Adding them to final list."
            )
            final_entity_names_set.update(missing_key_entities)
        
        entity_dicts = []
        seen_names = set()
        
        # Filter all candidates by final names (now includes all key entities)
        for e in all_candidate_entities:
            name = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
            if name and name in final_entity_names_set and name.lower() not in seen_names:
                entity_dicts.append(e)
                seen_names.add(name.lower())
        
        # Final safety check: if we still have no entities, use all candidates (shouldn't happen)
        if not entity_dicts:
            logger.error("Step 1.7: No entities after consolidation! Using all candidate entities as fallback.")
            for e in all_candidate_entities:
                name = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
                if name and name.lower() not in seen_names:
                    entity_dicts.append(e)
                    seen_names.add(name.lower())
        
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
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )

        # Handle Pydantic model
        if hasattr(result, 'keep_entities'):
            # Step 1.75 returns keep_entities as a list of entity names (strings), not entity dicts
            # We need to filter the existing entities in state to match keep_entities
            keep_entity_names = set(result.keep_entities)
            existing_entities = state.get("entities", [])
            filtered_entities = [
                e for e in existing_entities
                if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) in keep_entity_names
            ]
            
            # Build relation candidates from reclassify_as_relation
            relation_candidates = []
            for reclass in result.reclassify_as_relation:
                endpoints_dict = {ep.side: ep.entity_name for ep in reclass.endpoints}
                left = endpoints_dict.get("left", "")
                right = endpoints_dict.get("right", "")
                relation_candidates.append(
                    f"{reclass.name} links {left} and {right} ({reclass.relationship_type}, {reclass.key_strategy})."
                )
            
            result_dict = result.model_dump()
        else:
            filtered_entities = result.get("entities", [])
            relation_candidates = result.get("relation_candidates", []) or []
            result_dict = result

        return {
            "entities": filtered_entities,
            "current_step": "1.75",
            "previous_answers": {**prev_answers, "1.75": result_dict},
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
        for entity_name, result_item in zip(entity_names, results):
            # Handle Pydantic model (EntityCardinalityInfo)
            if hasattr(result_item, 'cardinality'):
                entity_cardinalities[entity_name] = {
                    "cardinality": result_item.cardinality,
                    "table_type": result_item.table_type,
                }
            elif isinstance(result_item, dict):
                entity_info = result_item.get("entity_info", [{}])[0] if result_item.get("entity_info") else {}
                entity_cardinalities[entity_name] = {
                    "cardinality": entity_info.get("cardinality"),
                    "table_type": entity_info.get("table_type"),
                }
            else:
                entity_cardinalities[entity_name] = {
                    "cardinality": None,
                    "table_type": None,
                }
        
        return {
            "entity_cardinalities": entity_cardinalities,
            "current_step": "1.8",
            "previous_answers": {**state.get("previous_answers", {}), "1.8": {"results": results}}
        }
    return node


def _wrap_step_1_76(step_func):
    """Wrap Step 1.76 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.76: Entity vs Attribute Guardrail")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            nl_description=state.get("nl_description"),
            domain=state.get("domain"),
        )

        # Handle Pydantic model
        if hasattr(result, 'entities'):
            filtered_entities = result.entities
            attribute_candidates = [c.model_dump() if hasattr(c, 'model_dump') else c for c in result.attribute_candidates]
            result_dict = result.model_dump()
        else:
            filtered_entities = result.get("entities", [])
            attribute_candidates = result.get("attribute_candidates", [])
            result_dict = result
        
        # Merge: return filtered entities and record removed candidates in metadata
        return {
            "entities": filtered_entities or state.get("entities", []),
            "metadata": {**state.get("metadata", {}), "removed_entity_candidates": attribute_candidates},
            "current_step": "1.76",
            "previous_answers": {**prev_answers, "1.76": result_dict},
        }

    return node


def _wrap_step_1_9(step_func):
    """Wrap Step 1.9 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 1.9: Key Relations Extraction")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {}) or {}
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

        # Include schema connectivity suggestions (Step 1.10) as additional hints to help the
        # relation extractor connect orphan entities. This prevents infinite loops when
        # Step 1.10 keeps reporting orphans.
        suggested_relations = metadata.get("suggested_relations", []) or []
        if suggested_relations:
            mentioned_relations = (mentioned_relations or []) + suggested_relations

        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
            mentioned_relations=mentioned_relations,
        )
        
        # Handle Pydantic model
        if hasattr(result, 'relations'):
            relations = result.relations
        elif isinstance(result, dict):
            relations = result.get("relations", [])
        else:
            relations = []
        
        # Convert RelationInfo objects to dicts
        relation_dicts = [
            r.model_dump() if hasattr(r, "model_dump") else (r.dict() if hasattr(r, "dict") else r)
            for r in relations
        ]
        
        # Validate relation entity names before merging into state
        from NL2DATA.utils.validation.schema_anchored import validate_entity_names
        existing_entities = state.get("entities", [])
        existing_entity_names = {
            e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
            for e in existing_entities
        }
        existing_entity_names = {e for e in existing_entity_names if e}
        
        # Extract entity names from relations and validate
        if existing_entity_names:
            # Build output dict with missing_relations format for validation
            relation_entities = []
            for rel in relation_dicts:
                rel_entities = rel.get("entities", []) if isinstance(rel, dict) else getattr(rel, "entities", [])
                relation_entities.extend(rel_entities)
            
            validation_result = validate_entity_names(
                output={"missing_relations": [{"entities": [e]} for e in set(relation_entities)]},
                allowed_entities=existing_entity_names,
                context="step_1_9_relation_extraction"
            )
            if not validation_result["valid"]:
                logger.warning(
                    f"Step 1.9: Relation entity name validation issues: {validation_result['errors']}. "
                    f"Suggestions: {validation_result['suggestions']}"
                )
        
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
        metadata = state.get("metadata", {}) or {}

        # Guardrail: prevent infinite orphan-connection loops.
        # If Step 1.10 keeps reporting orphans and Step 1.9 doesn't converge, LangGraph may
        # hit its recursion limit. We allow a few iterations, then proceed with warnings.
        max_connectivity_loops = 3
        connectivity_iter = int(metadata.get("schema_connectivity_iterations", 0) or 0) + 1

        result = await invoke_step_checked(
            step_1_10_schema_connectivity,
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
            entity_extraction_result=prev_answers.get("1.4")
        )
        
        # Handle Pydantic model
        if hasattr(result, 'orphan_entities'):
            orphan_entities = result.orphan_entities
            suggested_relations = result.suggested_relations
            # Convert connectivity_status list to dict for backward compatibility
            connectivity_status_list = result.connectivity_status
            connectivity_status_dict = {entry.entity_name: entry.is_connected for entry in connectivity_status_list}
            result_dict = result.model_dump()
        else:
            orphan_entities = result.get("orphan_entities", []) or []
            suggested_relations = result.get("suggested_relations", []) or []
            connectivity_status_dict = result.get("connectivity_status", {})
            result_dict = result
        
        forced_no_orphans = False
        if orphan_entities and connectivity_iter >= max_connectivity_loops:
            forced_no_orphans = True
            orphan_entities = []
            logger.warning(
                f"Step 1.10 connectivity did not converge after {max_connectivity_loops} iteration(s). "
                f"Forcing pipeline to continue with current relations."
            )

        return {
            "current_step": "1.10",
            "previous_answers": {**prev_answers, "1.10": result_dict},
            "metadata": {
                **metadata,
                "schema_connectivity_iterations": connectivity_iter,
                "orphan_entities": orphan_entities,
                "connectivity_status": connectivity_status_dict,
                "suggested_relations": suggested_relations,
                "connectivity_forced_passed": forced_no_orphans,
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
        
        all_entities = state.get("entities", [])
        
        for relation in relations:
            # Create relation ID
            relation_entities = relation.get("entities", []) if isinstance(relation, dict) else getattr(relation, "entities", [])
            relation_id = "+".join(sorted(relation_entities))
            relation_ids.append(relation_id)
            
            tasks.append(
                step_1_11_relation_cardinality_single(
                    relation=relation,
                    entities=all_entities,
                    nl_description=state["nl_description"]
                )
            )
        
        # Execute in parallel
        results = await asyncio.gather(*tasks)
        
        # Update relation cardinalities and merge back into relations
        relation_cardinalities = {}
        updated_relations = []
        
        for i, (relation, relation_id, result_item) in enumerate(zip(relations, relation_ids, results)):
            # Convert relation to dict if needed
            if isinstance(relation, dict):
                relation_dict = relation.copy()
            else:
                relation_dict = relation.model_dump() if hasattr(relation, 'model_dump') else relation.dict() if hasattr(relation, 'dict') else {}
            
            # Extract cardinality data from result
            card_dict = {}
            part_dict = {}
            
            # Handle Pydantic model (RelationCardinalityOutput)
            if hasattr(result_item, 'entity_cardinalities'):
                # Convert lists to dicts for backward compatibility
                card_dict = {entry.entity_name: entry.cardinality for entry in result_item.entity_cardinalities}
                part_dict = {entry.entity_name: entry.participation for entry in result_item.entity_participations}
            elif isinstance(result_item, dict):
                card_dict = result_item.get("entity_cardinalities", {})
                part_dict = result_item.get("entity_participations", {})
            
            # Store in relation_cardinalities dict (for backward compatibility)
            relation_cardinalities[relation_id] = {
                "entity_cardinalities": card_dict,
                "entity_participations": part_dict,
            }
            
            # Merge cardinality data back into relation dict
            relation_dict["entity_cardinalities"] = card_dict if card_dict else None
            relation_dict["entity_participations"] = part_dict if part_dict else None
            
            updated_relations.append(relation_dict)
        
        return {
            "relations": updated_relations,
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
        metadata = state.get("metadata", {}) or {}

        # Guardrail: prevent infinite validation loops.
        # LangGraph can hit recursion limits if a failing step keeps looping without convergence.
        max_validation_loops = 3
        validation_iter = int(metadata.get("relation_validation_iterations", 0) or 0) + 1

        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            relations=state.get("relations", []),
            relation_cardinalities=state.get("relation_cardinalities", {}),
            nl_description=state.get("nl_description"),
            previous_result=prev_answers.get("1.11")  # Pass cardinality result as previous_result
        )

        # Handle Pydantic model
        if hasattr(result, 'validation_passed'):
            validation_passed = result.validation_passed
            result_dict = result.model_dump()
        else:
            validation_passed = bool(result.get("validation_passed", False))
            result_dict = result
        
        forced_pass = False
        if (not validation_passed) and validation_iter >= max_validation_loops:
            forced_pass = True
            logger.warning(
                f"Step 1.12 validation did not converge after {max_validation_loops} iteration(s). "
                f"Forcing pipeline to continue with current relations."
            )
        
        return {
            "current_step": "1.12",
            "previous_answers": {**prev_answers, "1.12": result_dict},
            "metadata": {
                **metadata,
                "relation_validation_iterations": validation_iter,
                "validation_passed": validation_passed or forced_pass,
                "validation_forced_passed": forced_pass,
            }
        }
    return node


def create_phase_1_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 1 (Domain & Entity Discovery).
    
    This graph orchestrates all Phase 1 steps:
    1. Domain Detection & Inference (1.1) - merged from previous 1.1 and 1.3
    2. Entity Mention Detection (1.2)
    3. Key Entity Extraction (1.4)
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
        step_1_4_key_entity_extraction,
        step_1_5_relation_mention_detection,
        step_1_6_auxiliary_entity_suggestion,
        step_1_7_entity_consolidation,
        step_1_76_entity_attribute_guardrail,
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
    workflow.add_node("entity_extraction", _wrap_step_1_4(step_1_4_key_entity_extraction))
    workflow.add_node("relation_mention", _wrap_step_1_5(step_1_5_relation_mention_detection))
    workflow.add_node("auxiliary_entities", _wrap_step_1_6(step_1_6_auxiliary_entity_suggestion))
    workflow.add_node("entity_consolidation", _wrap_step_1_7(step_1_7_entity_consolidation))
    workflow.add_node("entity_attribute_guardrail", _wrap_step_1_76(step_1_76_entity_attribute_guardrail))
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
    workflow.add_edge("entity_mention", "entity_extraction")
    
    # Parallel: Relation mention and auxiliary entities (both depend on entity extraction)
    workflow.add_edge("entity_extraction", "relation_mention")
    workflow.add_edge("entity_extraction", "auxiliary_entities")
    
    # Merge parallel results before consolidation
    workflow.add_edge("relation_mention", "entity_consolidation")
    workflow.add_edge("auxiliary_entities", "entity_consolidation")
    
    # Insert deterministic guardrail between consolidation and reclassification
    workflow.add_edge("entity_consolidation", "entity_attribute_guardrail")
    workflow.add_edge("entity_attribute_guardrail", "entity_reclassification")
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

