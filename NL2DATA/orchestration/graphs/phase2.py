"""Phase 2: Attribute Discovery & Primary Key Identification Graph.

Aligned to the 13-phase pipeline plan (schema foundation track).

Phase 2 is responsible for:
- discovering entity attributes
- reconciling naming / synonyms
- identifying primary keys
- realizing foreign keys for binary relations (so Phase 3 can build PK/FK dependencies)
- extracting relation intrinsic attributes

Derived attributes and constraint inference are deferred to later phases (post-freeze).
"""

from typing import Dict, Any, List, Literal, Tuple
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _naming_validation_passed(state: IRGenerationState) -> Literal["passed", "failed", "max_retries"]:
    """Check if naming validation passed, with max retry limit to prevent infinite loops."""
    metadata = state.get("metadata", {})
    validation_passed = metadata.get("naming_validation_passed", False)
    
    # Track retry count to prevent infinite loops
    naming_validation_retry_count = metadata.get("naming_validation_retry_count", 0)
    max_retries = 5  # Maximum number of retries before giving up
    
    if validation_passed:
        return "passed"
    
    # Check if we've exceeded max retries
    if naming_validation_retry_count >= max_retries:
        logger.error(
            f"Naming validation failed after {max_retries} retries. "
            f"Stopping loop to prevent infinite iteration. "
            f"Please check the naming issues manually."
        )
        return "max_retries"
    
    return "failed"


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
        result = await invoke_step_checked(
            step_func,
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
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            nl_description=state["nl_description"],
            attribute_count_results=prev_answers.get("2.1"),
            domain=state.get("domain"),
            relations=state.get("relations", []),
            primary_keys=state.get("primary_keys", {})
        )
        
        # Normalize to entity -> attribute_list (not the wrapper payload)
        # result is a Pydantic model (IntrinsicAttributesBatchOutput), use attribute access
        entity_results = result.entity_results if hasattr(result, "entity_results") else []
        attributes = {}
        for entity_result in entity_results:
            entity_name = entity_result.entity_name if hasattr(entity_result, "entity_name") else ""
            entity_attrs = entity_result.attributes if hasattr(entity_result, "attributes") else []
            if entity_name:
                attributes[entity_name] = entity_attrs
                # Log warning if entity has no attributes (will fail Phase 2 gate)
                if not entity_attrs:
                    logger.warning(
                        f"Step 2.2: Entity '{entity_name}' has no attributes extracted. "
                        f"This will cause Phase 2 gate to fail. Check entity description and NL description."
                    )
        
        # Validate attribute names and entity-attribute consistency before merging into state
        from NL2DATA.utils.validation.schema_anchored import (
            validate_attribute_names,
            validate_entity_attribute_consistency
        )
        
        # Validate entity-attribute consistency
        entities = state.get("entities", [])
        consistency_errors = validate_entity_attribute_consistency(entities, attributes)
        if consistency_errors:
            logger.warning(
                f"Step 2.2: Entity-attribute consistency issues: {consistency_errors}"
            )
        
        # Validate attribute names (check against existing attributes if any)
        existing_attributes = state.get("attributes", {})
        if existing_attributes:
            schema_state = {
                "entities": entities,
                "attributes": existing_attributes
            }
            # Build output format for validation
            validation_output = {}
            for entity_name, attrs in attributes.items():
                if attrs:
                    validation_output["missing_intrinsic_attributes"] = [
                        {
                            "entity": entity_name,
                            "attribute": attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
                        }
                        for attr in attrs
                    ]
                    break  # Just validate first entity's attributes as example
            
            if validation_output:
                validation_result = validate_attribute_names(
                    output=validation_output,
                    schema_state=schema_state,
                    context="step_2_2_intrinsic_attributes"
                )
                if not validation_result["valid"]:
                    logger.warning(
                        f"Step 2.2: Attribute name validation issues: {validation_result['errors']}. "
                        f"Suggestions: {validation_result['suggestions']}"
                    )
        
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
        
        # Check if we're in a retry loop from naming validation (step 2.6)
        metadata = state.get("metadata", {})
        naming_validation_result = prev_answers.get("2.6")
        retry_count = metadata.get("naming_validation_retry_count", 0)
        
        # If we're retrying due to naming validation failure, log the issues
        if retry_count > 0 and naming_validation_result:
            if hasattr(naming_validation_result, 'naming_conflicts'):
                conflicts = naming_validation_result.naming_conflicts
                violations = naming_validation_result.naming_violations
            elif isinstance(naming_validation_result, dict):
                conflicts = naming_validation_result.get("naming_conflicts", [])
                violations = naming_validation_result.get("naming_violations", [])
            else:
                conflicts = []
                violations = []
            
            if conflicts or violations:
                logger.warning(
                    f"Step 2.3 retry #{retry_count}: Attempting to fix naming issues. "
                    f"Conflicts: {len(conflicts)}, Violations: {len(violations)}"
                )
        
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            entity_attributes=state.get("attributes", {}),  # entity_name -> list[dict]
            nl_description=state["nl_description"]
        )
        
        # Build updated_attributes from final_attribute_list in results
        # result is now AttributeSynonymBatchOutput with entity_results as list
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        updated_attributes = {}
        current_attributes = state.get("attributes", {})
        
        for synonym_result in entity_results_list:
            entity_name = synonym_result.entity_name if hasattr(synonym_result, 'entity_name') else synonym_result.get("entity_name", "")
            if not entity_name:
                continue
            
            final_list = synonym_result.final_attribute_list if hasattr(synonym_result, 'final_attribute_list') else synonym_result.get("final_attribute_list", [])
            
            # Build updated attributes from final list
            from NL2DATA.phases.phase2.step_2_3_attribute_synonym_detection import _build_updated_attributes_from_final_list
            original_attrs = current_attributes.get(entity_name, [])
            updated_attributes[entity_name] = _build_updated_attributes_from_final_list(
                attributes=original_attrs,
                final_attribute_list=final_list,
            )
        
        # Keep attributes that weren't processed
        for entity_name, attrs in current_attributes.items():
            if entity_name not in updated_attributes:
                updated_attributes[entity_name] = attrs
        
        return {
            "attributes": updated_attributes,
            "current_step": "2.3",
            "previous_answers": {**prev_answers, "2.3": result.model_dump() if hasattr(result, 'model_dump') else result}
        }
    return node


def _wrap_step_2_4(step_func):
    """Wrap Step 2.4 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.4: Composite Attribute Handling")
        prev_answers = state.get("previous_answers", {})
        attr_name_lists = _attributes_to_name_lists(state.get("attributes", {}))
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            entity_attributes=attr_name_lists,
            nl_description=state["nl_description"],
        )

        # Build composite_decompositions mapping from step output
        composite_decompositions = {}
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        entities_list = state.get("entities", [])
        
        for i, entity_result in enumerate(entity_results_list):
            entity_name = entities_list[i].get("name", "") if i < len(entities_list) and isinstance(entities_list[i], dict) else ""
            if not entity_name:
                continue
            
            entity_map = {}
            composite_attrs = entity_result.composite_attributes if hasattr(entity_result, 'composite_attributes') else entity_result.get("composite_attributes", [])
            for ca in composite_attrs:
                if not (ca.should_decompose if hasattr(ca, 'should_decompose') else ca.get("should_decompose", False)):
                    continue
                if not (ca.decomposition if hasattr(ca, 'decomposition') else ca.get("decomposition")):
                    continue
                dsls = ca.decomposition_dsls if hasattr(ca, 'decomposition_dsls') else ca.get("decomposition_dsls")
                if not dsls:
                    continue
                
                # Build sub-attribute to DSL mapping
                sub_map = {}
                if isinstance(dsls, list):
                    for dsl_info in dsls:
                        sub_attr = dsl_info.sub_attribute_name if hasattr(dsl_info, 'sub_attribute_name') else dsl_info.get("sub_attribute_name", "")
                        dsl_expr = dsl_info.dsl_expression if hasattr(dsl_info, 'dsl_expression') else dsl_info.get("dsl_expression", "")
                        if sub_attr and dsl_expr:
                            sub_map[sub_attr] = dsl_expr
                elif isinstance(dsls, dict):
                    sub_map = dsls
                
                if sub_map:
                    attr_name = ca.name if hasattr(ca, 'name') else ca.get("name", "")
                    if attr_name:
                        entity_map[attr_name] = sub_map
            
            if entity_map:
                composite_decompositions[entity_name] = entity_map
        
        # Composite handling currently produces recommendations; keep attributes unchanged.
        return {
            "current_step": "2.4",
            "previous_answers": {**prev_answers, "2.4": result.model_dump() if hasattr(result, 'model_dump') else result},
            "composite_decompositions": composite_decompositions,
        }
    return node


def _wrap_step_2_5(step_func):
    """Wrap Step 2.5 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.5: Temporal Attributes Detection")
        prev_answers = state.get("previous_answers", {})
        attr_name_lists = _attributes_to_name_lists(state.get("attributes", {}))
        result = await invoke_step_checked(
            step_func,
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
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            entity_attributes=_attributes_to_name_lists(state.get("attributes", {}))
        )
        
        # Handle Pydantic model result
        if hasattr(result, 'validation_passed'):
            validation_passed = result.validation_passed
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
        elif isinstance(result, dict):
            validation_passed = result.get("validation_passed", False)
            result_dict = result
        else:
            validation_passed = False
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
        
        # Increment retry count if validation failed
        current_metadata = state.get("metadata", {})
        retry_count = current_metadata.get("naming_validation_retry_count", 0)
        if not validation_passed:
            retry_count += 1
            logger.warning(
                f"Naming validation failed (attempt {retry_count}). "
                f"Looping back to synonym detection to fix naming issues."
            )
        else:
            # Reset retry count on success
            retry_count = 0
        
        return {
            "current_step": "2.6",
            "previous_answers": {**prev_answers, "2.6": result_dict},
            "metadata": {
                **current_metadata,
                "naming_validation_passed": validation_passed,
                "naming_validation_retry_count": retry_count
            }
        }
    return node


def _wrap_step_2_7(step_func):
    """Wrap Step 2.7 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.7: Primary Key Identification")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            entity_attributes=_attributes_to_name_lists(state.get("attributes", {}))
        )
        
        # Extract primary keys: entity_results is now a list of EntityPrimaryKeyResult objects
        # We need to convert to Dict[str, List[str]] for state
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        primary_keys: Dict[str, List[str]] = {}
        attributes = state.get("attributes", {})
        updated_attributes = {**attributes}  # Copy to avoid mutating state directly
        
        # Check if we need to add surrogate keys to attributes
        for pk_result in entity_results_list:
            entity_name = pk_result.entity_name if hasattr(pk_result, 'entity_name') else pk_result.get("entity_name", "")
            if not entity_name:
                continue
            
            # Extract the "primary_key" list from the result
            pk_list = pk_result.primary_key if hasattr(pk_result, 'primary_key') else pk_result.get("primary_key", [])
            if pk_list:
                primary_keys[entity_name] = pk_list
                
                # Check if any PK attributes are missing from attributes state (surrogate keys)
                entity_attrs = updated_attributes.get(entity_name, [])
                existing_attr_names = {a.get("name", "").lower() if isinstance(a, dict) else getattr(a, "name", "").lower() for a in entity_attrs}
                
                for pk_attr in pk_list:
                    if pk_attr.lower() not in existing_attr_names:
                        # This is a surrogate key that needs to be added
                        logger.info(f"Step 2.7: Adding surrogate key '{pk_attr}' to entity '{entity_name}' attributes")
                        if entity_name not in updated_attributes:
                            updated_attributes[entity_name] = []
                        
                        # Add the surrogate key attribute
                        updated_attributes[entity_name].append({
                            "name": pk_attr,
                            "description": f"Surrogate primary key for {entity_name}",
                            "type_hint": "integer",
                            "reasoning": "Auto-generated surrogate key for primary key identification"
                        })
        
        return {
            "primary_keys": primary_keys,
            "attributes": updated_attributes,  # Include any added surrogate keys
            "current_step": "2.7",
            "previous_answers": {**prev_answers, "2.7": result.model_dump() if hasattr(result, 'model_dump') else result}
        }
    return node


def _wrap_step_2_16(step_func):
    """Wrap Step 2.16 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.16: Cross-Entity Attribute Reconciliation")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            relations=state.get("relations", []),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )
        
        # Build updated_attributes from entity_results
        # result is now CrossEntityReconciliationBatchOutput with entity_results as list
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        updated_attributes = {}
        current_attributes = state.get("attributes", {})
        
        for recon_result in entity_results_list:
            entity_name = recon_result.entity_name if hasattr(recon_result, 'entity_name') else recon_result.get("entity_name", "")
            if not entity_name:
                continue
            
            # Convert AttributeInfo objects to dicts for state
            attrs = recon_result.attributes if hasattr(recon_result, 'attributes') else recon_result.get("attributes", [])
            updated_attributes[entity_name] = [
                attr.model_dump() if hasattr(attr, 'model_dump') else (attr if isinstance(attr, dict) else {"name": str(attr)})
                for attr in attrs
            ]
        
        # Keep attributes that weren't processed
        for entity_name, attrs in current_attributes.items():
            if entity_name not in updated_attributes:
                updated_attributes[entity_name] = attrs
        
        return {
            "attributes": updated_attributes,
            "current_step": "2.16",
            "previous_answers": {**prev_answers, "2.16": result.model_dump() if hasattr(result, 'model_dump') else result},
        }
    return node


def _wrap_step_2_8(step_func):
    """Wrap Step 2.8 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.8: Multivalued/Derived Detection")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            entity_attributes=_attributes_to_name_lists(state.get("attributes", {})),
            primary_keys=state.get("primary_keys", {}),
            nl_description=state["nl_description"],
            domain=state.get("domain"),
        )
        
        # Extract multivalued/derived results
        # result is now MultivaluedDerivedBatchOutput with entity_results as list
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        multivalued_derived = {}
        entity_derived_attributes = {}
        
        for entity_result in entity_results_list:
            entity_name = entity_result.entity_name if hasattr(entity_result, 'entity_name') else entity_result.get("entity_name", "")
            if not entity_name:
                continue
            
            # Convert to dict format for state compatibility
            mv_result_dict = {
                "multivalued": entity_result.multivalued if hasattr(entity_result, 'multivalued') else entity_result.get("multivalued", []),
                "derived": entity_result.derived if hasattr(entity_result, 'derived') else entity_result.get("derived", []),
                "multivalued_handling": [
                    {
                        "attribute_name": h.attribute_name if hasattr(h, 'attribute_name') else h.get("attribute_name", ""),
                        "strategy": h.strategy if hasattr(h, 'strategy') else h.get("strategy", "")
                    }
                    for h in (entity_result.multivalued_handling if hasattr(entity_result, 'multivalued_handling') else entity_result.get("multivalued_handling", []))
                ],
                "reasoning": entity_result.reasoning if hasattr(entity_result, 'reasoning') else entity_result.get("reasoning", ""),
            }
            
            multivalued_derived[entity_name] = mv_result_dict
            derived = mv_result_dict.get("derived", [])
            if derived:
                entity_derived_attributes[entity_name] = derived
        
        return {
            "multivalued_derived": multivalued_derived,
            "entity_derived_attributes": entity_derived_attributes,
            "current_step": "2.8",
            "previous_answers": {**prev_answers, "2.8": result.model_dump() if hasattr(result, 'model_dump') else result},
        }
    return node


def _wrap_step_2_9(step_func):
    """Wrap Step 2.9 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.9: Derived Attribute Formulas")
        prev_answers = state.get("previous_answers", {})
        entity_derived_attributes = state.get("entity_derived_attributes", {})
        
        if not entity_derived_attributes:
            logger.info("No derived attributes found, skipping formula generation")
            return {
                "derived_formulas": {},
                "current_step": "2.9",
                "previous_answers": {**prev_answers, "2.9": {"entity_results": {}}},
            }
        
        result = await invoke_step_checked(
            step_func,
            entity_derived_attributes=entity_derived_attributes,
            entity_attributes=state.get("attributes", {}),
            entity_descriptions={e.get("name", ""): e.get("description", "") for e in state.get("entities", [])},
            derivation_rules=state.get("seeded_derivation_rules"),
            nl_description=state.get("nl_description", ""),
        )
        
        # Store derived formulas in state
        # result is now DerivedFormulaBatchOutput with entity_results as list
        entity_results_list = result.entity_results if hasattr(result, 'entity_results') else result.get("entity_results", [])
        derived_formulas = {}
        for formula_result in entity_results_list:
            entity_name = formula_result.entity_name if hasattr(formula_result, 'entity_name') else formula_result.get("entity_name", "")
            attr_name = formula_result.attribute_name if hasattr(formula_result, 'attribute_name') else formula_result.get("attribute_name", "")
            if not entity_name or not attr_name:
                continue
            
            key = f"{entity_name}.{attr_name}"
            derived_formulas[key] = {
                "formula": formula_result.formula if hasattr(formula_result, 'formula') else formula_result.get("formula", ""),
                "dependencies": formula_result.dependencies if hasattr(formula_result, 'dependencies') else formula_result.get("dependencies", []),
                "formula_type": formula_result.formula_type if hasattr(formula_result, 'formula_type') else formula_result.get("formula_type", ""),
                "reasoning": formula_result.reasoning if hasattr(formula_result, 'reasoning') else formula_result.get("reasoning", ""),
            }
        
        return {
            "derived_formulas": derived_formulas,
            "current_step": "2.9",
            "previous_answers": {**prev_answers, "2.9": result.model_dump() if hasattr(result, 'model_dump') else result},
        }
    return node


def _wrap_step_2_15(step_func):
    """Wrap Step 2.15 to work as LangGraph node."""
    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 2.15: Relation Intrinsic Attributes")
        prev_answers = state.get("previous_answers", {})
        result = await invoke_step_checked(
            step_func,
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
    """Create LangGraph StateGraph for Phase 2 (Attribute Discovery & PK/FK foundation).
    
    This graph orchestrates all Phase 2 steps:
    1. Attribute Count Detection (2.1) - parallel per entity
    2. Intrinsic Attributes (2.2) - parallel per entity
    3. Attribute Synonym Detection (2.3) - parallel per entity
    4. Cross-Entity Attribute Reconciliation (2.16) - parallel per entity
    5. Composite Attribute Handling (2.4) - parallel per entity
    6. Temporal Attributes Detection (2.5) - parallel per entity
    7. Naming Convention Validation (2.6) - loop if validation fails
    8. Primary Key Identification (2.7) - parallel per entity
    9. Multivalued/Derived Detection (2.8) - parallel per entity (classifies existing attributes only)
    10. Derived Attribute Formulas (2.9) - parallel per derived attribute (same-entity dependencies only)
    11. Relation Intrinsic Attributes (2.15) - parallel per relation
    
    Note: Foreign keys are created deterministically in Phase 4 from relations and cardinalities.
    
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
    )
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
    
    # Conditional: Loop back if naming validation fails (with max retry limit)
    workflow.add_conditional_edges(
        "naming_validation",
        _naming_validation_passed,
        {
            "passed": "primary_keys",
            "failed": "synonym_detection",  # Loop back to fix naming issues
            "max_retries": "primary_keys"  # Continue despite failures after max retries (log error but don't block)
        }
    )
    
    workflow.add_edge("primary_keys", "multivalued_derived")
    workflow.add_edge("multivalued_derived", "derived_formulas")
    workflow.add_edge("derived_formulas", "relation_attributes")
    workflow.add_edge("relation_attributes", END)
    
    # Compile with checkpointing
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

