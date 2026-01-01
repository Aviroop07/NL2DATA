"""Phase 3, Step 3.3: Phase 2 Steps with Enhanced Context.

Re-run relevant Phase 2 steps (attribute discovery, primary keys, constraints)
with enhanced context from Phase 3 information needs.
Ensures attributes needed for queries are properly defined.
"""

from typing import Dict, Any, List, Optional
import asyncio

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.observability import traceable_step
from NL2DATA.phases.phase1.utils.data_extraction import extract_attribute_name
from NL2DATA.phases.phase2 import (
    step_2_2_intrinsic_attributes_batch,
    step_2_7_primary_key_identification_batch,
    step_2_9_derived_attribute_formulas_batch,
)

logger = get_logger(__name__)


@traceable_step("3.3", phase=3, tags=['phase_3_step_3'])
async def step_3_3_phase2_reexecution(
    entities: List[Dict[str, Any]],  # All entities from Phase 1
    relations: List[Dict[str, Any]],  # All relations from Phase 1
    attributes: Dict[str, List[Dict[str, Any]]],  # Current attributes from Phase 2
    primary_keys: Dict[str, List[str]],  # Current primary keys from Phase 2
    information_needs: List[Dict[str, Any]],  # Information needs from Step 3.1
    completeness_results: Dict[str, Dict[str, Any]],  # Completeness results from Step 3.2
    nl_description: str,
    domain: Optional[str] = None,
    entity_attributes: Optional[Dict[str, List[str]]] = None,  # entity -> attribute names (strings)
) -> Dict[str, Any]:
    """
    Step 3.3: Re-run relevant Phase 2 steps with enhanced context from Phase 3.
    
    This step identifies missing attributes/components from completeness checks and
    re-runs Phase 2 steps to add query-driven attributes.
    
    Args:
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Current attributes from Phase 2 (entity -> List[AttributeInfo])
        primary_keys: Current primary keys from Phase 2
        information_needs: Information needs from Step 3.1
        completeness_results: Completeness check results from Step 3.2
        nl_description: Original natural language description
        domain: Optional domain context from Phase 1
        entity_attributes: Optional dictionary mapping entity names to attribute name lists (strings)
        
    Returns:
        dict: Results from re-executed Phase 2 steps with any new attributes/components
        
    Example:
        >>> result = await step_3_3_phase2_reexecution(
        ...     entities=[{"name": "Order"}],
        ...     relations=[],
        ...     attributes={"Order": []},
        ...     primary_keys={"Order": ["order_id"]},
        ...     information_needs=[{"description": "List orders by date"}],
        ...     completeness_results={},
        ...     nl_description="E-commerce database"
        ... )
        >>> "new_attributes" in result
        True
    """
    logger.info("Starting Step 3.3: Phase 2 Steps with Enhanced Context")
    
    # Collect missing components from completeness results
    # Handle new format: missing_intrinsic_attributes and missing_derived_attributes
    missing_intrinsic_attrs_by_entity: Dict[str, List[Dict[str, Any]]] = {}
    missing_derived_attrs_by_entity: Dict[str, List[Dict[str, Any]]] = {}
    entities_needing_attributes: List[str] = []
    
    for info_id, completeness_result in completeness_results.items():
        missing_entities = completeness_result.get("missing_entities", [])
        missing_intrinsic_attrs = completeness_result.get("missing_intrinsic_attributes", [])
        missing_derived_attrs = completeness_result.get("missing_derived_attributes", [])
        
        # Legacy support: check for old format "missing_attributes" if new format is empty
        if not missing_intrinsic_attrs and not missing_derived_attrs:
            missing_attrs = completeness_result.get("missing_attributes", [])
            if missing_attrs:
                logger.warning(
                    f"Information need '{info_id}' uses legacy 'missing_attributes' format. "
                    f"Please update to use 'missing_intrinsic_attributes' and 'missing_derived_attributes'."
                )
                # Convert legacy format to intrinsic attributes
                for missing_attr in missing_attrs:
                    entity_name = missing_attr.get("entity", "")
                    attr_name = missing_attr.get("attribute", "")
                    if entity_name and attr_name:
                        if entity_name not in missing_intrinsic_attrs_by_entity:
                            missing_intrinsic_attrs_by_entity[entity_name] = []
                        missing_intrinsic_attrs_by_entity[entity_name].append({
                            "entity": entity_name,
                            "attribute": attr_name,
                            "reasoning": missing_attr.get("reasoning", "")
                        })
                        if entity_name not in entities_needing_attributes:
                            entities_needing_attributes.append(entity_name)
        
        # Track missing entities (these would need to be added in Phase 1, but we note them here)
        if missing_entities:
            logger.warning(
                f"Information need '{info_id}' requires missing entities: {', '.join(missing_entities)}. "
                f"These should be added in Phase 1, not Phase 2."
            )
        
        # Track missing intrinsic attributes by entity
        for missing_attr in missing_intrinsic_attrs:
            # Handle both dict and Pydantic model formats
            if isinstance(missing_attr, dict):
                entity_name = missing_attr.get("entity", "")
                attr_name = missing_attr.get("attribute", "")
                reasoning = missing_attr.get("reasoning", "")
            else:
                entity_name = getattr(missing_attr, "entity", "")
                attr_name = getattr(missing_attr, "attribute", "")
                reasoning = getattr(missing_attr, "reasoning", "")
            
            if entity_name and attr_name:
                if entity_name not in missing_intrinsic_attrs_by_entity:
                    missing_intrinsic_attrs_by_entity[entity_name] = []
                # Check if already added (avoid duplicates)
                existing = next(
                    (a for a in missing_intrinsic_attrs_by_entity[entity_name] 
                     if (a.get("attribute") if isinstance(a, dict) else getattr(a, "attribute", "")) == attr_name),
                    None
                )
                if not existing:
                    missing_intrinsic_attrs_by_entity[entity_name].append({
                        "entity": entity_name,
                        "attribute": attr_name,
                        "reasoning": reasoning
                    })
                    if entity_name not in entities_needing_attributes:
                        entities_needing_attributes.append(entity_name)
        
        # Track missing derived attributes by entity
        for missing_attr in missing_derived_attrs:
            # Handle both dict and Pydantic model formats
            if isinstance(missing_attr, dict):
                entity_name = missing_attr.get("entity", "")
                attr_name = missing_attr.get("attribute", "")
                derivation_hint = missing_attr.get("derivation_hint", "")
                reasoning = missing_attr.get("reasoning", "")
            else:
                entity_name = getattr(missing_attr, "entity", "")
                attr_name = getattr(missing_attr, "attribute", "")
                derivation_hint = getattr(missing_attr, "derivation_hint", "")
                reasoning = getattr(missing_attr, "reasoning", "")
            
            if entity_name and attr_name:
                if entity_name not in missing_derived_attrs_by_entity:
                    missing_derived_attrs_by_entity[entity_name] = []
                # Check if already added (avoid duplicates)
                existing = next(
                    (a for a in missing_derived_attrs_by_entity[entity_name] 
                     if (a.get("attribute") if isinstance(a, dict) else getattr(a, "attribute", "")) == attr_name),
                    None
                )
                if not existing:
                    missing_derived_attrs_by_entity[entity_name].append({
                        "entity": entity_name,
                        "attribute": attr_name,
                        "derivation_hint": derivation_hint,
                        "reasoning": reasoning
                    })
                    if entity_name not in entities_needing_attributes:
                        entities_needing_attributes.append(entity_name)
    
    if not entities_needing_attributes:
        logger.info("No missing attributes identified - no Phase 2 re-execution needed")
        return {
            "new_attributes": {},
            "new_derived_attributes": {},
            "updated_primary_keys": {},
            "updated_constraints": {},
            "summary": "No missing components identified, no re-execution performed"
        }
    
    total_intrinsic = sum(len(attrs) for attrs in missing_intrinsic_attrs_by_entity.values())
    total_derived = sum(len(attrs) for attrs in missing_derived_attrs_by_entity.values())
    logger.info(
        f"Re-executing Phase 2 steps for {len(entities_needing_attributes)} entities: "
        f"{total_intrinsic} missing intrinsic attributes, {total_derived} missing derived attributes"
    )
    
    # Build enhanced context for information needs
    info_needs_summary = []
    for info_need in information_needs:
        info_desc = info_need.get("description", "") if isinstance(info_need, dict) else getattr(info_need, "description", "")
        info_entities = info_need.get("entities_involved", []) if isinstance(info_need, dict) else getattr(info_need, "entities_involved", [])
        if info_desc:
            info_summary = f"  - {info_desc}"
            if info_entities:
                info_summary += f" (entities: {', '.join(info_entities)})"
            info_needs_summary.append(info_summary)
    
    enhanced_context = f"\n\nInformation Needs Context:\n" + "\n".join(info_needs_summary)
    enhanced_nl_description = nl_description + enhanced_context
    
    new_attributes = {}
    new_derived_attributes = {}
    new_derived_metrics = {}
    updated_primary_keys = {}
    updated_constraints = {}
    
    # Step 1: Handle missing intrinsic attributes
    entities_needing_intrinsic = [
        e_name for e_name in entities_needing_attributes
        if e_name in missing_intrinsic_attrs_by_entity
    ]
    
    if entities_needing_intrinsic:
        entities_to_process_intrinsic = [
            e for e in entities
            if (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) in entities_needing_intrinsic
        ]
        
        if entities_to_process_intrinsic:
            # Re-run attribute discovery for entities with missing intrinsic attributes
            logger.info(f"Re-running intrinsic attribute discovery for {len(entities_to_process_intrinsic)} entities")
            attr_result = await step_2_2_intrinsic_attributes_batch(
                entities=entities_to_process_intrinsic,
                nl_description=enhanced_nl_description,
                domain=domain,
            )
            
            entity_results = attr_result.get("entity_results", {})
            for entity_name, entity_attr_result in entity_results.items():
                if entity_name in entities_needing_intrinsic:
                    new_attrs = entity_attr_result.get("attributes", [])
                    if new_attrs:
                        # Merge with existing attributes (avoid duplicates)
                        existing_attr_names = {
                            extract_attribute_name(attr)
                            for attr in attributes.get(entity_name, [])
                        }
                        truly_new = [
                            attr for attr in new_attrs
                            if extract_attribute_name(attr) not in existing_attr_names
                        ]
                        if truly_new:
                            new_attributes[entity_name] = truly_new
                            logger.info(f"Added {len(truly_new)} new intrinsic attributes to {entity_name}")
    
    # Step 2: Handle missing derived attributes
    entities_needing_derived = [
        e_name for e_name in entities_needing_attributes
        if e_name in missing_derived_attrs_by_entity
    ]
    
    if entities_needing_derived:
        logger.info(f"Adding {sum(len(attrs) for attrs in missing_derived_attrs_by_entity.values())} derived attributes")
        
        # Build entity_derived_attributes dict for Step 2.9
        entity_derived_attributes: Dict[str, List[str]] = {}
        entity_attributes_for_derived: Dict[str, List[str]] = {}
        derivation_rules: Dict[str, Dict[str, str]] = {}
        entity_descriptions: Dict[str, str] = {}
        
        for entity_name in entities_needing_derived:
            derived_attrs_info = missing_derived_attrs_by_entity[entity_name]
            derived_attr_names = [attr_info.get("attribute") if isinstance(attr_info, dict) else getattr(attr_info, "attribute", "") 
                                  for attr_info in derived_attrs_info]
            
            entity_derived_attributes[entity_name] = derived_attr_names
            
            # Get all attributes for this entity (existing + new intrinsic)
            existing_attrs = attributes.get(entity_name, [])
            new_intrinsic = new_attributes.get(entity_name, [])
            all_attrs = existing_attrs + new_intrinsic
            # IMPORTANT: Step 2.9 currently requires the derived attribute itself to be present in the attribute list.
            # Include the derived attribute names so formula extraction can proceed.
            base_attr_names = [extract_attribute_name(attr) for attr in all_attrs]
            entity_attributes_for_derived[entity_name] = sorted(set(base_attr_names + [a for a in derived_attr_names if a]))
            
            # Build derivation_rules from derivation_hint
            entity_derivation_rules = {}
            for attr_info in derived_attrs_info:
                if isinstance(attr_info, dict):
                    attr_name = attr_info.get("attribute", "")
                    derivation_hint = attr_info.get("derivation_hint", "")
                else:
                    attr_name = getattr(attr_info, "attribute", "")
                    derivation_hint = getattr(attr_info, "derivation_hint", "")
                if attr_name and derivation_hint:
                    entity_derivation_rules[attr_name] = derivation_hint
            derivation_rules[entity_name] = entity_derivation_rules
            
            # Get entity description
            entity_obj = next(
                (e for e in entities if (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name),
                None
            )
            if entity_obj:
                entity_desc = entity_obj.get("description", "") if isinstance(entity_obj, dict) else getattr(entity_obj, "description", "")
                if entity_desc:
                    entity_descriptions[entity_name] = entity_desc
        
        # Call Step 2.9 to get formulas for derived attributes
        derived_formulas_result = await step_2_9_derived_attribute_formulas_batch(
            entity_derived_attributes=entity_derived_attributes,
            entity_attributes=entity_attributes_for_derived,
            entity_descriptions=entity_descriptions,
            derivation_rules=derivation_rules,
            nl_description=enhanced_nl_description,
        )
        
        # Build derived attributes with formulas
        derived_formulas_by_entity = derived_formulas_result.get("entity_results", {})
        for entity_name, derived_formulas in derived_formulas_by_entity.items():
            if entity_name in entities_needing_derived:
                derived_attrs_list = []
                metrics_list = []
                for attr_name, formula_info in derived_formulas.items():
                    if isinstance(formula_info, dict) and bool(formula_info.get("is_aggregate_metric", False)):
                        metrics_list.append(
                            {
                                "name": attr_name,
                                "description": f"Derived metric: {formula_info.get('reasoning', '')}",
                                "is_metric": True,
                                "derivation_formula": formula_info.get("formula", ""),
                                "derivation_expression_type": formula_info.get("expression_type", "other"),
                                "derivation_dependencies": formula_info.get("dependencies", []),
                            }
                        )
                        continue
                    # Do not add derived attributes without valid, non-empty formulas.
                    if not isinstance(formula_info, dict):
                        continue
                    if (formula_info.get("validation_errors") or []) and isinstance(formula_info.get("validation_errors"), list):
                        continue
                    formula_str = str(formula_info.get("formula", "") or "").strip()
                    if not formula_str:
                        continue
                    # Create derived attribute dict
                    derived_attr = {
                        "name": attr_name,
                        "description": f"Derived attribute: {formula_info.get('reasoning', '')}",
                        "is_derived": True,
                        "derivation_formula": formula_info.get("formula", ""),
                        "derivation_expression_type": formula_info.get("expression_type", "other"),
                        "derivation_dependencies": formula_info.get("dependencies", []),
                    }
                    derived_attrs_list.append(derived_attr)
                
                if derived_attrs_list:
                    new_derived_attributes[entity_name] = derived_attrs_list
                    logger.info(f"Added {len(derived_attrs_list)} derived attributes to {entity_name}")
                if metrics_list:
                    new_derived_metrics[entity_name] = metrics_list
                    logger.info(f"Stored {len(metrics_list)} derived metrics for {entity_name} (not added to ER attributes)")
    
    # Step 3: Re-run primary key identification if needed
    # (Only if entity doesn't have a primary key yet)
    entities_needing_pk = [
        e_name for e_name in entities_needing_attributes
        if not primary_keys.get(e_name) or len(primary_keys.get(e_name, [])) == 0
    ]
    
    if entities_needing_pk:
        logger.info(f"Re-running primary key identification for {len(entities_needing_pk)} entities")
        pk_entities = [
            e for e in entities
            if (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) in entities_needing_pk
        ]
        
        # Build attribute lists for PK identification
        entity_attr_lists = {}
        for entity_name in entities_needing_pk:
            # Combine existing, new intrinsic, and new derived attributes
            existing = attributes.get(entity_name, [])
            new_intrinsic = new_attributes.get(entity_name, [])
            new_derived = new_derived_attributes.get(entity_name, [])
            all_attrs = existing + new_intrinsic + new_derived
            # Convert to string list for PK step
            attr_names = [extract_attribute_name(attr) for attr in all_attrs]
            entity_attr_lists[entity_name] = attr_names
        
        pk_result = await step_2_7_primary_key_identification_batch(
            entities=pk_entities,
            entity_attributes=entity_attr_lists,
            nl_description=enhanced_nl_description,
            domain=domain,
        )
        
        pk_results = pk_result.get("entity_results", {})
        for entity_name, pk_info in pk_results.items():
            if entity_name in entities_needing_pk:
                pk_attrs = pk_info.get("primary_key", [])
                if pk_attrs:
                    updated_primary_keys[entity_name] = pk_attrs
                    logger.info(f"Identified primary key for {entity_name}: {', '.join(pk_attrs)}")
    
    # Re-run constraint steps if needed (optional - can be skipped for now)
    # These would be: unique constraints, nullability, check constraints
    # For now, we'll skip these as they're less critical for query support
    
    summary = (
        f"Phase 2 re-execution completed: {len(new_attributes)} entities with new intrinsic attributes, "
        f"{len(new_derived_attributes)} entities with new derived attributes, "
        f"{len(updated_primary_keys)} entities with updated primary keys"
    )
    logger.info(summary)
    
    return {
        "new_attributes": new_attributes,
        "new_derived_attributes": new_derived_attributes,
        "new_derived_metrics": new_derived_metrics,
        "updated_primary_keys": updated_primary_keys,
        "updated_constraints": updated_constraints,
        "summary": summary
    }

