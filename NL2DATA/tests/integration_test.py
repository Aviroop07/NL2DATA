"""Integration test for NL2DATA system - tests Phases 1, 2, 3, 4, 5, 6, and 7 with a single NL description."""

import asyncio
import sys
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NL2DATA.phases.phase1 import (
    step_1_1_domain_detection,
    step_1_2_entity_mention_detection,
    step_1_3_domain_inference,
    step_1_4_key_entity_extraction,
    step_1_5_relation_mention_detection,
    step_1_6_auxiliary_entity_suggestion,
    step_1_7_entity_consolidation,
    step_1_75_entity_relation_reclassification,
    step_1_76_entity_attribute_guardrail,
    step_1_8_entity_cardinality,
    step_1_9_key_relations_extraction,
    step_1_10_schema_connectivity_with_loop,
    step_1_11_relation_cardinality,
    step_1_12_relation_validation_with_loop,
)
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
from NL2DATA.phases.phase3 import (
    step_3_1_information_need_identification_with_loop,
    step_3_2_information_completeness_batch,
    step_3_3_phase2_reexecution,
    step_3_4_er_design_compilation,
    step_3_45_junction_table_naming,
    step_3_5_relational_schema_compilation,
)
from NL2DATA.phases.phase4 import (
    step_4_1_functional_dependency_analysis_batch,
    step_4_2_3nf_normalization,
    step_4_3_data_type_assignment_batch,
    step_4_4_categorical_detection_batch,
    step_4_5_check_constraint_detection_batch,
    step_4_6_categorical_value_extraction_batch,
    step_4_7_categorical_distribution_batch,
)
from NL2DATA.phases.phase5 import (
    step_5_1_ddl_compilation,
    step_5_2_ddl_validation,
    step_5_3_ddl_error_correction,
    step_5_4_schema_creation,
    step_5_5_sql_query_generation_batch,
)
from NL2DATA.phases.phase6 import (
    step_6_1_constraint_detection_with_loop,
    step_6_2_constraint_scope_analysis_batch,
    step_6_3_constraint_enforcement_strategy_batch,
    step_6_4_constraint_conflict_detection,
    step_6_5_constraint_compilation,
)
from NL2DATA.phases.phase7 import (
    step_7_1_numerical_range_definition_batch,
    step_7_2_text_generation_strategy_batch,
    step_7_3_boolean_dependency_analysis_batch,
    step_7_4_data_volume_specifications,
    step_7_5_partitioning_strategy_batch,
    step_7_6_distribution_compilation,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config
from NL2DATA.utils.rate_limiting.singleton import reset_rate_limiter
from NL2DATA.tests.utils.debug_dump import log_json
from NL2DATA.tests.utils.phase_artifact_dump import dump_phase_artifacts
from NL2DATA.utils.fd import seed_functional_dependencies_from_derived_formulas
from NL2DATA.tests.utils.phase_timing import timer_start, timer_elapsed_seconds, log_phase_duration
from NL2DATA.utils.pipeline_config import get_phase3_config

def read_nl_descriptions(file_path: str) -> List[str]:
    """Read all NL descriptions from a text file, separated by double newlines."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by double newlines (blank lines)
    descriptions = [desc.strip() for desc in content.split('\n\n') if desc.strip()]
    return descriptions

async def test_phases_1_2_3_4_5_6_7_integration(
    nl_description: str = None,
    description_index: int = None,
    max_phase: int = None,
    log_file_override: str = None,
    return_state: bool = False,
):
    """Test Phases 1, 2, 3, 4, 5, 6, and 7 with a single NL description.
    
    Args:
        nl_description: Natural language description to process
        description_index: Optional index for logging purposes
        max_phase: Maximum phase to run (1-7). If None, runs all phases.
    """
    # Use default description if none provided
    if nl_description is None:
        nl_description = """Create an IoT telemetry dataset for 10,000 industrial sensors deployed across 100 plants. There should be one high-frequency fact table called sensor_reading with at least 200 million rows over a 30-day period, plus dimension tables for sensors, plants, and sensor types. Sensor readings (temperature, vibration, current) should mostly remain within normal operating bands that differ by sensor type, with rare anomalies (0.1–0.5% of readings) modeled as spikes, drifts, or sudden step changes. Inject 3–5 "cascading failure" incidents in which a single plant experiences coordinated anomalies across many sensors in a narrow time window, and each incident is preceded by subtle early-warning deviations. Timestamps should be approximately uniform over time but with random missing intervals per sensor to simulate connectivity issues. Include synthetic maintenance events that reset some sensors' behavior. The data should stress time-series joins, anomaly detection queries, and "before/after incident" window aggregations."""
    
    logger = get_logger(__name__)
    log_config = get_config('logging')
    
    # Use the same log file for all descriptions, unless overridden by caller
    log_file = log_file_override or log_config.get('log_file')
    
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=log_config['log_to_file'],
        log_file=log_file,
        clear_existing=(description_index is None or description_index == 1),  # Clear for first description only
    )
    
    index_prefix = f"[Description {description_index}] " if description_index is not None else ""
    print("=" * 80)
    print(f"{index_prefix}Integration Test: Phases 1, 2, 3, 4, 5, 6, and 7")
    print("=" * 80)
    
    def _ascii_safe(s: object) -> str:
        """Best-effort ASCII-safe string for Windows consoles (no crashes)."""
        try:
            txt = str(s or "")
        except Exception:
            txt = ""
        txt = txt.replace('\u2192', '->').replace('\u00d7', 'x')
        try:
            return txt.encode("ascii", errors="replace").decode("ascii")
        except Exception:
            return str(txt)

    # Replace Unicode characters for Windows console compatibility.
    nl_description_display = _ascii_safe(nl_description)
    print(f"\nNatural Language Description:\n{nl_description_display}\n")
    
    all_passed = True
    state = {}  # Accumulate state across phases
    
    try:
        # ========================================================================
        # PHASE 1: Domain & Entity Discovery
        # ========================================================================
        phase_1_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 1: Domain & Entity Discovery")
        print("=" * 80)
        
        # Step 1.1: Domain Detection
        print("\n[Phase 1.1] Domain Detection...")
        result_1_1 = await step_1_1_domain_detection(nl_description)
        log_json(logger, "Phase 1.1 Domain Detection result", result_1_1)
        domain = result_1_1.get("domain", "")
        state["domain"] = domain
        print(f"  Domain: {domain}")
        
        # Step 1.2: Entity Mention Detection
        print("\n[Phase 1.2] Entity Mention Detection...")
        result_1_2 = await step_1_2_entity_mention_detection(nl_description)
        log_json(logger, "Phase 1.2 Entity Mention Detection result", result_1_2)
        mentioned_entities = result_1_2.get("mentioned_entities", [])
        state["mentioned_entities"] = mentioned_entities
        try:
            mentioned_entity_names = [e.get("name", "") for e in mentioned_entities if isinstance(e, dict)]
        except Exception:
            mentioned_entity_names = []
        log_json(logger, "Phase 1.2 mentioned_entity_names (derived)", mentioned_entity_names)
        print(f"  Mentioned entities: {', '.join([n for n in mentioned_entity_names if n])}")
        
        # Step 1.3: Domain Inference
        print("\n[Phase 1.3] Domain Inference...")
        result_1_3 = await step_1_3_domain_inference(nl_description, domain_detection_result=result_1_1)
        log_json(logger, "Phase 1.3 Domain Inference result", result_1_3)
        inferred_domain = result_1_3.get("primary_domain", domain)
        state["domain"] = inferred_domain
        print(f"  Inferred domain: {inferred_domain}")
        
        # Step 1.4: Key Entity Extraction
        print("\n[Phase 1.4] Key Entity Extraction...")
        result_1_4 = await step_1_4_key_entity_extraction(
            nl_description,
            domain=inferred_domain,
            mentioned_entities=mentioned_entity_names,
            entity_mention_result=result_1_2,
        )
        log_json(logger, "Phase 1.4 Key Entity Extraction result", result_1_4)
        key_entities = result_1_4.get("entities", [])
        state["key_entities"] = key_entities
        print(f"  Key entities: {len(key_entities)}")
        for entity in key_entities[:5]:
            name = _ascii_safe(entity.get("name", ""))
            desc = _ascii_safe(entity.get("description", ""))
            print(f"    - {name}: {desc}")
        
        # Step 1.5: Relation Mention Detection
        print("\n[Phase 1.5] Relation Mention Detection...")
        result_1_5 = await step_1_5_relation_mention_detection(nl_description, entities=key_entities)
        log_json(logger, "Phase 1.5 Relation Mention Detection result", result_1_5)
        explicit_relations = result_1_5.get("relations", [])
        state["explicit_relations"] = explicit_relations
        print(f"  Explicit relations: {len(explicit_relations)}")
        
        # Step 1.6: Auxiliary Entity Suggestion
        print("\n[Phase 1.6] Auxiliary Entity Suggestion...")
        result_1_6 = await step_1_6_auxiliary_entity_suggestion(
            nl_description, key_entities=key_entities, domain=inferred_domain
        )
        log_json(logger, "Phase 1.6 Auxiliary Entity Suggestion result", result_1_6)
        auxiliary_entities = result_1_6.get("suggested_entities", [])
        state["auxiliary_entities"] = auxiliary_entities
        print(f"  Auxiliary entities: {len(auxiliary_entities)}")
        
        # Step 1.7: Entity Consolidation
        print("\n[Phase 1.7] Entity Consolidation...")
        result_1_7 = await step_1_7_entity_consolidation(
            key_entities, auxiliary_entities=auxiliary_entities, domain=inferred_domain, nl_description=nl_description
        )
        log_json(logger, "Phase 1.7 Entity Consolidation result", result_1_7)
        final_entity_names = result_1_7.get("final_entities", [])
        # Reconstruct consolidated entities from final entity list
        all_entities_dict = {e.get("name", ""): e for e in key_entities + auxiliary_entities}
        consolidated_entities = [all_entities_dict[name] for name in final_entity_names if name in all_entities_dict]
        state["entities"] = consolidated_entities
        log_json(logger, "Phase 1.7 consolidated_entities (reconstructed)", consolidated_entities)
        print(f"  Consolidated entities: {len(consolidated_entities)}")

        # Step 1.75: Entity vs Relation Reclassification (associative-link guardrail)
        print("\n[Phase 1.75] Entity vs Relation Reclassification...")
        result_1_75 = await step_1_75_entity_relation_reclassification(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        log_json(logger, "Phase 1.75 Entity vs Relation Reclassification result", result_1_75)
        consolidated_entities = result_1_75.get("entities", consolidated_entities)
        state["entities"] = consolidated_entities
        removed = result_1_75.get("removed_entity_names", [])
        print(f"  Reclassified/removed associative entities: {len(removed)}")
        if removed:
            for name in removed:
                print(f"    - {name}")

        # Step 1.76: Entity vs Attribute Guardrail (deterministic)
        print("\n[Phase 1.76] Entity vs Attribute Guardrail...")
        result_1_76 = await step_1_76_entity_attribute_guardrail(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        log_json(logger, "Phase 1.76 Entity vs Attribute Guardrail result", result_1_76)
        consolidated_entities = result_1_76.get("entities", consolidated_entities)
        state["entities"] = consolidated_entities
        state["attribute_candidates_phase1"] = result_1_76.get("attribute_candidates", [])
        removed_attr = result_1_76.get("removed_entity_names", [])
        print(f"  Reclassified/removed attribute-like entities: {len(removed_attr)}")
        if removed_attr:
            for name in removed_attr:
                print(f"    - {name}")
        
        # Step 1.8: Entity Cardinality
        print("\n[Phase 1.8] Entity Cardinality...")
        result_1_8 = await step_1_8_entity_cardinality(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        log_json(logger, "Phase 1.8 Entity Cardinality result", result_1_8)
        entity_info_1_8 = result_1_8.get("entity_info", [])
        print(f"  Entity cardinalities identified for {len(entity_info_1_8)} entities")
        
        # Step 1.9: Key Relations Extraction
        print("\n[Phase 1.9] Key Relations Extraction...")
        result_1_9 = await step_1_9_key_relations_extraction(
            consolidated_entities, nl_description, domain=inferred_domain, mentioned_relations=explicit_relations
        )
        log_json(logger, "Phase 1.9 Key Relations Extraction result", result_1_9)
        key_relations = result_1_9.get("relations", [])
        state["relations"] = key_relations
        print(f"  Key relations: {len(key_relations)}")
        
        # Step 1.10: Schema Connectivity (with loop)
        print("\n[Phase 1.10] Schema Connectivity Validation (with loop)...")
        result_1_10 = await step_1_10_schema_connectivity_with_loop(
            consolidated_entities, key_relations, nl_description=nl_description,
            max_iterations=5, max_time_sec=180
        )
        log_json(logger, "Phase 1.10 Schema Connectivity (with loop) result", result_1_10)
        connectivity_result = result_1_10.get("final_result", {})
        log_json(logger, "Phase 1.10 connectivity_result (derived)", connectivity_result)
        orphan_count = len(connectivity_result.get("orphan_entities", []))
        print(f"  Connectivity validation: orphan_entities={orphan_count}")

        # If Step 1.10 added relations to resolve connectivity, propagate them downstream.
        updated_relations = result_1_10.get("updated_relations", key_relations)
        try:
            before_count = len(key_relations) if isinstance(key_relations, list) else 0
            after_count = len(updated_relations) if isinstance(updated_relations, list) else 0
        except Exception:
            before_count, after_count = 0, 0
        if isinstance(updated_relations, list) and after_count != before_count:
            logger.info(
                f"Phase 1.10 updated relation list for downstream steps: "
                f"{before_count} -> {after_count}"
            )
        log_json(logger, "Phase 1.10 updated_relations (propagated downstream)", updated_relations)
        key_relations = updated_relations
        state["relations"] = key_relations
        
        # Step 1.11: Relation Cardinality
        print("\n[Phase 1.11] Relation Cardinality...")
        result_1_11 = await step_1_11_relation_cardinality(
            key_relations, consolidated_entities, nl_description=nl_description
        )
        log_json(logger, "Phase 1.11 Relation Cardinality result", result_1_11)
        # Step 1.11 returns {"relation_cardinalities": [list]} - convert to dict keyed by relation_id
        relation_cardinalities_list = result_1_11.get("relation_cardinalities", [])
        # Persist raw Step 1.11 outputs in state (useful for debugging and downstream artifacts).
        state["relation_cardinalities"] = relation_cardinalities_list

        # IMPORTANT:
        # Merge Step 1.11 results back into the relation objects so Phase 1 end artifacts
        # (state["relations"] / er_diagram_phase1) include cardinalities + participations.
        #
        # Step 1.11 preserves input order, so we primarily zip by index; fall back to
        # entity-set matching if needed.
        enriched_relations = []
        try:
            # Build fallback lookup by entity-set
            by_key = {}
            for rc in relation_cardinalities_list or []:
                ents = rc.get("entities", []) or []
                if ents:
                    by_key[tuple(sorted(ents))] = rc

            for rel, rc in zip(key_relations or [], relation_cardinalities_list or []):
                rel_ents = rel.get("entities", []) or []
                rc_ents = rc.get("entities", []) or []
                # Prefer index-aligned merge when entities match; otherwise try key lookup.
                if rel_ents and rc_ents and tuple(rel_ents) == tuple(rc_ents):
                    rel2 = dict(rel)
                    rel2["entity_cardinalities"] = rc.get("entity_cardinalities")
                    rel2["entity_participations"] = rc.get("entity_participations")
                    enriched_relations.append(rel2)
                else:
                    rel2 = dict(rel)
                    hit = by_key.get(tuple(sorted(rel_ents))) if rel_ents else None
                    if isinstance(hit, dict):
                        rel2["entity_cardinalities"] = hit.get("entity_cardinalities")
                        rel2["entity_participations"] = hit.get("entity_participations")
                    enriched_relations.append(rel2)

            # If there were more relations than cardinality results (shouldn't happen),
            # append the remaining relations unchanged.
            if len(enriched_relations) < len(key_relations or []):
                for rel in (key_relations or [])[len(enriched_relations):]:
                    enriched_relations.append(dict(rel))
        except Exception:
            enriched_relations = [dict(r) for r in (key_relations or [])]

        key_relations = enriched_relations
        state["relations"] = key_relations
        # Convert list to dict using same relation_id format as step_2_14: "Entity1+Entity2"
        relation_results_1_11 = {}
        for rel_card in relation_cardinalities_list:
            entities_in_rel = rel_card.get("entities", [])
            if entities_in_rel:
                relation_id = f"{'+'.join(sorted(entities_in_rel))}"
                relation_results_1_11[relation_id] = rel_card
        log_json(logger, "Phase 1.11 relation_results_1_11 (derived)", relation_results_1_11)
        print(f"  Relation cardinalities identified for {len(relation_results_1_11)} relations")
        
        # Step 1.12: Relation Validation (with loop)
        print("\n[Phase 1.12] Relation Validation (with loop)...")
        # Get relation cardinalities from Step 1.11
        relation_cardinalities_list = result_1_11.get("relation_cardinalities", [])
        result_1_12 = await step_1_12_relation_validation_with_loop(
            consolidated_entities, key_relations, relation_cardinalities=relation_cardinalities_list,
            nl_description=nl_description, max_iterations=5, max_time_sec=180
        )
        # Keep file logging minimal/standardized: LLM request/response pairs are handled by PipelineLogger.
        # Avoid dumping large intermediate JSON blobs to the run log.
        validation_result = result_1_12.get("final_result", {})
        print(f"  Relation validation: {validation_result.get('validation_passed', False)}")
        
        print("\n[PASS] Phase 1 completed")
        phase_1_sec = timer_elapsed_seconds(phase_1_t0)
        log_phase_duration(logger, phase=1, seconds=phase_1_sec)
        print(f"[Timing] Phase 1 took {phase_1_sec:.2f} seconds")
        # Avoid dumping full state to the run log (huge). Artifacts can be written separately if needed.
        # Provide a lightweight ER-style snapshot early (entities + relations) so Phase 1
        # has a concrete "ER diagram" artifact even before Phase 3's compiled ER design.
        state["er_diagram_phase1"] = {
            "entities": state.get("entities", []),
            "relations": state.get("relations", []),
        }
        # Artifacts are intentionally not dumped to logs by default (keeps repo + logs clean).

        if max_phase and max_phase < 2:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 1.")
            if return_state:
                return {"success": all_passed, "state": state}
            return all_passed
        
        # ========================================================================
        # PHASE 2: Attribute Discovery & Schema Design
        # ========================================================================
        phase_2_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 2: Attribute Discovery & Schema Design")
        print("=" * 80)
        
        # Step 2.1: Attribute Count Detection
        print("\n[Phase 2.1] Attribute Count Detection...")
        result_2_1 = await step_2_1_attribute_count_detection_batch(consolidated_entities, nl_description)
        attr_count_results = result_2_1.get("entity_results", {})
        print(f"  Attribute counts detected for {len(attr_count_results)} entities")
        
        # Step 2.2: Intrinsic Attributes
        print("\n[Phase 2.2] Intrinsic Attributes...")
        result_2_2 = await step_2_2_intrinsic_attributes_batch(
            consolidated_entities,
            nl_description,
            domain=inferred_domain,
            relations=key_relations,
            primary_keys=state.get("primary_keys", {}),
        )
        entity_attr_results = result_2_2.get("entity_results", {})
        entity_attributes = {}
        for entity_name, attr_result in entity_attr_results.items():
            entity_attributes[entity_name] = attr_result.get("attributes", [])
        state["attributes"] = entity_attributes
        print(f"  Attributes identified for {len(entity_attributes)} entities")
        
        # Step 2.3: Attribute Synonym Detection
        print("\n[Phase 2.3] Attribute Synonym Detection...")
        result_2_3 = await step_2_3_attribute_synonym_detection_batch(
            consolidated_entities, entity_attributes, nl_description
        )
        synonym_results = result_2_3.get("entity_results", {})
        print(f"  Synonyms detected for {len(synonym_results)} entities")

        # Apply updated attributes if provided (LLM decided; we apply deterministically)
        updated_attrs = result_2_3.get("updated_attributes", {})
        if isinstance(updated_attrs, dict) and updated_attrs:
            entity_attributes = updated_attrs
            state["attributes"] = entity_attributes

        # Step 2.16: Cross-Entity Attribute Reconciliation (double precaution)
        print("\n[Phase 2.16] Cross-Entity Attribute Reconciliation...")
        result_2_16 = await step_2_16_cross_entity_attribute_reconciliation_batch(
            entities=consolidated_entities,
            attributes=entity_attributes,
            relations=key_relations,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        entity_attributes = result_2_16.get("updated_attributes", entity_attributes)
        state["attributes"] = entity_attributes
        print(f"  Cross-entity reconciliation processed for {len((result_2_16.get('entity_results') or {}))} entities")
        
        # Step 2.4: Composite Attribute Handling
        print("\n[Phase 2.4] Composite Attribute Handling...")
        entity_attr_lists = {
            name: [attr.get("name") if isinstance(attr, dict) else getattr(attr, "name", "") for attr in attrs]
            for name, attrs in entity_attributes.items()
        }
        result_2_4 = await step_2_4_composite_attribute_handling_batch(
            consolidated_entities, entity_attr_lists, nl_description
        )
        composite_results = result_2_4.get("entity_results", {})
        print(f"  Composite attributes handled for {len(composite_results)} entities")
        
        # Step 2.5: Temporal Attributes Detection
        print("\n[Phase 2.5] Temporal Attributes Detection...")
        result_2_5 = await step_2_5_temporal_attributes_detection_batch(
            consolidated_entities, entity_attr_lists, nl_description
        )
        temporal_results = result_2_5.get("entity_results", {})
        print(f"  Temporal attributes detected for {len(temporal_results)} entities")
        
        # Step 2.6: Naming Convention Validation
        print("\n[Phase 2.6] Naming Convention Validation...")
        entities_for_validation = [
            {"name": e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")}
            for e in consolidated_entities
        ]
        result_2_6 = step_2_6_naming_convention_validation(
            entities_for_validation, entity_attr_lists
        )
        naming_passed = result_2_6.get("validation_passed", False)
        print(f"  Naming validation: {'PASSED' if naming_passed else 'FAILED'}")
        
        # Step 2.7: Primary Key Identification
        print("\n[Phase 2.7] Primary Key Identification...")
        result_2_7 = await step_2_7_primary_key_identification_batch(
            consolidated_entities, entity_attr_lists, nl_description, domain=inferred_domain
        )
        pk_results = result_2_7.get("entity_results", {})
        entity_primary_keys = {
            name: pk_info.get("primary_key", [])
            for name, pk_info in pk_results.items()
        }
        state["primary_keys"] = entity_primary_keys
        print(f"  Primary keys identified for {len(entity_primary_keys)} entities")

        # If Step 2.7 chose a surrogate key not present in the attribute list, propagate it into attributes now.
        # This prevents downstream steps (2.8+, relational compilation, Phase 4) from operating on a PK column that doesn't exist.
        for ent_name, pk_list in (entity_primary_keys or {}).items():
            if not isinstance(pk_list, list):
                continue
            for pk_attr in pk_list:
                if not isinstance(pk_attr, str) or not pk_attr.strip():
                    continue
                if pk_attr not in (entity_attr_lists.get(ent_name) or []):
                    entity_attr_lists.setdefault(ent_name, []).append(pk_attr)
                # Ensure full attribute objects exist too
                attrs_obj_list = entity_attributes.get(ent_name) or []
                existing_names = {
                    (a.get("name") if isinstance(a, dict) else getattr(a, "name", ""))
                    for a in attrs_obj_list
                }
                if pk_attr not in existing_names:
                    attrs_obj_list.append(
                        {"name": pk_attr, "description": "Surrogate primary key (auto-added)", "type_hint": "integer"}
                    )
                    entity_attributes[ent_name] = attrs_obj_list
        state["attributes"] = entity_attributes
        
        # Step 2.8: Multivalued/Derived Detection
        print("\n[Phase 2.8] Multivalued/Derived Detection...")
        result_2_8 = await step_2_8_multivalued_derived_detection_batch(
            consolidated_entities, entity_attr_lists, nl_description, domain=inferred_domain
        )
        multivalued_results = result_2_8.get("entity_results", {})
        print(f"  Multivalued/derived attributes detected for {len(multivalued_results)} entities")
        
        # Step 2.9: Derived Attribute Formulas
        print("\n[Phase 2.9] Derived Attribute Formulas...")
        entity_derived_attrs = {}
        for entity_name, mv_result in multivalued_results.items():
            derived = mv_result.get("derived", [])
            if derived:
                entity_derived_attrs[entity_name] = derived
        # Build entity descriptions dict
        entity_descriptions = {
            e.get("name", "") if isinstance(e, dict) else getattr(e, "name", ""): 
            e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
            for e in consolidated_entities
        }
        result_2_9 = await step_2_9_derived_attribute_formulas_batch(
            entity_derived_attrs, entity_attr_lists, entity_descriptions=entity_descriptions, nl_description=nl_description
        )
        formula_results = result_2_9.get("entity_results", {})
        metric_results = result_2_9.get("entity_metrics", {}) or {}
        print(f"  Derived attribute formulas extracted for {len(formula_results)} entities")
        metric_entity_count = len([k for k, v in (metric_results or {}).items() if isinstance(v, dict) and v])
        if metric_entity_count:
            print(f"  Aggregate/query-level metrics detected for {metric_entity_count} entities (stored separately)")

        # Store in state for phase gates and later phases:
        # - derived_formulas: flat map keyed by "Entity.attr"
        # - derived_metrics: flat map keyed by "Entity.attr"
        derived_formulas_flat = {}
        for ent, mp in (formula_results or {}).items():
            if not isinstance(mp, dict):
                continue
            for attr, info in mp.items():
                if isinstance(attr, str) and attr and isinstance(info, dict):
                    derived_formulas_flat[f"{ent}.{attr}"] = info
        derived_metrics_flat = {}
        for ent, mp in (metric_results or {}).items():
            if not isinstance(mp, dict):
                continue
            for attr, info in mp.items():
                if isinstance(attr, str) and attr and isinstance(info, dict):
                    derived_metrics_flat[f"{ent}.{attr}"] = info
        state["derived_formulas"] = derived_formulas_flat
        state["derived_metrics"] = derived_metrics_flat

        # Deterministic FD seeding BEFORE Phase 3:
        # Any derived attribute is functionally determined by its dependencies.
        # We store these as an initial FD set; Step 4.1 (LLM) can add more later.
        try:
            # Do not seed FDs from aggregate/query-level metrics or invalid formulas.
            row_level_only = {}
            for ent, mp in (formula_results or {}).items():
                if not isinstance(mp, dict):
                    continue
                for attr, info in mp.items():
                    if not isinstance(attr, str) or not attr or not isinstance(info, dict):
                        continue
                    if bool(info.get("is_aggregate_metric", False)):
                        continue
                    if info.get("validation_errors"):
                        continue
                    if not str(info.get("formula", "") or "").strip():
                        continue
                    row_level_only.setdefault(ent, {})[attr] = info
            fd_seed = seed_functional_dependencies_from_derived_formulas(row_level_only)
        except Exception as e:
            logger.warning(f"Failed to seed functional dependencies from derived formulas: {e}")
            fd_seed = {}
        state["functional_dependencies"] = fd_seed
        total_seed_fds = sum(len(v) for v in (fd_seed or {}).values())
        print(f"  Seeded functional dependencies from derived attributes: {total_seed_fds} total across {len(fd_seed)} entities")
        
        # Step 2.10: Unique Constraints
        print("\n[Phase 2.10] Unique Constraints...")
        result_2_10 = await step_2_10_unique_constraints_batch(
            consolidated_entities, entity_attr_lists, entity_primary_keys, nl_description, domain=inferred_domain
        )
        unique_results = result_2_10.get("entity_results", {})
        print(f"  Unique constraints identified for {len(unique_results)} entities")
        
        # Step 2.11: Nullability Constraints
        print("\n[Phase 2.11] Nullability Constraints...")
        result_2_11 = await step_2_11_nullability_constraints_batch(
            consolidated_entities, entity_attr_lists, entity_primary_keys, nl_description, domain=inferred_domain
        )
        nullability_results = result_2_11.get("entity_results", {})
        print(f"  Nullability constraints identified for {len(nullability_results)} entities")
        
        # Step 2.12: Default Values
        print("\n[Phase 2.12] Default Values...")
        result_2_12 = await step_2_12_default_values_batch(
            consolidated_entities, entity_attr_lists, entity_nullability=nullability_results, nl_description=nl_description, domain=inferred_domain
        )
        default_results = result_2_12.get("entity_results", {})
        print(f"  Default values identified for {len(default_results)} entities")
        
        # Step 2.13: Check Constraints (disabled for now; requires finalized relational schema)
        # Step 2.14: Entity Cleanup (LLM-driven; no Python deletions)
        print("\n[Phase 2.14] Entity Cleanup (relation-connecting attribute cleanup)...")
        result_2_14 = await step_2_14_entity_cleanup_batch(
            entities=consolidated_entities,
            entity_attributes=entity_attributes,
            primary_keys=entity_primary_keys,
            relations=key_relations,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        cleaned_attrs = result_2_14.get("updated_attributes", {}) or {}
        if isinstance(cleaned_attrs, dict) and cleaned_attrs:
            entity_attributes = cleaned_attrs
            state["attributes"] = entity_attributes

        # Rebuild name-only lists for downstream steps
        entity_attr_lists = {
            name: [attr.get("name") if isinstance(attr, dict) else getattr(attr, "name", "") for attr in attrs]
            for name, attrs in entity_attributes.items()
        }

        # Foreign keys are constructed later during relational schema compilation
        foreign_keys = []
        state["foreign_keys"] = foreign_keys
        state["entity_attributes"] = entity_attributes
        print(f"  Entity cleanup completed for {len(cleaned_attrs)} entities; foreign keys deferred")
        
        print("\n[PASS] Phase 2 completed")
        phase_2_sec = timer_elapsed_seconds(phase_2_t0)
        log_phase_duration(logger, phase=2, seconds=phase_2_sec)
        print(f"[Timing] Phase 2 took {phase_2_sec:.2f} seconds")
        # Avoid duplicate timing logs and large artifact dumps.
        
        if max_phase and max_phase < 3:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 2.")
            if return_state:
                return {"success": all_passed, "state": state}
            return all_passed
        
        # ========================================================================
        # PHASE 3: Query Requirements & Schema Refinement
        # ========================================================================
        phase_3_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 3: Query Requirements & Schema Refinement")
        print("=" * 80)
        
        # Step 3.1: Information Need Identification
        print("\n[Phase 3.1] Information Need Identification (with loop)...")
        result_3_1 = await step_3_1_information_need_identification_with_loop(
            nl_description, consolidated_entities, key_relations, entity_attributes,
            entity_primary_keys, foreign_keys, domain=inferred_domain,
            max_iterations=5, max_time_sec=180
        )
        info_needs_result = result_3_1.get("final_result", {})
        information_needs = info_needs_result.get("information_needs", [])
        state["information_needs"] = information_needs
        print(f"  Information needs identified: {len(information_needs)}")
        for i, need in enumerate(information_needs[:3], 1):
            print(f"    {i}. {need.get('description', '')}")
        
        # Step 3.2: Information Completeness Check
        print("\n[Phase 3.2] Information Completeness Check...")
        phase3_cfg = get_phase3_config()
        result_3_2 = await step_3_2_information_completeness_batch(
            information_needs, consolidated_entities, key_relations, entity_attributes,
            entity_primary_keys, foreign_keys, nl_description=nl_description, domain=inferred_domain,
            max_iterations=phase3_cfg.step_3_2_max_iterations,
            max_time_sec=phase3_cfg.step_3_2_max_time_sec,
        )
        completeness_results = result_3_2.get("completeness_results", {})
        print(f"  Completeness checked for {len(completeness_results)} information needs")
        
        # Step 3.3: Phase 2 Steps with Enhanced Context
        print("\n[Phase 3.3] Phase 2 Steps with Enhanced Context...")
        result_3_3 = await step_3_3_phase2_reexecution(
            entities=consolidated_entities,
            relations=key_relations,
            attributes=entity_attributes,
            primary_keys=entity_primary_keys,
            information_needs=information_needs,
            completeness_results=completeness_results,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        
        # Merge new attributes from Step 3.3 into entity_attributes
        new_attributes = result_3_3.get("new_attributes", {})
        new_derived_attributes = result_3_3.get("new_derived_attributes", {})
        updated_primary_keys = result_3_3.get("updated_primary_keys", {})
        
        # Merge new intrinsic attributes
        for entity_name, new_attrs in new_attributes.items():
            if entity_name not in entity_attributes:
                entity_attributes[entity_name] = []
            # Add new attributes, avoiding duplicates
            existing_attr_names = {attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "") 
                                  for attr in entity_attributes[entity_name]}
            for new_attr in new_attrs:
                attr_name = new_attr.get("name", "") if isinstance(new_attr, dict) else getattr(new_attr, "name", "")
                if attr_name and attr_name not in existing_attr_names:
                    entity_attributes[entity_name].append(new_attr)
        
        # Merge new derived attributes
        for entity_name, new_derived_attrs in new_derived_attributes.items():
            if entity_name not in entity_attributes:
                entity_attributes[entity_name] = []
            # Add new derived attributes, avoiding duplicates
            existing_attr_names = {attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "") 
                                  for attr in entity_attributes[entity_name]}
            for new_derived_attr in new_derived_attrs:
                attr_name = new_derived_attr.get("name", "") if isinstance(new_derived_attr, dict) else getattr(new_derived_attr, "name", "")
                if attr_name and attr_name not in existing_attr_names:
                    entity_attributes[entity_name].append(new_derived_attr)
        
        # Merge updated primary keys
        for entity_name, new_pk in updated_primary_keys.items():
            entity_primary_keys[entity_name] = new_pk
        
        total_new_intrinsic = sum(len(attrs) for attrs in new_attributes.values())
        total_new_derived = sum(len(attrs) for attrs in new_derived_attributes.values())
        print(f"  Phase 2 re-execution completed: {total_new_intrinsic} new intrinsic attributes, {total_new_derived} new derived attributes")
        if updated_primary_keys:
            print(f"  Updated primary keys for {len(updated_primary_keys)} entities")
        
        # Step 3.4: ER Design Compilation
        print("\n[Phase 3.4] ER Design Compilation...")
        # Merge cardinality and participation info from Step 1.11 into relations
        # Use same relation_id format: "Entity1+Entity2" (sorted)
        relations_with_cardinality = []
        for relation in key_relations:
            relation_entities = relation.get("entities", [])
            if not relation_entities:
                relations_with_cardinality.append(relation)
                continue
            
            # Generate relation_id using same format as step_1_11 and step_2_14
            relation_id = f"{'+'.join(sorted(relation_entities))}"
            
            # Find matching relation result from Step 1.11
            rel_result = relation_results_1_11.get(relation_id)
            
            if rel_result:
                relation_copy = dict(relation) if isinstance(relation, dict) else relation.__dict__.copy()
                relation_copy["entity_cardinalities"] = rel_result.get("entity_cardinalities", {})
                relation_copy["entity_participations"] = rel_result.get("entity_participations", {})
                relations_with_cardinality.append(relation_copy)
            else:
                # No cardinality info found - log warning but continue
                logger.warning(
                    f"No cardinality info found for relation {relation_id} "
                    f"(entities: {relation_entities}). Proceeding without cardinality data."
                )
                relations_with_cardinality.append(relation)
        
        er_design = step_3_4_er_design_compilation(
            consolidated_entities, relations_with_cardinality, entity_attributes,
            entity_primary_keys, foreign_keys
        )
        state["er_design"] = er_design
        print(f"  ER design compiled: {len(er_design.get('entities', []))} entities, {len(er_design.get('relations', []))} relations")
        
        # Step 3.45: Junction Table Naming
        print("\n[Phase 3.45] Junction Table Naming...")
        junction_table_names = await step_3_45_junction_table_naming(
            relations=er_design.get("relations", []),
            entities=er_design.get("entities", []),
            nl_description=nl_description,
            domain=state.get("domain"),
        )
        state["junction_table_names"] = junction_table_names
        if junction_table_names:
            print(f"  Named {len(junction_table_names)} junction tables")
            for rel_key, name in list(junction_table_names.items())[:3]:
                print(f"    - {rel_key} -> {name}")
        else:
            print("  No junction tables needed")
        
        # Step 3.5: Relational Schema Compilation
        print("\n[Phase 3.5] Relational Schema Compilation...")
        relational_schema = step_3_5_relational_schema_compilation(
            er_design, foreign_keys, entity_primary_keys, constraints=state.get("constraints"), junction_table_names=junction_table_names
        )
        state["relational_schema"] = relational_schema
        tables = relational_schema.get("tables", [])
        print(f"  Relational schema compiled: {len(tables)} tables")
        for table in tables[:3]:
            print(f"    - {table.get('name', '')}: {len(table.get('columns', []))} columns")
        
        print("\n[PASS] Phase 3 completed")
        phase_3_sec = timer_elapsed_seconds(phase_3_t0)
        log_phase_duration(logger, phase=3, seconds=phase_3_sec)
        print(f"[Timing] Phase 3 took {phase_3_sec:.2f} seconds")
        dump_phase_artifacts(logger=logger, phase=3, state=state)

        # Respect max_phase: stop before entering Phase 4.
        if max_phase and max_phase < 4:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 3.")
            if return_state:
                return {"success": all_passed, "state": state}
            return all_passed
        
        
        # ========================================================================
        # PHASE 4: Functional Dependencies & Data Types
        # ========================================================================
        phase_4_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 4: Functional Dependencies & Data Types")
        print("=" * 80)
        
        # Step 4.1: Functional Dependency Analysis
        print("\n[Phase 4.1] Functional Dependency Analysis...")
        # Extract derived attributes from Step 2.9
        entity_derived_attrs_dict = {}
        for entity_name, formula_result in formula_results.items():
            formulas = formula_result.get("formulas", {})
            entity_derived_attrs_dict[entity_name] = formulas
        
        # Merge derived attributes from Step 3.3 (new_derived_attributes)
        # Step 3.3 derived attributes have structure: [{"name": "...", "derivation_formula": "...", ...}, ...]
        if new_derived_attributes:
            for entity_name, derived_attrs_list in new_derived_attributes.items():
                if entity_name not in entity_derived_attrs_dict:
                    entity_derived_attrs_dict[entity_name] = {}
                # Extract derivation_formula from each derived attribute
                for derived_attr in derived_attrs_list:
                    if isinstance(derived_attr, dict):
                        attr_name = derived_attr.get("name", "")
                        formula = derived_attr.get("derivation_formula", "")
                    else:
                        attr_name = getattr(derived_attr, "name", "")
                        formula = getattr(derived_attr, "derivation_formula", "")
                    if attr_name and formula:
                        # Merge with existing formulas (Step 3.3 takes precedence if duplicate)
                        entity_derived_attrs_dict[entity_name][attr_name] = formula
        
        # Mine FDs AFTER relational schema compilation, using relational schema tables/columns as source of truth.
        result_4_1 = await step_4_1_functional_dependency_analysis_batch(
            consolidated_entities,
            entity_attributes,
            entity_primary_keys,
            relational_schema=relational_schema,
            entity_derived_attributes=entity_derived_attrs_dict if entity_derived_attrs_dict else None,
            nl_description=nl_description,
            domain=inferred_domain,
            max_iterations=3,
            max_time_sec=180,
        )
        fd_results = result_4_1.get("entity_results", {})
        functional_dependencies = {}
        for entity_name, fd_result in fd_results.items():
            functional_dependencies[entity_name] = fd_result.get("functional_dependencies", [])
        state["functional_dependencies"] = functional_dependencies
        total_fds = sum(len(fds) for fds in functional_dependencies.values())
        print(f"  Functional dependencies identified: {total_fds} total across {len(functional_dependencies)} entities")
        for entity_name, fds in functional_dependencies.items():
            if fds:
                print(f"    - {entity_name}: {len(fds)} dependencies")
                for fd in fds[:2]:
                    lhs = fd.get("lhs", [])
                    rhs = fd.get("rhs", [])
                    print(f"      {', '.join(lhs)} -> {', '.join(rhs)}")
        
        # Step 4.2: 3NF Normalization
        print("\n[Phase 4.2] 3NF Normalization...")
        normalized_schema = step_4_2_3nf_normalization(relational_schema, functional_dependencies)
        normalized_tables = normalized_schema.get("normalized_tables", [])
        decomposition_steps = normalized_schema.get("decomposition_steps", [])
        state["normalized_schema"] = normalized_schema
        print(f"  3NF normalization completed: {len(normalized_tables)} normalized tables")
        if decomposition_steps:
            print(f"  Decomposition steps: {len(decomposition_steps)}")
            for step in decomposition_steps[:2]:
                # Replace Unicode arrow with ASCII arrow for Windows console compatibility
                step_str = str(step).replace('\u2192', '->')
                print(f"    - {step_str}")
        
        # Step 4.3: Data Type Assignment
        print("\n[Phase 4.3] Data Type Assignment...")
        # Prepare data structures for Step 4.3
        # Note: check_results from Step 2.13 is disabled, so initialize as empty
        check_results = {}  # Step 2.13 is disabled, so no check constraints from Phase 2
        entity_check_constraints = {}
        for entity_name, check_result in check_results.items():
            constraints = check_result.get("check_constraints", {})
            if constraints:
                entity_check_constraints[entity_name] = constraints
        
        entity_unique_constraints = {}
        for entity_name, unique_result in unique_results.items():
            unique_attrs = unique_result.get("unique_attributes", [])
            if unique_attrs:
                entity_unique_constraints[entity_name] = unique_attrs
        
        entity_nullable_attributes = {}
        for entity_name, null_result in nullability_results.items():
            nullable = null_result.get("nullable_attributes", [])
            if nullable:
                entity_nullable_attributes[entity_name] = nullable
        
        # Build entity relations dict (which relations involve each entity)
        entity_relations_dict = {}
        for entity in consolidated_entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_relations_dict[entity_name] = [
                rel for rel in key_relations
                if entity_name in (rel.get("entities", []) if isinstance(rel, dict) else [])
            ]
        
        result_4_3 = await step_4_3_data_type_assignment_batch(
            consolidated_entities,
            entity_attributes,
            entity_primary_keys=entity_primary_keys,
            entity_check_constraints=entity_check_constraints if entity_check_constraints else None,
            entity_unique_constraints=entity_unique_constraints if entity_unique_constraints else None,
            entity_nullable_attributes=entity_nullable_attributes if entity_nullable_attributes else None,
            entity_relations=entity_relations_dict if entity_relations_dict else None,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        type_results = result_4_3.get("entity_results", {})
        entity_attribute_types = {}
        for entity_name, type_result in type_results.items():
            if not type_result.get("error"):
                entity_attribute_types[entity_name] = type_result.get("attribute_types", {})
        state["attribute_types"] = entity_attribute_types
        total_types = sum(len(types) for types in entity_attribute_types.values())
        print(f"  Data types assigned: {total_types} attributes across {len(entity_attribute_types)} entities")
        for entity_name, types in list(entity_attribute_types.items())[:2]:
            print(f"    - {entity_name}: {len(types)} attributes")
            for attr_name, type_info in list(types.items())[:2]:
                sql_type = type_info.get("type", "")
                size = type_info.get("size")
                precision = type_info.get("precision")
                scale = type_info.get("scale")
                type_str = sql_type
                if size:
                    type_str += f"({size})"
                elif precision and scale:
                    type_str += f"({precision},{scale})"
                elif precision:
                    type_str += f"({precision})"
                print(f"      {attr_name}: {type_str}")
        
        # Step 4.4: Categorical Detection
        print("\n[Phase 4.4] Categorical Detection...")
        result_4_4 = await step_4_4_categorical_detection_batch(
            consolidated_entities,
            entity_attributes,
            entity_attribute_types=entity_attribute_types if entity_attribute_types else None,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        categorical_results = result_4_4.get("entity_results", {})
        entity_categorical_attributes = {}
        for entity_name, cat_result in categorical_results.items():
            if not cat_result.get("error"):
                categorical_attrs = cat_result.get("categorical_attributes", [])
                if categorical_attrs:
                    entity_categorical_attributes[entity_name] = categorical_attrs
        state["categorical_attributes"] = entity_categorical_attributes
        total_categorical = sum(len(attrs) for attrs in entity_categorical_attributes.values())
        print(f"  Categorical attributes detected: {total_categorical} across {len(entity_categorical_attributes)} entities")
        for entity_name, attrs in list(entity_categorical_attributes.items())[:2]:
            print(f"    - {entity_name}: {', '.join(attrs)}")
        
        # Step 4.5: Check Constraint Detection (for categorical attributes)
        print("\n[Phase 4.5] CHECK Constraint Detection (categorical)...")
        if entity_categorical_attributes:
            result_4_5 = await step_4_5_check_constraint_detection_batch(
                entity_categorical_attributes,
                entity_attributes,
                entity_attribute_types=entity_attribute_types if entity_attribute_types else None,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            check_cat_results = result_4_5.get("entity_results", {})
            total_check_constraints = sum(
                len(result.get("check_constraint_attributes", []))
                for result in check_cat_results.values()
            )
            print(f"  CHECK constraints for categorical attributes: {total_check_constraints} across {len(check_cat_results)} entities")
            for entity_name, result in list(check_cat_results.items())[:2]:
                check_attrs = result.get("check_constraint_attributes", [])
                if check_attrs:
                    print(f"    - {entity_name}: {', '.join(check_attrs)}")
        else:
            print("  No categorical attributes found, skipping CHECK constraint detection")
            check_cat_results = {}
        
        # Step 4.6: Categorical Value Extraction
        print("\n[Phase 4.6] Categorical Value Extraction...")
        if entity_categorical_attributes:
            result_4_6 = await step_4_6_categorical_value_extraction_batch(
                entity_categorical_attributes,
                entity_attributes,
                entity_attribute_types=entity_attribute_types,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            value_results = result_4_6.get("entity_results", {})
            entity_categorical_values = {}
            for entity_name, attr_results in value_results.items():
                entity_categorical_values[entity_name] = {}
                for attr_name, attr_result in attr_results.items():
                    if not attr_result.get("error"):
                        values = attr_result.get("values", [])
                        if values:
                            entity_categorical_values[entity_name][attr_name] = values
            state["categorical_values"] = entity_categorical_values
            total_values = sum(
                len(values) for attr_dict in entity_categorical_values.values()
                for values in attr_dict.values()
            )
            print(f"  Categorical values extracted: {total_values} total values")
            for entity_name, attr_dict in list(entity_categorical_values.items())[:2]:
                for attr_name, values in list(attr_dict.items())[:1]:
                    print(f"    - {entity_name}.{attr_name}: {', '.join(values)}")
        else:
            print("  No categorical attributes found, skipping value extraction")
            entity_categorical_values = {}
        
        # Step 4.7: Categorical Distribution
        print("\n[Phase 4.7] Categorical Distribution...")
        if entity_categorical_values:
            result_4_7 = await step_4_7_categorical_distribution_batch(
                entity_categorical_values,
                entity_attributes,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            distribution_results = result_4_7.get("entity_results", {})
            total_distributions = sum(
                len([r for r in attr_results.values() if not r.get("error")])
                for attr_results in distribution_results.values()
            )
            print(f"  Categorical distributions determined: {total_distributions} across {len(distribution_results)} entities")
            for entity_name, attr_results in list(distribution_results.items())[:2]:
                for attr_name, dist_result in list(attr_results.items())[:1]:
                    if not dist_result.get("error"):
                        distribution = dist_result.get("distribution", {})
                        dist_sum = sum(distribution.values())
                        print(f"    - {entity_name}.{attr_name}: {len(distribution)} values, sum={dist_sum:.3f}")
                        # Show a few probabilities
                        for value, prob in list(distribution.items())[:3]:
                            print(f"      {value}: {prob:.3f}")
        else:
            print("  No categorical values found, skipping distribution determination")
        
        print("\n[PASS] Phase 4 completed")
        phase_4_sec = timer_elapsed_seconds(phase_4_t0)
        log_phase_duration(logger, phase=4, seconds=phase_4_sec)
        print(f"[Timing] Phase 4 took {phase_4_sec:.2f} seconds")
        phase_4_sec = timer_elapsed_seconds(phase_4_t0)
        log_phase_duration(logger, phase=4, seconds=phase_4_sec)
        print(f"[Timing] Phase 4 took {phase_4_sec:.2f} seconds")
        dump_phase_artifacts(logger=logger, phase=4, state=state)
        
        
        # ========================================================================
        # PHASE 5: DDL & SQL Generation
        # ========================================================================
        phase_5_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 5: DDL & SQL Generation")
        print("=" * 80)
        
        # Step 5.1: DDL Compilation
        print("\n[Phase 5.1] DDL Compilation...")
        ddl_result = step_5_1_ddl_compilation(
            normalized_schema=normalized_schema,
            data_types=entity_attribute_types if entity_attribute_types else None,
            check_constraints=entity_categorical_values if entity_categorical_values else None,
        )
        ddl_statements = ddl_result.get("ddl_statements", [])
        state["ddl_statements"] = ddl_statements
        print(f"  DDL statements generated: {len(ddl_statements)}")
        for i, ddl in enumerate(ddl_statements[:2], 1):
            table_name = ddl.split("CREATE TABLE")[1].split("(")[0].strip() if "CREATE TABLE" in ddl else "Unknown"
            print(f"    {i}. {table_name}")
        
        # Step 5.2: DDL Validation
        print("\n[Phase 5.2] DDL Validation...")
        validation_result = step_5_2_ddl_validation(
            ddl_statements=ddl_statements,
            validate_with_db=True,
        )
        validation_passed = validation_result.get("validation_passed", False)
        syntax_errors = validation_result.get("syntax_errors", [])
        print(f"  Validation passed: {validation_passed}")
        if syntax_errors:
            print(f"  Syntax errors: {len(syntax_errors)}")
        
        # Step 5.3: DDL Error Correction (if needed)
        if not validation_passed:
            print("\n[Phase 5.3] DDL Error Correction...")
            try:
                correction_result = await step_5_3_ddl_error_correction(
                    validation_errors=validation_result,
                    original_ddl=ddl_statements,
                    normalized_schema=normalized_schema,
                    relational_schema=relational_schema,
                )
                ir_patches = correction_result.get("ir_patches", [])
                print(f"  IR patches generated: {len(ir_patches)}")
                # Note: In a real scenario, we would apply patches and re-run 5.1
            except Exception as e:
                print(f"  Error correction failed: {e}")
        else:
            print("\n[Phase 5.3] DDL Error Correction skipped (validation passed)")
        
        # Step 5.4: Schema Creation
        print("\n[Phase 5.4] Schema Creation...")
        schema_creation_result = step_5_4_schema_creation(
            ddl_statements=ddl_statements,
            database_path=None,  # Use in-memory database
        )
        creation_success = schema_creation_result.get("success", False)
        tables_created = schema_creation_result.get("tables_created", [])
        print(f"  Schema creation: {'SUCCESS' if creation_success else 'FAILED'}")
        print(f"  Tables created: {', '.join(tables_created)}")
        
        # Step 5.5: SQL Query Generation
        print("\n[Phase 5.5] SQL Query Generation...")
        if information_needs:
            sql_results = await step_5_5_sql_query_generation_batch(
                information_needs=information_needs,
                normalized_schema=normalized_schema,
                data_types=entity_attribute_types if entity_attribute_types else None,
                relations=key_relations,
            )
            state["sql_queries"] = sql_results
            valid_queries = [r for r in sql_results if r.get("validation_status") == "valid"]
            print(f"  SQL queries generated: {len(sql_results)}")
            print(f"  Valid queries: {len(valid_queries)}")
            for i, query_result in enumerate(valid_queries[:2], 1):
                sql = query_result.get("sql", "")
                print(f"    {i}. {sql}")
            
            # Execute SQL queries against the created schema
            print("\n[Phase 5.5] Executing SQL queries against created schema...")
            import sqlite3
            query_execution_results = []
            try:
                # Re-create schema in a new in-memory database for query execution
                conn = sqlite3.connect(":memory:")
                cursor = conn.cursor()
                
                # Execute DDL statements to recreate schema
                for ddl in ddl_statements:
                    try:
                        cursor.execute(ddl)
                    except sqlite3.Error as e:
                        logger.warning(f"Failed to recreate schema for query execution: {e}")
                
                conn.commit()
                
                # Execute each valid SQL query
                executed_count = 0
                failed_count = 0
                for query_result in valid_queries:
                    sql = query_result.get("sql", "")
                    if not sql:
                        continue
                    
                    try:
                        cursor.execute(sql)
                        # Try to fetch results (for SELECT queries)
                        try:
                            results = cursor.fetchall()
                            query_execution_results.append({
                                "sql": sql,
                                "status": "success",
                                "rows_returned": len(results),
                                "error": None
                            })
                            executed_count += 1
                        except sqlite3.OperationalError:
                            # Not a SELECT query (INSERT, UPDATE, DELETE, etc.)
                            query_execution_results.append({
                                "sql": sql,
                                "status": "success",
                                "rows_returned": cursor.rowcount,
                                "error": None
                            })
                            executed_count += 1
                    except sqlite3.Error as e:
                        query_execution_results.append({
                            "sql": sql,
                            "status": "failed",
                            "rows_returned": 0,
                            "error": str(e)
                        })
                        failed_count += 1
                        logger.warning(f"Query execution failed: {sql[:100]}... Error: {e}")
                
                conn.close()
                
                print(f"  Query execution: {executed_count} succeeded, {failed_count} failed")
                state["query_execution_results"] = query_execution_results
                
            except Exception as e:
                logger.error(f"Failed to execute queries: {e}", exc_info=True)
                print(f"  Query execution failed: {e}")
        else:
            print("  No information needs found, skipping SQL query generation")
            state["sql_queries"] = []
        
        print("\n[PASS] Phase 5 completed")
        phase_5_sec = timer_elapsed_seconds(phase_5_t0)
        log_phase_duration(logger, phase=5, seconds=phase_5_sec)
        print(f"[Timing] Phase 5 took {phase_5_sec:.2f} seconds")
        phase_5_sec = timer_elapsed_seconds(phase_5_t0)
        log_phase_duration(logger, phase=5, seconds=phase_5_sec)
        print(f"[Timing] Phase 5 took {phase_5_sec:.2f} seconds")
        dump_phase_artifacts(logger=logger, phase=5, state=state)
        
        
        # ========================================================================
        # PHASE 6: Constraints & Distributions
        # ========================================================================
        phase_6_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 6: Constraints & Distributions")
        print("=" * 80)
        
        # Step 6.1: Constraint Detection (with loop)
        print("\n[Phase 6.1] Constraint Detection (with loop)...")
        constraint_result = await step_6_1_constraint_detection_with_loop(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            max_iterations=5,
            max_time_sec=180,
        )
        
        # Handle None result (e.g., if loop timed out with 0 iterations)
        if constraint_result is None:
            logger.warning("Constraint detection returned None (likely timed out with 0 iterations). Using empty constraints.")
            constraint_result = {
                "statistical_constraints": [],
                "structural_constraints": [],
                "distribution_constraints": [],
                "other_constraints": [],
            }
        
        all_constraints = []
        for category in ["statistical_constraints", "structural_constraints", "distribution_constraints", "other_constraints"]:
            constraints_list = constraint_result.get(category, [])
            for constraint_desc in constraints_list:
                all_constraints.append({
                    "description": constraint_desc,
                    "category": category.replace("_constraints", ""),
                })
        
        print(f"  Constraint detection completed")
        print(f"  Total constraints: {len(all_constraints)}")
        print(f"    - Statistical: {len(constraint_result.get('statistical_constraints', []))}")
        print(f"    - Structural: {len(constraint_result.get('structural_constraints', []))}")
        print(f"    - Distribution: {len(constraint_result.get('distribution_constraints', []))}")
        print(f"    - Other: {len(constraint_result.get('other_constraints', []))}")
        
        # Step 6.2: Constraint Scope Analysis
        print("\n[Phase 6.2] Constraint Scope Analysis...")
        if all_constraints:
            scope_results = await step_6_2_constraint_scope_analysis_batch(
                constraints=all_constraints,
                normalized_schema=normalized_schema,
                phase2_constraints=check_results if check_results else None,
            )
            # Merge scope into constraints
            for constraint, scope_result in zip(all_constraints, scope_results):
                constraint["scope"] = scope_result
            print(f"  Scope analyzed for {len(scope_results)} constraints")
        else:
            print("  No constraints found, skipping scope analysis")
            scope_results = []
        
        # Step 6.3: Constraint Enforcement Strategy
        print("\n[Phase 6.3] Constraint Enforcement Strategy...")
        if all_constraints:
            enforcement_results = await step_6_3_constraint_enforcement_strategy_batch(
                constraints_with_scope=all_constraints,
                normalized_schema=normalized_schema,
                dsl_grammar=None,  # Could provide DSL grammar as external context
            )
            # Merge enforcement into constraints
            for constraint, enforcement_result in zip(all_constraints, enforcement_results):
                constraint["enforcement"] = enforcement_result
            print(f"  Enforcement strategies determined for {len(enforcement_results)} constraints")
        else:
            print("  No constraints found, skipping enforcement strategy")
            enforcement_results = []
        
        # Step 6.4: Constraint Conflict Detection
        print("\n[Phase 6.4] Constraint Conflict Detection...")
        conflict_result = step_6_4_constraint_conflict_detection(
            constraints=all_constraints,
        )
        conflicts = conflict_result.get("conflicts", [])
        conflict_validation_passed = conflict_result.get("validation_passed", False)
        print(f"  Conflict detection: {'PASSED' if conflict_validation_passed else 'FAILED'}")
        print(f"  Conflicts found: {len(conflicts)}")
        if conflicts:
            for conflict in conflicts[:3]:
                print(f"    - {conflict.get('constraint1', '')} vs {conflict.get('constraint2', '')}")
        
        # Step 6.5: Constraint Compilation
        print("\n[Phase 6.5] Constraint Compilation...")
        compiled_constraints = step_6_5_constraint_compilation(
            constraints=all_constraints,
        )
        state["constraints"] = compiled_constraints
        total_compiled = (
            len(compiled_constraints.get("statistical_constraints", [])) +
            len(compiled_constraints.get("structural_constraints", [])) +
            len(compiled_constraints.get("distribution_constraints", [])) +
            len(compiled_constraints.get("other_constraints", []))
        )
        print(f"  Constraints compiled: {total_compiled} total")
        
        print("\n[PASS] Phase 6 completed")
        phase_6_sec = timer_elapsed_seconds(phase_6_t0)
        log_phase_duration(logger, phase=6, seconds=phase_6_sec)
        print(f"[Timing] Phase 6 took {phase_6_sec:.2f} seconds")
        phase_6_sec = timer_elapsed_seconds(phase_6_t0)
        log_phase_duration(logger, phase=6, seconds=phase_6_sec)
        print(f"[Timing] Phase 6 took {phase_6_sec:.2f} seconds")
        dump_phase_artifacts(logger=logger, phase=6, state=state)
        
        
        # ========================================================================
        # PHASE 7: Generation Strategies
        # ========================================================================
        phase_7_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 7: Generation Strategies")
        print("=" * 80)
        
        # Prepare attribute lists by type
        numerical_attributes = []
        text_attributes = []
        boolean_attributes = []
        
        for entity_name, attrs in entity_attributes.items():
            entity_obj = next((e for e in consolidated_entities if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name), None)
            entity_desc = entity_obj.get("description", "") if isinstance(entity_obj, dict) else getattr(entity_obj, "description", "") if entity_obj else ""
            
            # Get attribute types for this entity
            attr_types = entity_attribute_types.get(entity_name, {}) if entity_attribute_types else {}
            
            for attr in attrs:
                attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
                attr_desc = attr.get("description", "") if isinstance(attr, dict) else getattr(attr, "description", "")
                attr_type_info = attr_types.get(attr_name, {})
                sql_type = attr_type_info.get("type", "") if attr_type_info else ""
                
                attr_meta = {
                    "entity_name": entity_name,
                    "attribute_name": attr_name,
                    "attribute_description": attr_desc,
                    "attribute_type": sql_type,
                    "entity_description": entity_desc,
                    "relations": [rel for rel in key_relations if entity_name in (rel.get("entities", []) if isinstance(rel, dict) else [])],
                    "entity_cardinality": entity_results_1_8.get(entity_name, {}).get("cardinality", "") if entity_results_1_8 else "",
                }
                
                # Categorize by type
                if sql_type.upper() in ("INTEGER", "INT", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
                    numerical_attributes.append(attr_meta)
                elif sql_type.upper() in ("VARCHAR", "CHAR", "TEXT", "STRING"):
                    text_attributes.append(attr_meta)
                elif sql_type.upper() in ("BOOLEAN", "BOOL", "TINYINT"):
                    boolean_attributes.append(attr_meta)
        
        # Step 7.1: Numerical Range Definition
        print("\n[Phase 7.1] Numerical Range Definition...")
        if numerical_attributes:
            # Build constraints map for numerical attributes
            constraints_map = {}
            for attr in numerical_attributes:
                key = f"{attr['entity_name']}.{attr['attribute_name']}"
                # Find constraints affecting this attribute
                affecting = [
                    c for c in all_constraints
                    if key in c.get("scope", {}).get("affected_attributes", [])
                ]
                if affecting:
                    constraints_map[key] = affecting
            
            numerical_strategies = await step_7_1_numerical_range_definition_batch(
                numerical_attributes=numerical_attributes,
                constraints_map=constraints_map if constraints_map else None,
            )
            state["numerical_strategies"] = numerical_strategies
            print(f"  Numerical ranges defined for {len(numerical_strategies)} attributes")
        else:
            print("  No numerical attributes found, skipping range definition")
            numerical_strategies = {}
        
        # Step 7.2: Text Generation Strategy
        print("\n[Phase 7.2] Text Generation Strategy...")
        if text_attributes:
            text_strategies = await step_7_2_text_generation_strategy_batch(
                text_attributes=text_attributes,
                generator_catalog=None,  # Could provide generator catalog as external context
            )
            state["text_strategies"] = text_strategies
            print(f"  Text generators selected for {len(text_strategies)} attributes")
        else:
            print("  No text attributes found, skipping text generation strategy")
            text_strategies = {}
        
        # Step 7.3: Boolean Dependency Analysis
        print("\n[Phase 7.3] Boolean Dependency Analysis...")
        if boolean_attributes:
            # Build related attributes map
            related_attributes_map = {}
            for attr in boolean_attributes:
                key = f"{attr['entity_name']}.{attr['attribute_name']}"
                # Include all attributes from same entity and related entities
                related = []
                for other_attr in numerical_attributes + text_attributes + boolean_attributes:
                    if other_attr["entity_name"] == attr["entity_name"] or \
                       any(attr["entity_name"] in (rel.get("entities", []) if isinstance(rel, dict) else []) for rel in attr.get("relations", [])):
                        related.append(other_attr)
                related_attributes_map[key] = related
            
            boolean_strategies = await step_7_3_boolean_dependency_analysis_batch(
                boolean_attributes=boolean_attributes,
                related_attributes_map=related_attributes_map if related_attributes_map else None,
                dsl_grammar=None,  # Could provide DSL grammar as external context
            )
            state["boolean_strategies"] = boolean_strategies
            print(f"  Boolean dependencies analyzed for {len(boolean_strategies)} attributes")
        else:
            print("  No boolean attributes found, skipping boolean dependency analysis")
            boolean_strategies = {}
        
        # Step 7.4: Data Volume Specifications
        print("\n[Phase 7.4] Data Volume Specifications...")
        # Prepare entities with cardinality info
        entities_with_cardinality = []
        for entity in consolidated_entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            cardinality_info = entity_results_1_8.get(entity_name, {}) if entity_results_1_8 else {}
            entities_with_cardinality.append({
                "name": entity_name,
                "cardinality": cardinality_info.get("cardinality", ""),
                "cardinality_hint": cardinality_info.get("cardinality_hint", ""),
                "table_type": cardinality_info.get("table_type", ""),
            })
        
        volume_result = await step_7_4_data_volume_specifications(
            entities=entities_with_cardinality,
            nl_description=nl_description,
        )
        entity_volumes = volume_result.get("entity_volumes", {})
        state["entity_volumes"] = entity_volumes
        print(f"  Data volumes specified for {len(entity_volumes)} entities")
        for entity_name, volume in list(entity_volumes.items())[:3]:
            expected = volume.get("expected_rows", 0)
            print(f"    - {entity_name}: {expected:,} expected rows")
        
        # Step 7.5: Partitioning Strategy
        print("\n[Phase 7.5] Partitioning Strategy...")
        # Prepare entities with volumes for partitioning
        entities_with_volumes = []
        for entity_name, volume in entity_volumes.items():
            entity_obj = next((e for e in consolidated_entities if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name), None)
            # Find table columns from normalized schema
            table_obj = next((t for t in normalized_tables if t.get("name", "") == entity_name), None)
            columns = table_obj.get("columns", []) if table_obj else []
            
            entities_with_volumes.append({
                "entity_name": entity_name,
                "entity_volume": volume,
                "table_type": entity_results_1_8.get(entity_name, {}).get("table_type", "") if entity_results_1_8 else "",
                "columns": columns,
                "nl_description": nl_description,
            })
        
        partitioning_strategies = await step_7_5_partitioning_strategy_batch(
            entities_with_volumes=entities_with_volumes,
        )
        state["partitioning_strategies"] = partitioning_strategies
        needs_partitioning = sum(1 for s in partitioning_strategies.values() if s.get("needs_partitioning", False))
        print(f"  Partitioning strategies determined for {len(partitioning_strategies)} entities")
        print(f"  Entities needing partitioning: {needs_partitioning}")
        
        # Step 7.6: Distribution Compilation
        print("\n[Phase 7.6] Distribution Compilation...")
        # Prepare categorical strategies from Phase 4.7
        categorical_strategies = {}
        if entity_categorical_values:
            # Get distributions from Phase 4.7 results
            for entity_name, attr_dict in entity_categorical_values.items():
                for attr_name, values in attr_dict.items():
                    key = f"{entity_name}.{attr_name}"
                    # Try to get distribution from Phase 4.7 results
                    # For now, create a simple uniform distribution
                    if values:
                        uniform_prob = 1.0 / len(values)
                        categorical_strategies[key] = {
                            "distribution": {v: uniform_prob for v in values}
                        }
        
        generation_ir = step_7_6_distribution_compilation(
            numerical_strategies=numerical_strategies,
            text_strategies=text_strategies,
            boolean_strategies=boolean_strategies,
            categorical_strategies=categorical_strategies if categorical_strategies else None,
            entity_volumes=entity_volumes,
            partitioning_strategies=partitioning_strategies,
        )
        state["generation_ir"] = generation_ir
        column_gen_specs = generation_ir.get("column_gen_specs", [])
        print(f"  GenerationIR compiled: {len(column_gen_specs)} column generation specs")
        print(f"  Entity volumes: {len(generation_ir.get('entity_volumes', {}))}")
        print(f"  Partitioning strategies: {len(generation_ir.get('partitioning_strategies', {}))}")
        
        print("\n[PASS] Phase 7 completed")
        phase_7_sec = timer_elapsed_seconds(phase_7_t0)
        log_phase_duration(logger, phase=7, seconds=phase_7_sec)
        print(f"[Timing] Phase 7 took {phase_7_sec:.2f} seconds")
        phase_7_sec = timer_elapsed_seconds(phase_7_t0)
        log_phase_duration(logger, phase=7, seconds=phase_7_sec)
        print(f"[Timing] Phase 7 took {phase_7_sec:.2f} seconds")
        dump_phase_artifacts(logger=logger, phase=7, state=state)
        
        # ========================================================================
        # SUMMARY
        # ========================================================================
        print("\n" + "=" * 80)
        print("INTEGRATION TEST SUMMARY")
        print("=" * 80)
        print(f"Phase 1: {len(consolidated_entities)} entities, {len(key_relations)} relations")
        print(f"Phase 2: Attributes for {len(entity_attributes)} entities, {len(entity_primary_keys)} primary keys, {len(foreign_keys)} foreign keys")
        print(f"Phase 3: {len(information_needs)} information needs, {len(tables)} tables")
        categorical_count = sum(len(attrs) for attrs in entity_categorical_attributes.values()) if entity_categorical_attributes else 0
        print(f"Phase 4: {total_fds} functional dependencies, {len(normalized_tables)} normalized tables, {total_types} data types, {categorical_count} categorical attributes")
        sql_queries = state.get('sql_queries', [])
        query_exec_results = state.get('query_execution_results', [])
        executed_success = len([r for r in query_exec_results if r.get('status') == 'success']) if query_exec_results else 0
        print(f"Phase 5: {len(ddl_statements)} DDL statements, {len(sql_queries)} SQL queries generated, {executed_success} queries executed successfully")
        total_constraints = (
            len(compiled_constraints.get("statistical_constraints", [])) +
            len(compiled_constraints.get("structural_constraints", [])) +
            len(compiled_constraints.get("distribution_constraints", [])) +
            len(compiled_constraints.get("other_constraints", []))
        )
        print(f"Phase 6: {total_constraints} constraints compiled")
        print(f"Phase 7: {len(column_gen_specs)} column generation specs, {len(entity_volumes)} entity volumes")
        print("\n[PASS] All phases completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    if return_state:
        return {"success": all_passed, "state": state}
    return all_passed

async def test_phases_1_to_5_integration(
    nl_description: str = None,
    description_index: int = None,
    log_file_override: str = None,
):
    """Test Phases 1, 2, 3, 4, 5, 6, and 7 with a single NL description."""
    # Use default description if none provided
    if nl_description is None:
        nl_description = """I need a database for an online bookstore. Customers can place orders for books. 
    Each order can contain multiple books. Books have titles, authors, and prices. 
    I need to track customer addresses and order dates."""
    
    logger = get_logger(__name__)
    log_config = get_config('logging')
    
    # Use the same log file for all descriptions, unless overridden by caller
    log_file = log_file_override or log_config.get('log_file')
    
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=log_config['log_to_file'],
        log_file=log_file,
        clear_existing=(description_index is None or description_index == 1),  # Clear for first description only
    )
    
    index_prefix = f"[Description {description_index}] " if description_index is not None else ""
    print("=" * 80)
    print(f"{index_prefix}Integration Test: Phases 1, 2, 3, 4, 5, 6, and 7")
    print("=" * 80)
    
    # Replace Unicode characters for Windows console compatibility
    nl_description_display = nl_description.replace('\u2192', '->').replace('\u00d7', 'x')
    print(f"\nNatural Language Description:\n{nl_description_display}\n")
    
    all_passed = True
    state = {}  # Accumulate state across phases
    
    try:
        # ========================================================================
        # PHASE 1: Domain & Entity Discovery
        # ========================================================================
        phase_1_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 1: Domain & Entity Discovery")
        print("=" * 80)
        
        # Step 1.1: Domain Detection
        print("\n[Phase 1.1] Domain Detection...")
        result_1_1 = await step_1_1_domain_detection(nl_description)
        domain = result_1_1.get("domain", "")
        state["domain"] = domain
        print(f"  Domain: {domain}")
        
        # Step 1.2: Entity Mention Detection
        print("\n[Phase 1.2] Entity Mention Detection...")
        result_1_2 = await step_1_2_entity_mention_detection(nl_description)
        mentioned_entities = result_1_2.get("mentioned_entities", [])
        state["mentioned_entities"] = mentioned_entities
        try:
            mentioned_entity_names = [e.get("name", "") for e in mentioned_entities if isinstance(e, dict)]
        except Exception:
            mentioned_entity_names = []
        print(f"  Mentioned entities: {', '.join([n for n in mentioned_entity_names if n])}")
        
        # Step 1.3: Domain Inference
        print("\n[Phase 1.3] Domain Inference...")
        result_1_3 = await step_1_3_domain_inference(nl_description, domain_detection_result=result_1_1)
        inferred_domain = result_1_3.get("primary_domain", domain)
        state["domain"] = inferred_domain
        print(f"  Inferred domain: {inferred_domain}")
        
        # Step 1.4: Key Entity Extraction
        print("\n[Phase 1.4] Key Entity Extraction...")
        result_1_4 = await step_1_4_key_entity_extraction(
            nl_description,
            domain=inferred_domain,
            mentioned_entities=mentioned_entity_names,
            entity_mention_result=result_1_2,
        )
        key_entities = result_1_4.get("entities", [])
        state["key_entities"] = key_entities
        print(f"  Key entities: {len(key_entities)}")
        for entity in key_entities[:5]:
            print(f"    - {entity.get('name', '')}: {entity.get('description', '')}")
        
        # Step 1.5: Relation Mention Detection
        print("\n[Phase 1.5] Relation Mention Detection...")
        result_1_5 = await step_1_5_relation_mention_detection(nl_description, entities=key_entities)
        explicit_relations = result_1_5.get("relations", [])
        state["explicit_relations"] = explicit_relations
        print(f"  Explicit relations: {len(explicit_relations)}")
        
        # Step 1.6: Auxiliary Entity Suggestion
        print("\n[Phase 1.6] Auxiliary Entity Suggestion...")
        result_1_6 = await step_1_6_auxiliary_entity_suggestion(
            nl_description, key_entities=key_entities, domain=inferred_domain
        )
        auxiliary_entities = result_1_6.get("suggested_entities", [])
        state["auxiliary_entities"] = auxiliary_entities
        print(f"  Auxiliary entities: {len(auxiliary_entities)}")
        
        # Step 1.7: Entity Consolidation
        print("\n[Phase 1.7] Entity Consolidation...")
        result_1_7 = await step_1_7_entity_consolidation(
            key_entities, auxiliary_entities=auxiliary_entities, domain=inferred_domain, nl_description=nl_description
        )
        final_entity_names = result_1_7.get("final_entities", [])
        # Reconstruct consolidated entities from final entity list
        all_entities_dict = {e.get("name", ""): e for e in key_entities + auxiliary_entities}
        consolidated_entities = [all_entities_dict[name] for name in final_entity_names if name in all_entities_dict]
        state["entities"] = consolidated_entities
        print(f"  Consolidated entities: {len(consolidated_entities)}")

        # Step 1.75: Entity vs Relation Reclassification (associative-link guardrail)
        print("\n[Phase 1.75] Entity vs Relation Reclassification...")
        result_1_75 = await step_1_75_entity_relation_reclassification(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        consolidated_entities = result_1_75.get("entities", consolidated_entities)
        state["entities"] = consolidated_entities
        removed = result_1_75.get("removed_entity_names", [])
        print(f"  Reclassified/removed associative entities: {len(removed)}")
        if removed:
            for name in removed:
                print(f"    - {name}")

        # Step 1.76: Entity vs Attribute Guardrail (deterministic)
        print("\n[Phase 1.76] Entity vs Attribute Guardrail...")
        result_1_76 = await step_1_76_entity_attribute_guardrail(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        consolidated_entities = result_1_76.get("entities", consolidated_entities)
        state["entities"] = consolidated_entities
        state["attribute_candidates_phase1"] = result_1_76.get("attribute_candidates", [])
        removed_attr = result_1_76.get("removed_entity_names", [])
        print(f"  Reclassified/removed attribute-like entities: {len(removed_attr)}")
        if removed_attr:
            for name in removed_attr:
                print(f"    - {name}")
        
        # Step 1.8: Entity Cardinality
        print("\n[Phase 1.8] Entity Cardinality...")
        result_1_8 = await step_1_8_entity_cardinality(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        entity_info_1_8 = result_1_8.get("entity_info", [])
        print(f"  Entity cardinalities identified for {len(entity_info_1_8)} entities")
        
        # Step 1.9: Key Relations Extraction
        print("\n[Phase 1.9] Key Relations Extraction...")
        result_1_9 = await step_1_9_key_relations_extraction(
            consolidated_entities, nl_description, domain=inferred_domain, mentioned_relations=explicit_relations
        )
        key_relations = result_1_9.get("relations", [])
        state["relations"] = key_relations
        print(f"  Key relations: {len(key_relations)}")
        
        # Step 1.10: Schema Connectivity (with loop)
        print("\n[Phase 1.10] Schema Connectivity Validation (with loop)...")
        result_1_10 = await step_1_10_schema_connectivity_with_loop(
            consolidated_entities, key_relations, nl_description=nl_description,
            max_iterations=5, max_time_sec=180
        )
        connectivity_result = result_1_10.get("final_result", {})
        orphan_count = len(connectivity_result.get("orphan_entities", []))
        print(f"  Connectivity validation: orphan_entities={orphan_count}")
        
        # Step 1.11: Relation Cardinality
        print("\n[Phase 1.11] Relation Cardinality...")
        result_1_11 = await step_1_11_relation_cardinality(
            key_relations, consolidated_entities, nl_description=nl_description
        )
        # Step 1.11 returns {"relation_cardinalities": [list]} - convert to dict keyed by relation_id
        relation_cardinalities_list = result_1_11.get("relation_cardinalities", [])
        # Convert list to dict using same relation_id format as step_2_14: "Entity1+Entity2"
        relation_results_1_11 = {}
        for rel_card in relation_cardinalities_list:
            entities_in_rel = rel_card.get("entities", [])
            if entities_in_rel:
                relation_id = f"{'+'.join(sorted(entities_in_rel))}"
                relation_results_1_11[relation_id] = rel_card
        print(f"  Relation cardinalities identified for {len(relation_results_1_11)} relations")
        
        # Step 1.12: Relation Validation (with loop)
        print("\n[Phase 1.12] Relation Validation (with loop)...")
        # Get relation cardinalities from Step 1.11
        relation_cardinalities_list = result_1_11.get("relation_cardinalities", [])
        result_1_12 = await step_1_12_relation_validation_with_loop(
            consolidated_entities, key_relations, relation_cardinalities=relation_cardinalities_list,
            nl_description=nl_description, max_iterations=5, max_time_sec=180
        )
        validation_result = result_1_12.get("final_result", {})
        print(f"  Relation validation: {validation_result.get('validation_passed', False)}")
        
        print("\n[PASS] Phase 1 completed")
        phase_1_sec = timer_elapsed_seconds(phase_1_t0)
        log_phase_duration(logger, phase=1, seconds=phase_1_sec)
        print(f"[Timing] Phase 1 took {phase_1_sec:.2f} seconds")
        log_json(logger, "Phase 1 state snapshot", state)
        # Provide a lightweight ER-style snapshot early (entities + relations) so Phase 1
        # has a concrete "ER diagram" artifact even before Phase 3's compiled ER design.
        state["er_diagram_phase1"] = {
            "entities": state.get("entities", []),
            "relations": state.get("relations", []),
        }
        dump_phase_artifacts(logger=logger, phase=1, state=state)

        if max_phase and max_phase < 2:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 1.")
            return all_passed
        
        # ========================================================================
        # PHASE 2: Attribute Discovery & Schema Design
        # ========================================================================
        phase_2_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 2: Attribute Discovery & Schema Design")
        print("=" * 80)
        
        # Step 2.1: Attribute Count Detection
        print("\n[Phase 2.1] Attribute Count Detection...")
        result_2_1 = await step_2_1_attribute_count_detection_batch(consolidated_entities, nl_description)
        attr_count_results = result_2_1.get("entity_results", {})
        print(f"  Attribute counts detected for {len(attr_count_results)} entities")
        
        # Step 2.2: Intrinsic Attributes
        print("\n[Phase 2.2] Intrinsic Attributes...")
        result_2_2 = await step_2_2_intrinsic_attributes_batch(
            consolidated_entities, nl_description, domain=inferred_domain
        )
        entity_attr_results = result_2_2.get("entity_results", {})
        entity_attributes = {}
        for entity_name, attr_result in entity_attr_results.items():
            entity_attributes[entity_name] = attr_result.get("attributes", [])
        state["attributes"] = entity_attributes
        print(f"  Attributes identified for {len(entity_attributes)} entities")
        
        # Step 2.3: Attribute Synonym Detection
        print("\n[Phase 2.3] Attribute Synonym Detection...")
        result_2_3 = await step_2_3_attribute_synonym_detection_batch(
            consolidated_entities, entity_attributes, nl_description
        )
        synonym_results = result_2_3.get("entity_results", {})
        print(f"  Synonyms detected for {len(synonym_results)} entities")

        # Apply updated attributes if provided (LLM decided; we apply deterministically)
        updated_attrs = result_2_3.get("updated_attributes", {})
        if isinstance(updated_attrs, dict) and updated_attrs:
            entity_attributes = updated_attrs
            state["attributes"] = entity_attributes

        # Step 2.16: Cross-Entity Attribute Reconciliation (double precaution)
        print("\n[Phase 2.16] Cross-Entity Attribute Reconciliation...")
        result_2_16 = await step_2_16_cross_entity_attribute_reconciliation_batch(
            entities=consolidated_entities,
            attributes=entity_attributes,
            relations=key_relations,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        entity_attributes = result_2_16.get("updated_attributes", entity_attributes)
        state["attributes"] = entity_attributes
        print(f"  Cross-entity reconciliation processed for {len((result_2_16.get('entity_results') or {}))} entities")
        
        # Step 2.4: Composite Attribute Handling
        print("\n[Phase 2.4] Composite Attribute Handling...")
        entity_attr_lists = {
            name: [attr.get("name") if isinstance(attr, dict) else getattr(attr, "name", "") for attr in attrs]
            for name, attrs in entity_attributes.items()
        }
        result_2_4 = await step_2_4_composite_attribute_handling_batch(
            consolidated_entities, entity_attr_lists, nl_description
        )
        composite_results = result_2_4.get("entity_results", {})
        print(f"  Composite attributes handled for {len(composite_results)} entities")
        
        # Step 2.5: Temporal Attributes Detection
        print("\n[Phase 2.5] Temporal Attributes Detection...")
        result_2_5 = await step_2_5_temporal_attributes_detection_batch(
            consolidated_entities, entity_attr_lists, nl_description
        )
        temporal_results = result_2_5.get("entity_results", {})
        print(f"  Temporal attributes detected for {len(temporal_results)} entities")
        
        # Step 2.6: Naming Convention Validation
        print("\n[Phase 2.6] Naming Convention Validation...")
        entities_for_validation = [
            {"name": e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")}
            for e in consolidated_entities
        ]
        result_2_6 = step_2_6_naming_convention_validation(
            entities_for_validation, entity_attr_lists
        )
        naming_passed = result_2_6.get("validation_passed", False)
        print(f"  Naming validation: {'PASSED' if naming_passed else 'FAILED'}")
        
        # Step 2.7: Primary Key Identification
        print("\n[Phase 2.7] Primary Key Identification...")
        result_2_7 = await step_2_7_primary_key_identification_batch(
            consolidated_entities, entity_attr_lists, nl_description, domain=inferred_domain
        )
        pk_results = result_2_7.get("entity_results", {})
        entity_primary_keys = {
            name: pk_info.get("primary_key", [])
            for name, pk_info in pk_results.items()
        }
        state["primary_keys"] = entity_primary_keys
        print(f"  Primary keys identified for {len(entity_primary_keys)} entities")

        # If Step 2.7 chose a surrogate key not present in the attribute list, propagate it into attributes now.
        for ent_name, pk_list in (entity_primary_keys or {}).items():
            if not isinstance(pk_list, list):
                continue
            for pk_attr in pk_list:
                if not isinstance(pk_attr, str) or not pk_attr.strip():
                    continue
                if pk_attr not in (entity_attr_lists.get(ent_name) or []):
                    entity_attr_lists.setdefault(ent_name, []).append(pk_attr)
                attrs_obj_list = entity_attributes.get(ent_name) or []
                existing_names = {
                    (a.get("name") if isinstance(a, dict) else getattr(a, "name", ""))
                    for a in attrs_obj_list
                }
                if pk_attr not in existing_names:
                    attrs_obj_list.append(
                        {"name": pk_attr, "description": "Surrogate primary key (auto-added)", "type_hint": "integer"}
                    )
                    entity_attributes[ent_name] = attrs_obj_list
        state["attributes"] = entity_attributes
        
        # Step 2.8: Multivalued/Derived Detection
        print("\n[Phase 2.8] Multivalued/Derived Detection...")
        result_2_8 = await step_2_8_multivalued_derived_detection_batch(
            consolidated_entities, entity_attr_lists, nl_description, domain=inferred_domain
        )
        multivalued_results = result_2_8.get("entity_results", {})
        print(f"  Multivalued/derived attributes detected for {len(multivalued_results)} entities")
        
        # Step 2.9: Derived Attribute Formulas
        print("\n[Phase 2.9] Derived Attribute Formulas...")
        entity_derived_attrs = {}
        for entity_name, mv_result in multivalued_results.items():
            derived = mv_result.get("derived", [])
            if derived:
                entity_derived_attrs[entity_name] = derived
        # Build entity descriptions dict
        entity_descriptions = {
            e.get("name", "") if isinstance(e, dict) else getattr(e, "name", ""): 
            e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
            for e in consolidated_entities
        }
        result_2_9 = await step_2_9_derived_attribute_formulas_batch(
            entity_derived_attrs, entity_attr_lists, entity_descriptions=entity_descriptions, nl_description=nl_description
        )
        formula_results = result_2_9.get("entity_results", {})
        print(f"  Derived attribute formulas extracted for {len(formula_results)} entities")
        
        # Step 2.10: Unique Constraints
        print("\n[Phase 2.10] Unique Constraints...")
        result_2_10 = await step_2_10_unique_constraints_batch(
            consolidated_entities, entity_attr_lists, entity_primary_keys, nl_description, domain=inferred_domain
        )
        unique_results = result_2_10.get("entity_results", {})
        print(f"  Unique constraints identified for {len(unique_results)} entities")
        
        # Step 2.11: Nullability Constraints
        print("\n[Phase 2.11] Nullability Constraints...")
        result_2_11 = await step_2_11_nullability_constraints_batch(
            consolidated_entities, entity_attr_lists, entity_primary_keys, nl_description, domain=inferred_domain
        )
        nullability_results = result_2_11.get("entity_results", {})
        print(f"  Nullability constraints identified for {len(nullability_results)} entities")
        
        # Step 2.12: Default Values
        print("\n[Phase 2.12] Default Values...")
        result_2_12 = await step_2_12_default_values_batch(
            consolidated_entities, entity_attr_lists, entity_nullability=nullability_results, nl_description=nl_description, domain=inferred_domain
        )
        default_results = result_2_12.get("entity_results", {})
        print(f"  Default values identified for {len(default_results)} entities")
        
        # Step 2.13: Check Constraints (disabled for now; requires finalized relational schema)
        # Step 2.14: Entity Cleanup (LLM-driven; no Python deletions)
        print("\n[Phase 2.14] Entity Cleanup (relation-connecting attribute cleanup)...")
        result_2_14 = await step_2_14_entity_cleanup_batch(
            entities=consolidated_entities,
            entity_attributes=entity_attributes,
            primary_keys=entity_primary_keys,
            relations=key_relations,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        cleaned_attrs = result_2_14.get("updated_attributes", {}) or {}
        if isinstance(cleaned_attrs, dict) and cleaned_attrs:
            entity_attributes = cleaned_attrs
            state["attributes"] = entity_attributes

        # Rebuild name-only lists for downstream steps
        entity_attr_lists = {
            name: [attr.get("name") if isinstance(attr, dict) else getattr(attr, "name", "") for attr in attrs]
            for name, attrs in entity_attributes.items()
        }

        # Foreign keys are constructed later during relational schema compilation
        foreign_keys = []
        state["foreign_keys"] = foreign_keys
        state["entity_attributes"] = entity_attributes
        print(f"  Entity cleanup completed for {len(cleaned_attrs)} entities; foreign keys deferred")
        
        print("\n[PASS] Phase 2 completed")
        dump_phase_artifacts(logger=logger, phase=2, state=state)
        
        # ========================================================================
        # PHASE 3: Query Requirements & Schema Refinement
        # ========================================================================
        phase_3_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 3: Query Requirements & Schema Refinement")
        print("=" * 80)
        
        # Step 3.1: Information Need Identification
        print("\n[Phase 3.1] Information Need Identification (with loop)...")
        result_3_1 = await step_3_1_information_need_identification_with_loop(
            nl_description, consolidated_entities, key_relations, entity_attributes,
            entity_primary_keys, foreign_keys, domain=inferred_domain,
            max_iterations=5, max_time_sec=180
        )
        info_needs_result = result_3_1.get("final_result", {})
        information_needs = info_needs_result.get("information_needs", [])
        state["information_needs"] = information_needs
        print(f"  Information needs identified: {len(information_needs)}")
        for i, need in enumerate(information_needs[:3], 1):
            print(f"    {i}. {need.get('description', '')}")
        
        # Step 3.2: Information Completeness Check
        print("\n[Phase 3.2] Information Completeness Check...")
        phase3_cfg = get_phase3_config()
        result_3_2 = await step_3_2_information_completeness_batch(
            information_needs, consolidated_entities, key_relations, entity_attributes,
            entity_primary_keys, foreign_keys, nl_description=nl_description, domain=inferred_domain,
            max_iterations=phase3_cfg.step_3_2_max_iterations,
            max_time_sec=phase3_cfg.step_3_2_max_time_sec,
        )
        completeness_results = result_3_2.get("completeness_results", {})
        print(f"  Completeness checked for {len(completeness_results)} information needs")
        
        # Step 3.3: Phase 2 Steps with Enhanced Context
        print("\n[Phase 3.3] Phase 2 Steps with Enhanced Context...")
        result_3_3 = await step_3_3_phase2_reexecution(
            entities=consolidated_entities,
            relations=key_relations,
            attributes=entity_attributes,
            primary_keys=entity_primary_keys,
            information_needs=information_needs,
            completeness_results=completeness_results,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        
        # Merge new attributes from Step 3.3 into entity_attributes
        new_attributes = result_3_3.get("new_attributes", {})
        new_derived_attributes = result_3_3.get("new_derived_attributes", {})
        updated_primary_keys = result_3_3.get("updated_primary_keys", {})
        
        # Merge new intrinsic attributes
        for entity_name, new_attrs in new_attributes.items():
            if entity_name not in entity_attributes:
                entity_attributes[entity_name] = []
            # Add new attributes, avoiding duplicates
            existing_attr_names = {attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "") 
                                  for attr in entity_attributes[entity_name]}
            for new_attr in new_attrs:
                attr_name = new_attr.get("name", "") if isinstance(new_attr, dict) else getattr(new_attr, "name", "")
                if attr_name and attr_name not in existing_attr_names:
                    entity_attributes[entity_name].append(new_attr)
        
        # Merge new derived attributes
        for entity_name, new_derived_attrs in new_derived_attributes.items():
            if entity_name not in entity_attributes:
                entity_attributes[entity_name] = []
            # Add new derived attributes, avoiding duplicates
            existing_attr_names = {attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "") 
                                  for attr in entity_attributes[entity_name]}
            for new_derived_attr in new_derived_attrs:
                attr_name = new_derived_attr.get("name", "") if isinstance(new_derived_attr, dict) else getattr(new_derived_attr, "name", "")
                if attr_name and attr_name not in existing_attr_names:
                    entity_attributes[entity_name].append(new_derived_attr)
        
        # Merge updated primary keys
        for entity_name, new_pk in updated_primary_keys.items():
            entity_primary_keys[entity_name] = new_pk
        
        total_new_intrinsic = sum(len(attrs) for attrs in new_attributes.values())
        total_new_derived = sum(len(attrs) for attrs in new_derived_attributes.values())
        print(f"  Phase 2 re-execution completed: {total_new_intrinsic} new intrinsic attributes, {total_new_derived} new derived attributes")
        if updated_primary_keys:
            print(f"  Updated primary keys for {len(updated_primary_keys)} entities")
        
        # Step 3.4: ER Design Compilation
        print("\n[Phase 3.4] ER Design Compilation...")
        # Merge cardinality and participation info from Step 1.11 into relations
        # Use same relation_id format: "Entity1+Entity2" (sorted)
        relations_with_cardinality = []
        for relation in key_relations:
            relation_entities = relation.get("entities", [])
            if not relation_entities:
                relations_with_cardinality.append(relation)
                continue
            
            # Generate relation_id using same format as step_1_11 and step_2_14
            relation_id = f"{'+'.join(sorted(relation_entities))}"
            
            # Find matching relation result from Step 1.11
            rel_result = relation_results_1_11.get(relation_id)
            
            if rel_result:
                relation_copy = dict(relation) if isinstance(relation, dict) else relation.__dict__.copy()
                relation_copy["entity_cardinalities"] = rel_result.get("entity_cardinalities", {})
                relation_copy["entity_participations"] = rel_result.get("entity_participations", {})
                relations_with_cardinality.append(relation_copy)
            else:
                # No cardinality info found - log warning but continue
                logger.warning(
                    f"No cardinality info found for relation {relation_id} "
                    f"(entities: {relation_entities}). Proceeding without cardinality data."
                )
                relations_with_cardinality.append(relation)
        
        er_design = step_3_4_er_design_compilation(
            consolidated_entities, relations_with_cardinality, entity_attributes,
            entity_primary_keys, foreign_keys
        )
        state["er_design"] = er_design
        print(f"  ER design compiled: {len(er_design.get('entities', []))} entities, {len(er_design.get('relations', []))} relations")
        
        # Step 3.45: Junction Table Naming
        print("\n[Phase 3.45] Junction Table Naming...")
        junction_table_names = await step_3_45_junction_table_naming(
            relations=er_design.get("relations", []),
            entities=er_design.get("entities", []),
            nl_description=nl_description,
            domain=state.get("domain"),
        )
        state["junction_table_names"] = junction_table_names
        if junction_table_names:
            print(f"  Named {len(junction_table_names)} junction tables")
            for rel_key, name in list(junction_table_names.items())[:3]:
                print(f"    - {rel_key} -> {name}")
        else:
            print("  No junction tables needed")
        
        # Step 3.5: Relational Schema Compilation
        print("\n[Phase 3.5] Relational Schema Compilation...")
        relational_schema = step_3_5_relational_schema_compilation(
            er_design, foreign_keys, entity_primary_keys, constraints=state.get("constraints"), junction_table_names=junction_table_names
        )
        state["relational_schema"] = relational_schema
        tables = relational_schema.get("tables", [])
        print(f"  Relational schema compiled: {len(tables)} tables")
        for table in tables[:3]:
            print(f"    - {table.get('name', '')}: {len(table.get('columns', []))} columns")
        
        print("\n[PASS] Phase 3 completed")
        dump_phase_artifacts(logger=logger, phase=3, state=state)
        
        if max_phase and max_phase < 4:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 3.")
            return all_passed
        
        # ========================================================================
        # PHASE 4: Functional Dependencies & Data Types
        # ========================================================================
        phase_4_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 4: Functional Dependencies & Data Types")
        print("=" * 80)
        
        # Step 4.1: Functional Dependency Analysis
        print("\n[Phase 4.1] Functional Dependency Analysis...")
        # Extract derived attributes from Step 2.9
        entity_derived_attrs_dict = {}
        for entity_name, formula_result in formula_results.items():
            formulas = formula_result.get("formulas", {})
            entity_derived_attrs_dict[entity_name] = formulas
        
        # Merge derived attributes from Step 3.3 (new_derived_attributes)
        # Step 3.3 derived attributes have structure: [{"name": "...", "derivation_formula": "...", ...}, ...]
        if new_derived_attributes:
            for entity_name, derived_attrs_list in new_derived_attributes.items():
                if entity_name not in entity_derived_attrs_dict:
                    entity_derived_attrs_dict[entity_name] = {}
                # Extract derivation_formula from each derived attribute
                for derived_attr in derived_attrs_list:
                    if isinstance(derived_attr, dict):
                        attr_name = derived_attr.get("name", "")
                        formula = derived_attr.get("derivation_formula", "")
                    else:
                        attr_name = getattr(derived_attr, "name", "")
                        formula = getattr(derived_attr, "derivation_formula", "")
                    if attr_name and formula:
                        # Merge with existing formulas (Step 3.3 takes precedence if duplicate)
                        entity_derived_attrs_dict[entity_name][attr_name] = formula
        
        # Mine FDs AFTER relational schema compilation, using relational schema tables/columns as source of truth.
        result_4_1 = await step_4_1_functional_dependency_analysis_batch(
            consolidated_entities,
            entity_attributes,
            entity_primary_keys,
            relational_schema=relational_schema,
            entity_derived_attributes=entity_derived_attrs_dict if entity_derived_attrs_dict else None,
            nl_description=nl_description,
            domain=inferred_domain,
            max_iterations=3,
            max_time_sec=180,
        )
        fd_results = result_4_1.get("entity_results", {})
        functional_dependencies = {}
        for entity_name, fd_result in fd_results.items():
            functional_dependencies[entity_name] = fd_result.get("functional_dependencies", [])
        state["functional_dependencies"] = functional_dependencies
        total_fds = sum(len(fds) for fds in functional_dependencies.values())
        print(f"  Functional dependencies identified: {total_fds} total across {len(functional_dependencies)} entities")
        for entity_name, fds in functional_dependencies.items():
            if fds:
                print(f"    - {entity_name}: {len(fds)} dependencies")
                for fd in fds[:2]:
                    lhs = fd.get("lhs", [])
                    rhs = fd.get("rhs", [])
                    print(f"      {', '.join(lhs)} -> {', '.join(rhs)}")
        
        # Step 4.2: 3NF Normalization
        print("\n[Phase 4.2] 3NF Normalization...")
        normalized_schema = step_4_2_3nf_normalization(relational_schema, functional_dependencies)
        normalized_tables = normalized_schema.get("normalized_tables", [])
        decomposition_steps = normalized_schema.get("decomposition_steps", [])
        state["normalized_schema"] = normalized_schema
        print(f"  3NF normalization completed: {len(normalized_tables)} normalized tables")
        if decomposition_steps:
            print(f"  Decomposition steps: {len(decomposition_steps)}")
            for step in decomposition_steps[:2]:
                # Replace Unicode arrow with ASCII arrow for Windows console compatibility
                step_str = str(step).replace('\u2192', '->')
                print(f"    - {step_str}")
        
        # Step 4.3: Data Type Assignment
        print("\n[Phase 4.3] Data Type Assignment...")
        # Prepare data structures for Step 4.3
        # Note: check_results from Step 2.13 is disabled, so initialize as empty
        check_results = {}  # Step 2.13 is disabled, so no check constraints from Phase 2
        entity_check_constraints = {}
        for entity_name, check_result in check_results.items():
            constraints = check_result.get("check_constraints", {})
            if constraints:
                entity_check_constraints[entity_name] = constraints
        
        entity_unique_constraints = {}
        for entity_name, unique_result in unique_results.items():
            unique_attrs = unique_result.get("unique_attributes", [])
            if unique_attrs:
                entity_unique_constraints[entity_name] = unique_attrs
        
        entity_nullable_attributes = {}
        for entity_name, null_result in nullability_results.items():
            nullable = null_result.get("nullable_attributes", [])
            if nullable:
                entity_nullable_attributes[entity_name] = nullable
        
        # Build entity relations dict (which relations involve each entity)
        entity_relations_dict = {}
        for entity in consolidated_entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_relations_dict[entity_name] = [
                rel for rel in key_relations
                if entity_name in (rel.get("entities", []) if isinstance(rel, dict) else [])
            ]
        
        result_4_3 = await step_4_3_data_type_assignment_batch(
            consolidated_entities,
            entity_attributes,
            entity_primary_keys=entity_primary_keys,
            entity_check_constraints=entity_check_constraints if entity_check_constraints else None,
            entity_unique_constraints=entity_unique_constraints if entity_unique_constraints else None,
            entity_nullable_attributes=entity_nullable_attributes if entity_nullable_attributes else None,
            entity_relations=entity_relations_dict if entity_relations_dict else None,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        type_results = result_4_3.get("entity_results", {})
        entity_attribute_types = {}
        for entity_name, type_result in type_results.items():
            if not type_result.get("error"):
                entity_attribute_types[entity_name] = type_result.get("attribute_types", {})
        state["attribute_types"] = entity_attribute_types
        total_types = sum(len(types) for types in entity_attribute_types.values())
        print(f"  Data types assigned: {total_types} attributes across {len(entity_attribute_types)} entities")
        for entity_name, types in list(entity_attribute_types.items())[:2]:
            print(f"    - {entity_name}: {len(types)} attributes")
            for attr_name, type_info in list(types.items())[:2]:
                sql_type = type_info.get("type", "")
                size = type_info.get("size")
                precision = type_info.get("precision")
                scale = type_info.get("scale")
                type_str = sql_type
                if size:
                    type_str += f"({size})"
                elif precision and scale:
                    type_str += f"({precision},{scale})"
                elif precision:
                    type_str += f"({precision})"
                print(f"      {attr_name}: {type_str}")
        
        # Step 4.4: Categorical Detection
        print("\n[Phase 4.4] Categorical Detection...")
        result_4_4 = await step_4_4_categorical_detection_batch(
            consolidated_entities,
            entity_attributes,
            entity_attribute_types=entity_attribute_types if entity_attribute_types else None,
            nl_description=nl_description,
            domain=inferred_domain,
        )
        categorical_results = result_4_4.get("entity_results", {})
        entity_categorical_attributes = {}
        for entity_name, cat_result in categorical_results.items():
            if not cat_result.get("error"):
                categorical_attrs = cat_result.get("categorical_attributes", [])
                if categorical_attrs:
                    entity_categorical_attributes[entity_name] = categorical_attrs
        state["categorical_attributes"] = entity_categorical_attributes
        total_categorical = sum(len(attrs) for attrs in entity_categorical_attributes.values())
        print(f"  Categorical attributes detected: {total_categorical} across {len(entity_categorical_attributes)} entities")
        for entity_name, attrs in list(entity_categorical_attributes.items())[:2]:
            print(f"    - {entity_name}: {', '.join(attrs)}")
        
        # Step 4.5: Check Constraint Detection (for categorical attributes)
        print("\n[Phase 4.5] CHECK Constraint Detection (categorical)...")
        if entity_categorical_attributes:
            result_4_5 = await step_4_5_check_constraint_detection_batch(
                entity_categorical_attributes,
                entity_attributes,
                entity_attribute_types=entity_attribute_types if entity_attribute_types else None,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            check_cat_results = result_4_5.get("entity_results", {})
            total_check_constraints = sum(
                len(result.get("check_constraint_attributes", []))
                for result in check_cat_results.values()
            )
            print(f"  CHECK constraints for categorical attributes: {total_check_constraints} across {len(check_cat_results)} entities")
            for entity_name, result in list(check_cat_results.items())[:2]:
                check_attrs = result.get("check_constraint_attributes", [])
                if check_attrs:
                    print(f"    - {entity_name}: {', '.join(check_attrs)}")
        else:
            print("  No categorical attributes found, skipping CHECK constraint detection")
            check_cat_results = {}
        
        # Step 4.6: Categorical Value Extraction
        print("\n[Phase 4.6] Categorical Value Extraction...")
        if entity_categorical_attributes:
            result_4_6 = await step_4_6_categorical_value_extraction_batch(
                entity_categorical_attributes,
                entity_attributes,
                entity_attribute_types=entity_attribute_types,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            value_results = result_4_6.get("entity_results", {})
            entity_categorical_values = {}
            for entity_name, attr_results in value_results.items():
                entity_categorical_values[entity_name] = {}
                for attr_name, attr_result in attr_results.items():
                    if not attr_result.get("error"):
                        values = attr_result.get("values", [])
                        if values:
                            entity_categorical_values[entity_name][attr_name] = values
            state["categorical_values"] = entity_categorical_values
            total_values = sum(
                len(values) for attr_dict in entity_categorical_values.values()
                for values in attr_dict.values()
            )
            print(f"  Categorical values extracted: {total_values} total values")
            for entity_name, attr_dict in list(entity_categorical_values.items())[:2]:
                for attr_name, values in list(attr_dict.items())[:1]:
                    print(f"    - {entity_name}.{attr_name}: {', '.join(values)}")
        else:
            print("  No categorical attributes found, skipping value extraction")
            entity_categorical_values = {}
        
        # Step 4.7: Categorical Distribution
        print("\n[Phase 4.7] Categorical Distribution...")
        if entity_categorical_values:
            result_4_7 = await step_4_7_categorical_distribution_batch(
                entity_categorical_values,
                entity_attributes,
                nl_description=nl_description,
                domain=inferred_domain,
            )
            distribution_results = result_4_7.get("entity_results", {})
            total_distributions = sum(
                len([r for r in attr_results.values() if not r.get("error")])
                for attr_results in distribution_results.values()
            )
            print(f"  Categorical distributions determined: {total_distributions} across {len(distribution_results)} entities")
            for entity_name, attr_results in list(distribution_results.items())[:2]:
                for attr_name, dist_result in list(attr_results.items())[:1]:
                    if not dist_result.get("error"):
                        distribution = dist_result.get("distribution", {})
                        dist_sum = sum(distribution.values())
                        print(f"    - {entity_name}.{attr_name}: {len(distribution)} values, sum={dist_sum:.3f}")
                        # Show a few probabilities
                        for value, prob in list(distribution.items())[:3]:
                            print(f"      {value}: {prob:.3f}")
        else:
            print("  No categorical values found, skipping distribution determination")
        
        print("\n[PASS] Phase 4 completed")
        dump_phase_artifacts(logger=logger, phase=4, state=state)
        
        if max_phase and max_phase < 5:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 4.")
            return all_passed
        
        # ========================================================================
        # PHASE 5: DDL & SQL Generation
        # ========================================================================
        phase_5_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 5: DDL & SQL Generation")
        print("=" * 80)
        
        # Step 5.1: DDL Compilation
        print("\n[Phase 5.1] DDL Compilation...")
        ddl_result = step_5_1_ddl_compilation(
            normalized_schema=normalized_schema,
            data_types=entity_attribute_types if entity_attribute_types else None,
            check_constraints=entity_categorical_values if entity_categorical_values else None,
        )
        ddl_statements = ddl_result.get("ddl_statements", [])
        state["ddl_statements"] = ddl_statements
        print(f"  DDL statements generated: {len(ddl_statements)}")
        for i, ddl in enumerate(ddl_statements[:3], 1):
            table_name = ddl.split("CREATE TABLE")[1].split("(")[0].strip() if "CREATE TABLE" in ddl else "Unknown"
            print(f"    {i}. {table_name}")
            # Show first few lines of DDL
            ddl_lines = ddl.split('\n')[:5]
            for line in ddl_lines:
                if line.strip():
                    print(f"       {line.strip()}")
        
        # Step 5.2: DDL Validation
        print("\n[Phase 5.2] DDL Validation...")
        validation_result = step_5_2_ddl_validation(
            ddl_statements=ddl_statements,
            validate_with_db=True,
        )
        validation_passed = validation_result.get("validation_passed", False)
        syntax_errors = validation_result.get("syntax_errors", [])
        print(f"  Validation passed: {validation_passed}")
        if syntax_errors:
            print(f"  Syntax errors: {len(syntax_errors)}")
            for error in syntax_errors[:2]:
                print(f"    - {error.get('error', 'Unknown error')}")
        
        # Step 5.3: DDL Error Correction (if needed)
        if not validation_passed:
            print("\n[Phase 5.3] DDL Error Correction...")
            try:
                correction_result = await step_5_3_ddl_error_correction(
                    validation_errors=validation_result,
                    original_ddl=ddl_statements,
                    normalized_schema=normalized_schema,
                    relational_schema=relational_schema,
                )
                ir_patches = correction_result.get("ir_patches", [])
                print(f"  IR patches generated: {len(ir_patches)}")
                # Note: In a real scenario, we would apply patches and re-run 5.1
            except Exception as e:
                print(f"  Error correction failed: {e}")
        else:
            print("\n[Phase 5.3] DDL Error Correction skipped (validation passed)")
        
        # Step 5.4: Schema Creation
        print("\n[Phase 5.4] Schema Creation...")
        schema_creation_result = step_5_4_schema_creation(
            ddl_statements=ddl_statements,
            database_path=None,  # Use in-memory database
        )
        creation_success = schema_creation_result.get("success", False)
        tables_created = schema_creation_result.get("tables_created", [])
        print(f"  Schema creation: {'SUCCESS' if creation_success else 'FAILED'}")
        print(f"  Tables created: {', '.join(tables_created)}")
        
        # Step 5.5: SQL Query Generation
        print("\n[Phase 5.5] SQL Query Generation...")
        if information_needs:
            sql_results = await step_5_5_sql_query_generation_batch(
                information_needs=information_needs,
                normalized_schema=normalized_schema,
                data_types=entity_attribute_types if entity_attribute_types else None,
                relations=key_relations,
            )
            state["sql_queries"] = sql_results
            valid_queries = [r for r in sql_results if r.get("validation_status") == "valid"]
            print(f"  SQL queries generated: {len(sql_results)}")
            print(f"  Valid queries: {len(valid_queries)}")
            for i, query_result in enumerate(valid_queries[:3], 1):
                sql = query_result.get("sql", "")
                print(f"    {i}. {sql}")
            
            # Execute SQL queries against the created schema
            print("\n[Phase 5.5] Executing SQL queries against created schema...")
            import sqlite3
            query_execution_results = []
            try:
                # Re-create schema in a new in-memory database for query execution
                conn = sqlite3.connect(":memory:")
                cursor = conn.cursor()
                
                # Execute DDL statements to recreate schema
                for ddl in ddl_statements:
                    try:
                        cursor.execute(ddl)
                    except sqlite3.Error as e:
                        logger.warning(f"Failed to recreate schema for query execution: {e}")
                
                conn.commit()
                
                # Execute each valid SQL query
                executed_count = 0
                failed_count = 0
                for query_result in valid_queries:
                    sql = query_result.get("sql", "")
                    if not sql:
                        continue
                    
                    try:
                        cursor.execute(sql)
                        # Try to fetch results (for SELECT queries)
                        try:
                            results = cursor.fetchall()
                            query_execution_results.append({
                                "sql": sql,
                                "status": "success",
                                "rows_returned": len(results),
                                "error": None
                            })
                            executed_count += 1
                        except sqlite3.OperationalError:
                            # Not a SELECT query (INSERT, UPDATE, DELETE, etc.)
                            query_execution_results.append({
                                "sql": sql,
                                "status": "success",
                                "rows_returned": cursor.rowcount,
                                "error": None
                            })
                            executed_count += 1
                    except sqlite3.Error as e:
                        query_execution_results.append({
                            "sql": sql,
                            "status": "failed",
                            "rows_returned": 0,
                            "error": str(e)
                        })
                        failed_count += 1
                        logger.warning(f"Query execution failed: {sql[:100]}... Error: {e}")
                
                conn.close()
                
                print(f"  Query execution: {executed_count} succeeded, {failed_count} failed")
                state["query_execution_results"] = query_execution_results
                
            except Exception as e:
                logger.error(f"Failed to execute queries: {e}", exc_info=True)
                print(f"  Query execution failed: {e}")
        else:
            print("  No information needs found, skipping SQL query generation")
            state["sql_queries"] = []
        
        print("\n[PASS] Phase 5 completed")
        dump_phase_artifacts(logger=logger, phase=5, state=state)
        
        if max_phase and max_phase < 6:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 5.")
            return all_passed
        
        # ========================================================================
        # PHASE 6: Constraints & Distributions
        # ========================================================================
        phase_6_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 6: Constraints & Distributions")
        print("=" * 80)
        
        # Step 6.1: Constraint Detection (with loop)
        print("\n[Phase 6.1] Constraint Detection (with loop)...")
        constraint_result = await step_6_1_constraint_detection_with_loop(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            max_iterations=5,
            max_time_sec=180,
        )
        
        # Handle None result (e.g., if loop timed out with 0 iterations)
        if constraint_result is None:
            logger.warning("Constraint detection returned None (likely timed out with 0 iterations). Using empty constraints.")
            constraint_result = {
                "statistical_constraints": [],
                "structural_constraints": [],
                "distribution_constraints": [],
                "other_constraints": [],
            }
        
        all_constraints = []
        for category in ["statistical_constraints", "structural_constraints", "distribution_constraints", "other_constraints"]:
            constraints_list = constraint_result.get(category, [])
            for constraint_desc in constraints_list:
                all_constraints.append({
                    "description": constraint_desc,
                    "category": category.replace("_constraints", ""),
                })
        
        print(f"  Constraint detection completed")
        print(f"  Total constraints: {len(all_constraints)}")
        print(f"    - Statistical: {len(constraint_result.get('statistical_constraints', []))}")
        print(f"    - Structural: {len(constraint_result.get('structural_constraints', []))}")
        print(f"    - Distribution: {len(constraint_result.get('distribution_constraints', []))}")
        print(f"    - Other: {len(constraint_result.get('other_constraints', []))}")
        
        # Step 6.2: Constraint Scope Analysis
        print("\n[Phase 6.2] Constraint Scope Analysis...")
        if all_constraints:
            scope_results = await step_6_2_constraint_scope_analysis_batch(
                constraints=all_constraints,
                normalized_schema=normalized_schema,
                phase2_constraints=check_results if check_results else None,
            )
            # Merge scope into constraints
            for constraint, scope_result in zip(all_constraints, scope_results):
                constraint["scope"] = scope_result
            print(f"  Scope analyzed for {len(scope_results)} constraints")
        else:
            print("  No constraints found, skipping scope analysis")
            scope_results = []
        
        # Step 6.3: Constraint Enforcement Strategy
        print("\n[Phase 6.3] Constraint Enforcement Strategy...")
        if all_constraints:
            enforcement_results = await step_6_3_constraint_enforcement_strategy_batch(
                constraints_with_scope=all_constraints,
                normalized_schema=normalized_schema,
                dsl_grammar=None,  # Could provide DSL grammar as external context
            )
            # Merge enforcement into constraints
            for constraint, enforcement_result in zip(all_constraints, enforcement_results):
                constraint["enforcement"] = enforcement_result
            print(f"  Enforcement strategies determined for {len(enforcement_results)} constraints")
        else:
            print("  No constraints found, skipping enforcement strategy")
            enforcement_results = []
        
        # Step 6.4: Constraint Conflict Detection
        print("\n[Phase 6.4] Constraint Conflict Detection...")
        conflict_result = step_6_4_constraint_conflict_detection(
            constraints=all_constraints,
        )
        conflicts = conflict_result.get("conflicts", [])
        conflict_validation_passed = conflict_result.get("validation_passed", False)
        print(f"  Conflict detection: {'PASSED' if conflict_validation_passed else 'FAILED'}")
        print(f"  Conflicts found: {len(conflicts)}")
        if conflicts:
            for conflict in conflicts:
                print(f"    - {conflict.get('constraint1', '')} vs {conflict.get('constraint2', '')}")
        
        # Step 6.5: Constraint Compilation
        print("\n[Phase 6.5] Constraint Compilation...")
        compiled_constraints = step_6_5_constraint_compilation(
            constraints=all_constraints,
        )
        state["constraints"] = compiled_constraints
        total_compiled = (
            len(compiled_constraints.get("statistical_constraints", [])) +
            len(compiled_constraints.get("structural_constraints", [])) +
            len(compiled_constraints.get("distribution_constraints", [])) +
            len(compiled_constraints.get("other_constraints", []))
        )
        print(f"  Constraints compiled: {total_compiled} total")
        
        print("\n[PASS] Phase 6 completed")
        dump_phase_artifacts(logger=logger, phase=6, state=state)
        
        if max_phase and max_phase < 7:
            print(f"\n[STOP] Reached max_phase={max_phase}. Stopping after Phase 6.")
            return all_passed
        
        # ========================================================================
        # PHASE 7: Generation Strategies
        # ========================================================================
        phase_7_t0 = timer_start()
        print("\n" + "=" * 80)
        print("PHASE 7: Generation Strategies")
        print("=" * 80)
        
        # Prepare attribute lists by type
        numerical_attributes = []
        text_attributes = []
        boolean_attributes = []
        
        for entity_name, attrs in entity_attributes.items():
            entity_obj = next((e for e in consolidated_entities if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name), None)
            entity_desc = entity_obj.get("description", "") if isinstance(entity_obj, dict) else getattr(entity_obj, "description", "") if entity_obj else ""
            
            # Get attribute types for this entity
            attr_types = entity_attribute_types.get(entity_name, {}) if entity_attribute_types else {}
            
            for attr in attrs:
                attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
                attr_desc = attr.get("description", "") if isinstance(attr, dict) else getattr(attr, "description", "")
                attr_type_info = attr_types.get(attr_name, {})
                sql_type = attr_type_info.get("type", "") if attr_type_info else ""
                
                attr_meta = {
                    "entity_name": entity_name,
                    "attribute_name": attr_name,
                    "attribute_description": attr_desc,
                    "attribute_type": sql_type,
                    "entity_description": entity_desc,
                    "relations": [rel for rel in key_relations if entity_name in (rel.get("entities", []) if isinstance(rel, dict) else [])],
                    "entity_cardinality": entity_results_1_8.get(entity_name, {}).get("cardinality", "") if entity_results_1_8 else "",
                }
                
                # Categorize by type
                if sql_type.upper() in ("INTEGER", "INT", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
                    numerical_attributes.append(attr_meta)
                elif sql_type.upper() in ("VARCHAR", "CHAR", "TEXT", "STRING"):
                    text_attributes.append(attr_meta)
                elif sql_type.upper() in ("BOOLEAN", "BOOL", "TINYINT"):
                    boolean_attributes.append(attr_meta)
        
        # Step 7.1: Numerical Range Definition
        print("\n[Phase 7.1] Numerical Range Definition...")
        if numerical_attributes:
            # Build constraints map for numerical attributes
            constraints_map = {}
            for attr in numerical_attributes:
                key = f"{attr['entity_name']}.{attr['attribute_name']}"
                # Find constraints affecting this attribute
                affecting = [
                    c for c in all_constraints
                    if key in c.get("scope", {}).get("affected_attributes", [])
                ]
                if affecting:
                    constraints_map[key] = affecting
            
            numerical_strategies = await step_7_1_numerical_range_definition_batch(
                numerical_attributes=numerical_attributes,
                constraints_map=constraints_map if constraints_map else None,
            )
            state["numerical_strategies"] = numerical_strategies
            print(f"  Numerical ranges defined for {len(numerical_strategies)} attributes")
        else:
            print("  No numerical attributes found, skipping range definition")
            numerical_strategies = {}
        
        # Step 7.2: Text Generation Strategy
        print("\n[Phase 7.2] Text Generation Strategy...")
        if text_attributes:
            text_strategies = await step_7_2_text_generation_strategy_batch(
                text_attributes=text_attributes,
                generator_catalog=None,  # Could provide generator catalog as external context
            )
            state["text_strategies"] = text_strategies
            print(f"  Text generators selected for {len(text_strategies)} attributes")
        else:
            print("  No text attributes found, skipping text generation strategy")
            text_strategies = {}
        
        # Step 7.3: Boolean Dependency Analysis
        print("\n[Phase 7.3] Boolean Dependency Analysis...")
        if boolean_attributes:
            # Build related attributes map
            related_attributes_map = {}
            for attr in boolean_attributes:
                key = f"{attr['entity_name']}.{attr['attribute_name']}"
                # Include all attributes from same entity and related entities
                related = []
                for other_attr in numerical_attributes + text_attributes + boolean_attributes:
                    if other_attr["entity_name"] == attr["entity_name"] or \
                       any(attr["entity_name"] in (rel.get("entities", []) if isinstance(rel, dict) else []) for rel in attr.get("relations", [])):
                        related.append(other_attr)
                related_attributes_map[key] = related
            
            boolean_strategies = await step_7_3_boolean_dependency_analysis_batch(
                boolean_attributes=boolean_attributes,
                related_attributes_map=related_attributes_map if related_attributes_map else None,
                dsl_grammar=None,  # Could provide DSL grammar as external context
            )
            state["boolean_strategies"] = boolean_strategies
            print(f"  Boolean dependencies analyzed for {len(boolean_strategies)} attributes")
        else:
            print("  No boolean attributes found, skipping boolean dependency analysis")
            boolean_strategies = {}
        
        # Step 7.4: Data Volume Specifications
        print("\n[Phase 7.4] Data Volume Specifications...")
        # Prepare entities with cardinality info
        entities_with_cardinality = []
        for entity in consolidated_entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            cardinality_info = entity_results_1_8.get(entity_name, {}) if entity_results_1_8 else {}
            entities_with_cardinality.append({
                "name": entity_name,
                "cardinality": cardinality_info.get("cardinality", ""),
                "cardinality_hint": cardinality_info.get("cardinality_hint", ""),
                "table_type": cardinality_info.get("table_type", ""),
            })
        
        volume_result = await step_7_4_data_volume_specifications(
            entities=entities_with_cardinality,
            nl_description=nl_description,
        )
        entity_volumes = volume_result.get("entity_volumes", {})
        state["entity_volumes"] = entity_volumes
        print(f"  Data volumes specified for {len(entity_volumes)} entities")
        for entity_name, volume in list(entity_volumes.items())[:3]:
            expected = volume.get("expected_rows", 0)
            print(f"    - {entity_name}: {expected:,} expected rows")
        
        # Step 7.5: Partitioning Strategy
        print("\n[Phase 7.5] Partitioning Strategy...")
        # Prepare entities with volumes for partitioning
        entities_with_volumes = []
        for entity_name, volume in entity_volumes.items():
            entity_obj = next((e for e in consolidated_entities if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name), None)
            # Find table columns from normalized schema
            table_obj = next((t for t in normalized_tables if t.get("name", "") == entity_name), None)
            columns = table_obj.get("columns", []) if table_obj else []
            
            entities_with_volumes.append({
                "entity_name": entity_name,
                "entity_volume": volume,
                "table_type": entity_results_1_8.get(entity_name, {}).get("table_type", "") if entity_results_1_8 else "",
                "columns": columns,
                "nl_description": nl_description,
            })
        
        partitioning_strategies = await step_7_5_partitioning_strategy_batch(
            entities_with_volumes=entities_with_volumes,
        )
        state["partitioning_strategies"] = partitioning_strategies
        needs_partitioning = sum(1 for s in partitioning_strategies.values() if s.get("needs_partitioning", False))
        print(f"  Partitioning strategies determined for {len(partitioning_strategies)} entities")
        print(f"  Entities needing partitioning: {needs_partitioning}")
        
        # Step 7.6: Distribution Compilation
        print("\n[Phase 7.6] Distribution Compilation...")
        # Prepare categorical strategies from Phase 4.7
        categorical_strategies = {}
        if entity_categorical_values:
            # Get distributions from Phase 4.7 results
            # distribution_results is defined in Phase 4.7 step above
            for entity_name, attr_dict in entity_categorical_values.items():
                for attr_name, values in attr_dict.items():
                    key = f"{entity_name}.{attr_name}"
                    # Try to get distribution from Phase 4.7 results
                    if 'distribution_results' in locals() and distribution_results:
                        attr_results = distribution_results.get(entity_name, {})
                        dist_result = attr_results.get(attr_name, {}) if isinstance(attr_results, dict) else {}
                        if dist_result and not dist_result.get("error"):
                            distribution = dist_result.get("distribution", {})
                            if distribution:
                                categorical_strategies[key] = {
                                    "distribution": distribution
                                }
                                continue
                    # Fallback: create a simple uniform distribution
                    if values:
                        uniform_prob = 1.0 / len(values)
                        categorical_strategies[key] = {
                            "distribution": {v: uniform_prob for v in values}
                        }
        
        generation_ir = step_7_6_distribution_compilation(
            numerical_strategies=numerical_strategies,
            text_strategies=text_strategies,
            boolean_strategies=boolean_strategies,
            categorical_strategies=categorical_strategies if categorical_strategies else None,
            entity_volumes=entity_volumes,
            partitioning_strategies=partitioning_strategies,
        )
        state["generation_ir"] = generation_ir
        column_gen_specs = generation_ir.get("column_gen_specs", [])
        print(f"  GenerationIR compiled: {len(column_gen_specs)} column generation specs")
        print(f"  Entity volumes: {len(generation_ir.get('entity_volumes', {}))}")
        print(f"  Partitioning strategies: {len(generation_ir.get('partitioning_strategies', {}))}")
        
        print("\n[PASS] Phase 7 completed")
        dump_phase_artifacts(logger=logger, phase=7, state=state)
        
        # ========================================================================
        # SUMMARY
        # ========================================================================
        print("\n" + "=" * 80)
        print("INTEGRATION TEST SUMMARY (Phases 1-7)")
        print("=" * 80)
        print(f"Phase 1: {len(consolidated_entities)} entities, {len(key_relations)} relations")
        print(f"Phase 2: Attributes for {len(entity_attributes)} entities, {len(entity_primary_keys)} primary keys, {len(foreign_keys)} foreign keys")
        print(f"Phase 3: {len(information_needs)} information needs, {len(tables)} tables")
        categorical_count = sum(len(attrs) for attrs in entity_categorical_attributes.values()) if entity_categorical_attributes else 0
        print(f"Phase 4: {total_fds} functional dependencies, {len(normalized_tables)} normalized tables, {total_types} data types, {categorical_count} categorical attributes")
        sql_queries = state.get('sql_queries', [])
        query_exec_results = state.get('query_execution_results', [])
        executed_success = len([r for r in query_exec_results if r.get('status') == 'success']) if query_exec_results else 0
        print(f"Phase 5: {len(ddl_statements)} DDL statements, {len(sql_queries)} SQL queries generated, {executed_success} queries executed successfully")
        total_constraints = (
            len(compiled_constraints.get("statistical_constraints", [])) +
            len(compiled_constraints.get("structural_constraints", [])) +
            len(compiled_constraints.get("distribution_constraints", [])) +
            len(compiled_constraints.get("other_constraints", []))
        )
        print(f"Phase 6: {total_constraints} constraints compiled")
        print(f"Phase 7: {len(column_gen_specs)} column generation specs, {len(entity_volumes)} entity volumes")
        print("\n[PASS] Phases 1-7 completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed


def process_all_descriptions_sequential(
    descriptions_file: str,
    phases: str = "1-7",
    max_phase: int = None
):
    """Process all NL descriptions from a file sequentially."""
    print("=" * 80)
    print("SEQUENTIAL INTEGRATION TEST")
    print("=" * 80)
    
    # Read all descriptions
    descriptions = read_nl_descriptions(descriptions_file)
    total = len(descriptions)
    print(f"\nFound {total} NL descriptions to process")
    
    if total == 0:
        print("No descriptions found in file!")
        return False
    
    if max_phase:
        print(f"Running up to Phase {max_phase}")
    
    print("Processing sequentially (one at a time)")
    print("=" * 80)
    
    # Process descriptions sequentially
    results: List[Tuple[int, bool, str]] = []
    
    for idx, description in enumerate(descriptions, start=1):
        try:
            # CRITICAL: Reset rate limiter to create new semaphores for new event loop
            # Each asyncio.run() creates a new event loop, and semaphores are bound to their event loop
            reset_rate_limiter()
            
            print(f"\n{'=' * 80}")
            print(f"Processing description {idx} of {total}")
            print(f"{'=' * 80}")
            
            if phases == "1-5":
                success = asyncio.run(test_phases_1_to_5_integration(description, idx))
            else:
                success = asyncio.run(test_phases_1_2_3_4_5_6_7_integration(description, idx, max_phase=max_phase))
            
            results.append((idx, success, ""))
            print(f"\n[Description {idx}] {'SUCCESS' if success else 'FAILED'}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"\n[Description {idx}] Error: {error_msg}")
            import traceback
            traceback.print_exc()
            results.append((idx, False, error_msg))
    
    # Print summary
    print("\n" + "=" * 80)
    print("SEQUENTIAL TEST SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for _, success, _ in results if success)
    failed_count = total - success_count
    
    print(f"Total descriptions: {total}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")
    
    if failed_count > 0:
        print("\nFailed descriptions:")
        for idx, success, error in results:
            if not success:
                print(f"  Description {idx}: {error if error else 'Unknown error'}")
    
    print("=" * 80)
    return failed_count == 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run integration tests")
    parser.add_argument(
        "--phases",
        type=str,
        choices=["1-5", "1-7"],
        default="1-7",
        help="Which phases to test (default: 1-7)"
    )
    parser.add_argument(
        "--descriptions-file",
        type=str,
        default="nl_descriptions.txt",
        help="Path to file containing NL descriptions (default: nl_descriptions.txt)"
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Process only a single description (for testing)"
    )
    parser.add_argument(
        "--max-phase",
        type=int,
        default=None,
        help="Maximum phase to run (1-7). If specified, stops after this phase."
    )
    args = parser.parse_args()
    
    # Determine descriptions file path
    descriptions_file = args.descriptions_file
    if not Path(descriptions_file).is_absolute():
        # Try relative to project root
        project_root = Path(__file__).parent.parent.parent
        descriptions_file = project_root / descriptions_file
    
    if args.single:
        # Single description mode (original behavior)
        if args.phases == "1-5":
            success = asyncio.run(test_phases_1_to_5_integration())
        else:
            success = asyncio.run(test_phases_1_2_3_4_5_6_7_integration(max_phase=args.max_phase))
    else:
        # Sequential mode
        success = process_all_descriptions_sequential(
            str(descriptions_file),
            args.phases,
            max_phase=args.max_phase
        )
    
    sys.exit(0 if success else 1)

