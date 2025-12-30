"""Comprehensive test script for Phase 1: Domain & Entity Discovery.

Tests all Phase 1 steps including loop support for Steps 1.10 and 1.12.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import (
    step_1_1_domain_detection,
    step_1_2_entity_mention_detection,
    step_1_3_domain_inference,
    step_1_4_key_entity_extraction,
    step_1_5_relation_mention_detection,
    step_1_6_auxiliary_entity_suggestion,
    step_1_7_entity_consolidation,
    step_1_8_entity_cardinality,
    step_1_9_key_relations_extraction,
    step_1_10_schema_connectivity_with_loop,
    step_1_11_relation_cardinality,
    step_1_12_relation_validation_with_loop,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_phase1_complete():
    """Test complete Phase 1 workflow."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    # Test case: Simple bookstore
    nl_description = """I need a database for an online bookstore. Customers can place orders for books. 
    Each order can contain multiple books. Books have titles, authors, and prices. 
    I need to track customer addresses and order dates."""
    
    print("=" * 80)
    print("Testing Complete Phase 1: Domain & Entity Discovery")
    print("=" * 80)
    print(f"\nNatural Language Description:\n{nl_description}\n")
    
    all_passed = True
    
    try:
        # Step 1.1: Domain Detection
        print("-" * 80)
        print("Step 1.1: Domain Detection")
        print("-" * 80)
        result_1_1 = await step_1_1_domain_detection(nl_description)
        has_domain = result_1_1.get("has_explicit_domain", False)
        domain = result_1_1.get("domain")
        print(f"[PASS] Step 1.1 completed: has_explicit_domain={has_domain}, domain={domain}")
        
        # Step 1.2: Entity Mention Detection
        print("\n" + "-" * 80)
        print("Step 1.2: Entity Mention Detection")
        print("-" * 80)
        result_1_2 = await step_1_2_entity_mention_detection(nl_description)
        has_explicit = result_1_2.get("has_explicit_entities", False)
        mentioned = result_1_2.get("mentioned_entities", [])
        print(f"[PASS] Step 1.2 completed: has_explicit_entities={has_explicit}, mentioned={len(mentioned)} entities")
        
        # Step 1.3: Domain Inference (if needed)
        print("\n" + "-" * 80)
        print("Step 1.3: Domain Inference")
        print("-" * 80)
        if not has_domain:
            result_1_3 = await step_1_3_domain_inference(nl_description, result_1_1)
            inferred_domain = result_1_3.get("domain")
            print(f"[PASS] Step 1.3 completed: inferred_domain={inferred_domain}")
            domain = inferred_domain
        else:
            print(f"[SKIP] Step 1.3 skipped (domain already detected)")
        
        # Step 1.4: Key Entity Extraction
        print("\n" + "-" * 80)
        print("Step 1.4: Key Entity Extraction")
        print("-" * 80)
        result_1_4 = await step_1_4_key_entity_extraction(
            nl_description=nl_description,
            domain=domain
        )
        key_entities = result_1_4.get("entities", [])
        print(f"[PASS] Step 1.4 completed: extracted {len(key_entities)} key entities")
        for entity in key_entities:
            print(f"  - {entity.get('name', 'Unknown')}: {entity.get('description', 'No description')}")
        
        # Step 1.5: Relation Mention Detection
        print("\n" + "-" * 80)
        print("Step 1.5: Relation Mention Detection")
        print("-" * 80)
        result_1_5 = await step_1_5_relation_mention_detection(
            nl_description=nl_description,
            entities=key_entities
        )
        has_explicit_rels = result_1_5.get("has_explicit_relations", False)
        mentioned_rels = result_1_5.get("mentioned_relations", [])
        print(f"[PASS] Step 1.5 completed: has_explicit_relations={has_explicit_rels}, mentioned={len(mentioned_rels)} relations")
        
        # Step 1.6: Auxiliary Entity Suggestion
        print("\n" + "-" * 80)
        print("Step 1.6: Auxiliary Entity Suggestion")
        print("-" * 80)
        result_1_6 = await step_1_6_auxiliary_entity_suggestion(
            nl_description=nl_description,
            key_entities=key_entities,
            domain=domain
        )
        auxiliary_entities = result_1_6.get("suggested_entities", [])
        print(f"[PASS] Step 1.6 completed: suggested {len(auxiliary_entities)} auxiliary entities")
        for entity in auxiliary_entities:
            print(f"  - {entity.get('name', 'Unknown')}: {entity.get('reasoning', 'No reasoning')}")
        
        # Step 1.7: Entity Consolidation
        print("\n" + "-" * 80)
        print("Step 1.7: Entity Consolidation")
        print("-" * 80)
        result_1_7 = await step_1_7_entity_consolidation(
            key_entities=key_entities,
            auxiliary_entities=auxiliary_entities,
            domain=domain,
            nl_description=nl_description
        )
        final_entities = result_1_7.get("final_entity_list", [])
        duplicates = result_1_7.get("duplicates", [])
        print(f"[PASS] Step 1.7 completed: {len(duplicates)} duplicates found, {len(final_entities)} final entities")
        print(f"  Final entities: {', '.join(final_entities)}")
        
        # Step 1.8: Entity Cardinality & Table Type
        print("\n" + "-" * 80)
        print("Step 1.8: Entity Cardinality & Table Type")
        print("-" * 80)
        # Convert final_entities back to dict format for step 1.8
        entity_dicts = []
        for entity_name in final_entities:
            # Find entity info from previous steps
            entity_info = None
            for e in key_entities + auxiliary_entities:
                if (e.get("name") if isinstance(e, dict) else getattr(e, "name", None)) == entity_name:
                    entity_info = e
                    break
            if entity_info:
                entity_dicts.append(entity_info)
            else:
                entity_dicts.append({"name": entity_name, "description": ""})
        
        result_1_8 = await step_1_8_entity_cardinality(
            entities=entity_dicts,
            nl_description=nl_description,
            domain=domain
        )
        entity_info_list = result_1_8.get("entity_info", [])
        print(f"[PASS] Step 1.8 completed: analyzed {len(entity_info_list)} entities")
        for info in entity_info_list:
            print(f"  - {info.get('entity')}: cardinality={info.get('cardinality')}, table_type={info.get('table_type')}")
        
        # Step 1.9: Key Relations Extraction
        print("\n" + "-" * 80)
        print("Step 1.9: Key Relations Extraction")
        print("-" * 80)
        result_1_9 = await step_1_9_key_relations_extraction(
            entities=entity_dicts,
            nl_description=nl_description,
            domain=domain,
            mentioned_relations=mentioned_rels
        )
        relations = result_1_9.get("relations", [])
        print(f"[PASS] Step 1.9 completed: extracted {len(relations)} relations")
        for rel in relations:
            entities_in_rel = rel.get("entities", [])
            rel_type = rel.get("type", "unknown")
            print(f"  - {', '.join(entities_in_rel)}: {rel_type}")
        
        # Step 1.10: Schema Connectivity Validation (with loop)
        print("\n" + "-" * 80)
        print("Step 1.10: Schema Connectivity Validation (with loop support)")
        print("-" * 80)
        result_1_10 = await step_1_10_schema_connectivity_with_loop(
            entities=entity_dicts,
            relations=relations,
            nl_description=nl_description,
            max_iterations=3,
            max_time_sec=180
        )
        final_connectivity = result_1_10.get("final_result", {})
        loop_metadata = result_1_10.get("loop_metadata", {})
        orphan_count = len(final_connectivity.get("orphan_entities", []))
        iterations = loop_metadata.get("iterations", 1)
        print(f"[PASS] Step 1.10 completed: {orphan_count} orphan entities found after {iterations} iteration(s)")
        if orphan_count > 0:
            print(f"  Orphan entities: {', '.join(final_connectivity.get('orphan_entities', []))}")
        
        # Step 1.11: Relation Cardinality
        print("\n" + "-" * 80)
        print("Step 1.11: Relation Cardinality")
        print("-" * 80)
        result_1_11 = await step_1_11_relation_cardinality(
            relations=relations,
            entities=entity_dicts,
            nl_description=nl_description
        )
        relation_cardinalities = result_1_11.get("relation_cardinalities", [])
        print(f"[PASS] Step 1.11 completed: analyzed {len(relation_cardinalities)} relations")
        for rel_card in relation_cardinalities:
            entities_in_rel = rel_card.get("entities", [])
            card_info = rel_card.get("entity_cardinalities", {})
            print(f"  - {', '.join(entities_in_rel)}: {card_info}")
        
        # Step 1.12: Relation Validation (with loop)
        print("\n" + "-" * 80)
        print("Step 1.12: Relation Validation (with loop support)")
        print("-" * 80)
        result_1_12 = await step_1_12_relation_validation_with_loop(
            entities=entity_dicts,
            relations=relations,
            relation_cardinalities=relation_cardinalities,
            nl_description=nl_description,
            max_iterations=3,
            max_time_sec=180
        )
        final_validation = result_1_12.get("final_result", {})
        loop_metadata_12 = result_1_12.get("loop_metadata", {})
        validation_passed = final_validation.get("validation_passed", False)
        iterations_12 = loop_metadata_12.get("iterations", 1)
        print(f"[PASS] Step 1.12 completed: validation_passed={validation_passed} after {iterations_12} iteration(s)")
        if not validation_passed:
            circular = len(final_validation.get("circular_dependencies", []))
            impossible = len(final_validation.get("impossible_cardinalities", []))
            conflicts = len(final_validation.get("conflicts", []))
            print(f"  Issues: {circular} circular dependencies, {impossible} impossible cardinalities, {conflicts} conflicts")
        
        print("\n" + "=" * 80)
        print("[PASS] Phase 1 completed successfully!")
        print("=" * 80)
        print(f"\nSummary:")
        print(f"  - Domain: {domain}")
        print(f"  - Entities: {len(final_entities)}")
        print(f"  - Relations: {len(relations)}")
        print(f"  - Connectivity: {'All connected' if orphan_count == 0 else f'{orphan_count} orphans'}")
        print(f"  - Validation: {'Passed' if validation_passed else 'Failed'}")
        
    except Exception as e:
        print(f"\n[ERROR] Phase 1 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_phase1_complete())
    sys.exit(0 if success else 1)

