"""Comprehensive test script for Phase 2: Attribute Discovery & Schema Design.

Tests all implemented Phase 2 steps (2.1-2.5).
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import (
    step_2_1_attribute_count_detection_batch,
    step_2_2_intrinsic_attributes_batch,
    step_2_3_attribute_synonym_detection_batch,
    step_2_4_composite_attribute_handling_batch,
    step_2_5_temporal_attributes_detection_batch,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_phase2_steps():
    """Test Phase 2 steps with a realistic scenario."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    # Test case: Bookstore entities from Phase 1
    nl_description = """I need a database for an online bookstore. Customers can place orders for books. 
    Each order can contain multiple books. Books have titles, authors, ISBN, publication date, and prices. 
    Customers have names, email addresses, phone numbers, and shipping addresses. 
    Orders have order dates, total amounts, and status (pending, shipped, delivered)."""
    
    # Entities from Phase 1 (simplified for testing)
    entities = [
        {"name": "Customer", "description": "An individual who purchases books from the online bookstore"},
        {"name": "Book", "description": "An item available for purchase in the online bookstore"},
        {"name": "Order", "description": "A transaction made by a customer to purchase one or more books"},
    ]
    
    domain = "e-commerce"
    
    print("=" * 80)
    print("Testing Phase 2: Attribute Discovery & Schema Design")
    print("=" * 80)
    print(f"\nNatural Language Description:\n{nl_description}\n")
    print(f"Entities to process: {', '.join(e['name'] for e in entities)}\n")
    
    all_passed = True
    
    try:
        # Step 2.1: Attribute Count Detection
        print("-" * 80)
        print("Step 2.1: Attribute Count Detection")
        print("-" * 80)
        result_2_1 = await step_2_1_attribute_count_detection_batch(
            entities=entities,
            nl_description=nl_description
        )
        entity_results_2_1 = result_2_1.get("entity_results", {})
        print(f"[PASS] Step 2.1 completed: processed {len(entity_results_2_1)} entities")
        for entity_name, result in entity_results_2_1.items():
            has_count = result.get("has_explicit_count", False)
            count = result.get("count")
            explicit_attrs = result.get("explicit_attributes", [])
            print(f"  - {entity_name}: has_explicit_count={has_count}, count={count}, explicit_attributes={len(explicit_attrs)}")
        
        # Step 2.2: Intrinsic Attributes
        print("\n" + "-" * 80)
        print("Step 2.2: Intrinsic Attributes")
        print("-" * 80)
        result_2_2 = await step_2_2_intrinsic_attributes_batch(
            entities=entities,
            nl_description=nl_description,
            attribute_count_results=result_2_1,
            domain=domain
        )
        entity_results_2_2 = result_2_2.get("entity_results", {})
        print(f"[PASS] Step 2.2 completed: processed {len(entity_results_2_2)} entities")
        for entity_name, result in entity_results_2_2.items():
            attributes = result.get("attributes", [])
            print(f"  - {entity_name}: {len(attributes)} attributes extracted")
            for attr in attributes[:3]:  # Show first 3
                attr_name = attr.get("name", "Unknown")
                attr_desc = attr.get("description", "")
                print(f"    * {attr_name}: {attr_desc if attr_desc else 'No description'}")
            if len(attributes) > 3:
                print(f"    ... and {len(attributes) - 3} more")
        
        # Step 2.3: Attribute Synonym Detection
        print("\n" + "-" * 80)
        print("Step 2.3: Attribute Synonym Detection")
        print("-" * 80)
        # Build entity_attributes dict for step 2.3 (needs list of attribute dicts)
        entity_attributes = {}
        for entity_name, result in entity_results_2_2.items():
            attributes = result.get("attributes", [])
            entity_attributes[entity_name] = attributes  # Keep as dicts for step 2.3
        
        result_2_3 = await step_2_3_attribute_synonym_detection_batch(
            entities=entities,
            entity_attributes=entity_attributes,
            nl_description=nl_description
        )
        entity_results_2_3 = result_2_3.get("entity_results", {})
        print(f"[PASS] Step 2.3 completed: processed {len(entity_results_2_3)} entities")
        for entity_name, result in entity_results_2_3.items():
            synonyms = result.get("synonyms", [])
            merged = result.get("merged_attributes", [])
            final_attrs = result.get("final_attribute_list", [])
            print(f"  - {entity_name}: {len(synonyms)} synonym groups found, {len(merged)} merged, {len(final_attrs)} final attributes")
            if synonyms:
                for syn_info in synonyms[:2]:  # Show first 2
                    attr1 = syn_info.get("attr1", "")
                    attr2 = syn_info.get("attr2", "")
                    should_merge = syn_info.get("should_merge", False)
                    print(f"    * {attr1} <-> {attr2}: should_merge={should_merge}")
        
        # Step 2.4: Composite Attribute Handling
        print("\n" + "-" * 80)
        print("Step 2.4: Composite Attribute Handling")
        print("-" * 80)
        # Build entity_attributes dict for step 2.4 (use final_attribute_list from 2.3 - list of strings)
        entity_attributes_2_4 = {}
        for entity_name, result in entity_results_2_3.items():
            final_attrs = result.get("final_attribute_list", [])
            entity_attributes_2_4[entity_name] = final_attrs
        
        result_2_4 = await step_2_4_composite_attribute_handling_batch(
            entities=entities,
            entity_attributes=entity_attributes_2_4,
            nl_description=nl_description
        )
        entity_results_2_4 = result_2_4.get("entity_results", {})
        print(f"[PASS] Step 2.4 completed: processed {len(entity_results_2_4)} entities")
        for entity_name, result in entity_results_2_4.items():
            composite_attrs = result.get("composite_attributes", [])
            print(f"  - {entity_name}: {len(composite_attrs)} composite attributes identified")
            if composite_attrs:
                for comp_attr in composite_attrs[:2]:  # Show first 2
                    attr_name = comp_attr.get("name", "Unknown")
                    should_decompose = comp_attr.get("should_decompose", False)
                    decomposition = comp_attr.get("decomposition", [])
                    print(f"    * {attr_name}: should_decompose={should_decompose}")
                    if should_decompose and decomposition:
                        print(f"      -> Decompose into: {', '.join(decomposition)}")
        
        # Step 2.5: Temporal Attributes Detection
        print("\n" + "-" * 80)
        print("Step 2.5: Temporal Attributes Detection")
        print("-" * 80)
        # Build entity_attributes dict for step 2.5
        # Use final_attribute_list from 2.3, plus any decomposed attributes from 2.4
        entity_attributes_2_5 = {}
        for entity_name, result in entity_results_2_3.items():
            final_attrs = result.get("final_attribute_list", [])
            # Add decomposed attributes from step 2.4 if any
            comp_result = entity_results_2_4.get(entity_name, {})
            for comp_attr in comp_result.get("composite_attributes", []):
                if comp_attr.get("should_decompose", False):
                    decomposition = comp_attr.get("decomposition", [])
                    final_attrs.extend(decomposition)
            entity_attributes_2_5[entity_name] = final_attrs
        
        result_2_5 = await step_2_5_temporal_attributes_detection_batch(
            entities=entities,
            entity_attributes=entity_attributes_2_5,
            nl_description=nl_description
        )
        entity_results_2_5 = result_2_5.get("entity_results", {})
        print(f"[PASS] Step 2.5 completed: processed {len(entity_results_2_5)} entities")
        for entity_name, result in entity_results_2_5.items():
            needs_temporal = result.get("needs_temporal", False)
            temporal_attrs = result.get("temporal_attributes", [])  # This is a list of strings
            print(f"  - {entity_name}: needs_temporal={needs_temporal}, {len(temporal_attrs)} temporal attributes")
            if temporal_attrs:
                print(f"    * Temporal attributes: {', '.join(temporal_attrs)}")
        
        print("\n" + "=" * 80)
        print("[PASS] Phase 2 steps (2.1-2.5) completed successfully!")
        print("=" * 80)
        print(f"\nSummary:")
        print(f"  - Entities processed: {len(entities)}")
        print(f"  - Step 2.1: Attribute count detection completed")
        print(f"  - Step 2.2: Intrinsic attributes extracted")
        print(f"  - Step 2.3: Attribute synonyms detected and merged")
        print(f"  - Step 2.4: Composite attributes handled")
        print(f"  - Step 2.5: Temporal attributes added")
        
    except Exception as e:
        print(f"\n[ERROR] Phase 2 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_phase2_steps())
    sys.exit(0 if success else 1)

