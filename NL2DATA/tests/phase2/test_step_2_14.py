"""Test script for Step 2.14: Relation Realization."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_14_relation_realization_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_14():
    """Test Step 2.14: Relation Realization."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.14: Relation Realization")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: One-to-many relation (Customer -> Order)
        print("\n" + "-" * 80)
        print("Test Case 1: One-to-many relation (Customer -> Order)")
        print("-" * 80)
        nl_description = """A customer management system where customers can place multiple orders. 
        Each order belongs to one customer."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer who places orders"},
            {"name": "Order", "description": "An order placed by a customer"},
        ]
        
        relations_1 = [
            {
                "entities": ["Customer", "Order"],
                "type": "one-to-many",
                "description": "A customer can place multiple orders",
                "arity": 2
            }
        ]
        
        entity_primary_keys_1 = {
            "Customer": ["customer_id"],
            "Order": ["order_id"],
        }
        
        entity_attributes_1 = {
            "Customer": ["customer_id", "name", "email"],
            "Order": ["order_id", "order_date", "total_amount"],
        }
        
        relation_cardinalities_1 = {
            "Customer+Order": {
                "Customer": "1",
                "Order": "N"
            }
        }
        
        result_1 = await step_2_14_relation_realization_batch(
            relations=relations_1,
            entities=entities_1,
            entity_primary_keys=entity_primary_keys_1,
            entity_attributes=entity_attributes_1,
            relation_cardinalities=relation_cardinalities_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        relation_results_1 = result_1.get("relation_results", {})
        print(f"[PASS] Step 2.14 completed: processed {len(relation_results_1)} relations")
        
        for relation_id, result in relation_results_1.items():
            realization_type = result.get("realization_type", "")
            realization_attrs = result.get("realization_attrs", {})
            junction_table_name = result.get("junction_table_name")
            exists = result.get("exists", False)
            needs_creation = result.get("needs_creation", False)
            referential_integrity = result.get("referential_integrity", {})
            reasoning = result.get("reasoning", "")
            
            print(f"  - Relation: {relation_id}")
            print(f"    Realization type: {realization_type}")
            if realization_attrs:
                print(f"    Foreign key attributes: {realization_attrs}")
                for fk_attr, ref in realization_attrs.items():
                    integrity = referential_integrity.get(fk_attr, "N/A")
                    print(f"      * {fk_attr} -> {ref} (integrity: {integrity})")
            if junction_table_name:
                print(f"    Junction table: {junction_table_name}")
            print(f"    Exists: {exists}, Needs creation: {needs_creation}")
            print(f"    Reasoning: {reasoning}")
            
            # Validate that referenced entities and PKs exist
            if realization_attrs:
                for fk_attr, ref in realization_attrs.items():
                    if "." in ref:
                        ref_entity, ref_attr = ref.split(".", 1)
                        if ref_entity not in entity_primary_keys_1:
                            print(f"    [ERROR] Referenced entity {ref_entity} has no primary key")
                            all_passed = False
                        elif ref_attr not in entity_primary_keys_1.get(ref_entity, []):
                            print(f"    [ERROR] Foreign key {fk_attr} references {ref} but {ref_attr} is not in PK of {ref_entity}")
                            all_passed = False
                        else:
                            print(f"    [OK] Foreign key {fk_attr} validated")
            
            if not all_passed:
                print(f"    [ERROR] Validation failed for relation {relation_id}")
            else:
                print(f"    [OK] Relation realization validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.14 tests passed!")
        else:
            print("[ERROR] Some Step 2.14 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.14 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_14())
    sys.exit(0 if success else 1)

