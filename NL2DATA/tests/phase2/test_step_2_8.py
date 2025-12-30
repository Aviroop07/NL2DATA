"""Test script for Step 2.8: Multivalued/Derived Detection."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_8_multivalued_derived_detection_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_8():
    """Test Step 2.8: Multivalued/Derived Detection."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.8: Multivalued/Derived Detection")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Order with derived total_price
        print("\n" + "-" * 80)
        print("Test Case 1: Order entity with derived and multivalued attributes")
        print("-" * 80)
        nl_description = """An order system where orders have items with quantities and unit prices. 
        The total price is calculated from quantity * unit_price. 
        Customers can have multiple phone numbers and addresses."""
        
        entities_1 = [
            {"name": "Order", "description": "A customer order with items"},
            {"name": "Customer", "description": "A customer who places orders"},
        ]
        
        entity_attributes_1 = {
            "Order": ["order_id", "quantity", "unit_price", "total_price", "order_date"],
            "Customer": ["customer_id", "name", "email", "phone_numbers", "addresses"],
        }
        
        result_1 = await step_2_8_multivalued_derived_detection_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.8 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            multivalued = result.get("multivalued", [])
            derived = result.get("derived", [])
            derivation_rules = result.get("derivation_rules", {})
            multivalued_handling = result.get("multivalued_handling", {})
            reasoning = result.get("reasoning", {})
            
            print(f"  - {entity_name}:")
            if multivalued:
                print(f"    Multivalued attributes: {multivalued}")
                for attr in multivalued:
                    handling = multivalued_handling.get(attr, "N/A")
                    reason = reasoning.get(attr, "N/A")
                    print(f"      * {attr}: handling={handling}")
                    print(f"        Reasoning: {reason}")
            else:
                print(f"    Multivalued attributes: None")
            
            if derived:
                print(f"    Derived attributes: {derived}")
                for attr in derived:
                    rule = derivation_rules.get(attr, "N/A")
                    reason = reasoning.get(attr, "N/A")
                    print(f"      * {attr}: rule={rule}")
                    print(f"        Reasoning: {reason}")
            else:
                print(f"    Derived attributes: None")
            
            # Validate that attributes exist
            available_attrs = entity_attributes_1.get(entity_name, [])
            invalid_multivalued = [attr for attr in multivalued if attr not in available_attrs]
            invalid_derived = [attr for attr in derived if attr not in available_attrs]
            
            if invalid_multivalued or invalid_derived:
                if invalid_multivalued:
                    print(f"    [ERROR] Invalid multivalued attributes: {invalid_multivalued}")
                if invalid_derived:
                    print(f"    [ERROR] Invalid derived attributes: {invalid_derived}")
                all_passed = False
            else:
                print(f"    [OK] All attributes validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.8 tests passed!")
        else:
            print("[ERROR] Some Step 2.8 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.8 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_8())
    sys.exit(0 if success else 1)

