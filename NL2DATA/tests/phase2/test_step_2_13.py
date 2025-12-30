"""Test script for Step 2.13: Check Constraints."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_13_check_constraints_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_13():
    """Test Step 2.13: Check Constraints."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.13: Check Constraints (Value Ranges)")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Product with price and quantity constraints
        print("\n" + "-" * 80)
        print("Test Case 1: Product entity with price and quantity constraints")
        print("-" * 80)
        nl_description = """A product catalog where products have prices that must be non-negative 
        and quantities that must be greater than zero. Discount percentages must be between 0 and 100."""
        
        entities_1 = [
            {"name": "Product", "description": "A product in the catalog"},
        ]
        
        entity_attributes_1 = {
            "Product": ["product_id", "name", "price", "quantity", "discount_percentage"],
        }
        
        result_1 = await step_2_13_check_constraints_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.13 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            check_constraints = result.get("check_constraints", {})
            
            print(f"  - {entity_name}:")
            if check_constraints:
                print(f"    Check constraints ({len(check_constraints)}):")
                for attr, constraint_info in check_constraints.items():
                    condition = constraint_info.get("condition", "")
                    description = constraint_info.get("description", "")
                    reasoning = constraint_info.get("reasoning", "")
                    print(f"      * {attr}:")
                    print(f"        Condition: {condition}")
                    print(f"        Description: {description}")
                    print(f"        Reasoning: {reasoning}")
            else:
                print(f"    Check constraints: None")
            
            # Validate that attributes exist
            available_attrs = entity_attributes_1.get(entity_name, [])
            invalid_attrs = [attr for attr in check_constraints.keys() if attr not in available_attrs]
            
            if invalid_attrs:
                print(f"    [ERROR] Invalid attributes with constraints: {invalid_attrs}")
                all_passed = False
            else:
                print(f"    [OK] All check constraints validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.13 tests passed!")
        else:
            print("[ERROR] Some Step 2.13 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.13 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_13())
    sys.exit(0 if success else 1)

