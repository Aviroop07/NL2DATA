"""Test script for Step 2.9: Derived Attribute Formulas."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_9_derived_attribute_formulas_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_9():
    """Test Step 2.9: Derived Attribute Formulas."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.9: Derived Attribute Formulas")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Order with derived total_price
        print("\n" + "-" * 80)
        print("Test Case 1: Order entity with derived total_price")
        print("-" * 80)
        nl_description = """An order system where orders have items with quantities and unit prices. 
        The total price is calculated from quantity * unit_price."""
        
        entity_derived_attributes = {
            "Order": ["total_price"],
        }
        
        entity_attributes = {
            "Order": ["order_id", "quantity", "unit_price", "total_price", "order_date"],
        }
        
        entity_descriptions = {
            "Order": "A customer order with items",
        }
        
        derivation_rules = {
            "Order": {
                "total_price": "quantity * unit_price"
            }
        }
        
        result_1 = await step_2_9_derived_attribute_formulas_batch(
            entity_derived_attributes=entity_derived_attributes,
            entity_attributes=entity_attributes,
            entity_descriptions=entity_descriptions,
            derivation_rules=derivation_rules,
            nl_description=nl_description
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.9 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, attr_results in entity_results_1.items():
            print(f"  - {entity_name}:")
            for attr_name, result in attr_results.items():
                formula = result.get("formula", "")
                expr_type = result.get("expression_type", "")
                dependencies = result.get("dependencies", [])
                reasoning = result.get("reasoning", "")
                
                print(f"    * {attr_name}:")
                print(f"      Formula: {formula}")
                print(f"      Expression Type: {expr_type}")
                print(f"      Dependencies: {dependencies}")
                print(f"      Reasoning: {reasoning}")
                
                # Validate that dependencies exist
                available_attrs = entity_attributes.get(entity_name, [])
                invalid_deps = [dep for dep in dependencies if dep not in available_attrs]
                
                if invalid_deps:
                    print(f"      [ERROR] Invalid dependencies: {invalid_deps}")
                    all_passed = False
                elif not formula:
                    print(f"      [WARNING] No formula extracted")
                else:
                    print(f"      [OK] Formula validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.9 tests passed!")
        else:
            print("[ERROR] Some Step 2.9 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.9 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_9())
    sys.exit(0 if success else 1)

