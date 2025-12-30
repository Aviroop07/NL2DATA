"""Test script for Step 2.12: Default Values."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_12_default_values_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_12():
    """Test Step 2.12: Default Values."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.12: Default Values")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Order with status and created_at defaults
        print("\n" + "-" * 80)
        print("Test Case 1: Order entity with status and timestamp defaults")
        print("-" * 80)
        nl_description = """An order system where orders have a status that defaults to 'pending' 
        and a created_at timestamp that defaults to CURRENT_TIMESTAMP."""
        
        entities_1 = [
            {"name": "Order", "description": "A customer order"},
        ]
        
        entity_attributes_1 = {
            "Order": ["order_id", "status", "created_at", "total_amount"],
        }
        
        entity_nullability_1 = {
            "Order": {
                "nullable_attributes": ["status"],
                "non_nullable_attributes": ["order_id", "created_at", "total_amount"],
                "reasoning": {}
            }
        }
        
        result_1 = await step_2_12_default_values_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            entity_nullability=entity_nullability_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.12 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            default_values = result.get("default_values", {})
            reasoning = result.get("reasoning", {})
            
            print(f"  - {entity_name}:")
            if default_values:
                print(f"    Default values ({len(default_values)}):")
                for attr, default in default_values.items():
                    reason = reasoning.get(attr, "N/A")
                    print(f"      * {attr} = {default}")
                    print(f"        Reasoning: {reason}")
            else:
                print(f"    Default values: None")
            
            # Validate that attributes exist
            available_attrs = entity_attributes_1.get(entity_name, [])
            invalid_attrs = [attr for attr in default_values.keys() if attr not in available_attrs]
            
            if invalid_attrs:
                print(f"    [ERROR] Invalid attributes with defaults: {invalid_attrs}")
                all_passed = False
            else:
                print(f"    [OK] All default values validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.12 tests passed!")
        else:
            print("[ERROR] Some Step 2.12 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.12 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_12())
    sys.exit(0 if success else 1)

