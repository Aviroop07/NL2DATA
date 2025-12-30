"""Test script for Step 2.10: Unique Constraints."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_10_unique_constraints_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_10():
    """Test Step 2.10: Unique Constraints."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.10: Unique Constraints")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Customer with unique email and username
        print("\n" + "-" * 80)
        print("Test Case 1: Customer entity with unique email and username")
        print("-" * 80)
        nl_description = """A customer management system where each customer has a unique email address and username. 
        Customers can have multiple phone numbers."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer in the system"},
        ]
        
        entity_attributes_1 = {
            "Customer": ["customer_id", "email", "username", "name", "phone"],
        }
        
        entity_primary_keys_1 = {
            "Customer": ["customer_id"],
        }
        
        result_1 = await step_2_10_unique_constraints_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            entity_primary_keys=entity_primary_keys_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.10 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            unique_attrs = result.get("unique_attributes", [])
            unique_combos = result.get("unique_combinations", [])
            reasoning = result.get("reasoning", {})
            
            print(f"  - {entity_name}:")
            if unique_attrs:
                print(f"    Unique attributes: {unique_attrs}")
                for attr in unique_attrs:
                    reason = reasoning.get(attr, "N/A")
                    print(f"      * {attr}: {reason}")
            else:
                print(f"    Unique attributes: None")
            
            if unique_combos:
                print(f"    Unique combinations: {unique_combos}")
                for combo in unique_combos:
                    combo_key = "+".join(combo)
                    reason = reasoning.get(combo_key, "N/A")
                    print(f"      * {combo_key}: {reason}")
            else:
                print(f"    Unique combinations: None")
            
            # Validate that attributes exist and primary key is excluded
            available_attrs = entity_attributes_1.get(entity_name, [])
            primary_key = entity_primary_keys_1.get(entity_name, [])
            invalid_unique = [attr for attr in unique_attrs if attr not in available_attrs]
            pk_in_unique = [attr for attr in unique_attrs if attr in primary_key]
            
            if invalid_unique:
                print(f"    [ERROR] Invalid unique attributes: {invalid_unique}")
                all_passed = False
            if pk_in_unique:
                print(f"    [ERROR] Primary key in unique constraints: {pk_in_unique}")
                all_passed = False
            if not invalid_unique and not pk_in_unique:
                print(f"    [OK] All unique constraints validated")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.10 tests passed!")
        else:
            print("[ERROR] Some Step 2.10 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.10 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_10())
    sys.exit(0 if success else 1)

