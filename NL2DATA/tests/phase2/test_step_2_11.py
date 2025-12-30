"""Test script for Step 2.11: Nullability Constraints."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2 import step_2_11_nullability_constraints_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_2_11():
    """Test Step 2.11: Nullability Constraints."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 2.11: Nullability Constraints")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Customer with required and optional attributes
        print("\n" + "-" * 80)
        print("Test Case 1: Customer entity with required and optional attributes")
        print("-" * 80)
        nl_description = """A customer management system where customers must have an ID, name, and email. 
        Phone number and address are optional."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer in the system"},
        ]
        
        entity_attributes_1 = {
            "Customer": ["customer_id", "name", "email", "phone", "address"],
        }
        
        entity_primary_keys_1 = {
            "Customer": ["customer_id"],
        }
        
        result_1 = await step_2_11_nullability_constraints_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            entity_primary_keys=entity_primary_keys_1,
            nl_description=nl_description,
            domain="e-commerce"
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        print(f"[PASS] Step 2.11 completed: processed {len(entity_results_1)} entities")
        
        for entity_name, result in entity_results_1.items():
            nullable_attrs = result.get("nullable_attributes", [])
            non_nullable_attrs = result.get("non_nullable_attributes", [])
            reasoning = result.get("reasoning", {})
            
            print(f"  - {entity_name}:")
            print(f"    Nullable attributes ({len(nullable_attrs)}): {nullable_attrs}")
            for attr in nullable_attrs[:3]:  # Show first 3
                reason = reasoning.get(attr, "N/A")
                print(f"      * {attr}: {reason}")
            
            print(f"    Non-nullable attributes ({len(non_nullable_attrs)}): {non_nullable_attrs}")
            for attr in non_nullable_attrs[:3]:  # Show first 3
                reason = reasoning.get(attr, "N/A")
                print(f"      * {attr}: {reason}")
            
            # Validate that all attributes are classified
            available_attrs = entity_attributes_1.get(entity_name, [])
            all_classified = set(nullable_attrs) | set(non_nullable_attrs)
            missing = set(available_attrs) - all_classified
            overlap = set(nullable_attrs) & set(non_nullable_attrs)
            
            if missing:
                print(f"    [WARNING] Unclassified attributes: {missing}")
            if overlap:
                print(f"    [ERROR] Overlapping attributes: {overlap}")
                all_passed = False
            
            # Check that primary key is non-nullable
            primary_key = entity_primary_keys_1.get(entity_name, [])
            pk_nullable = [attr for attr in primary_key if attr in nullable_attrs]
            if pk_nullable:
                print(f"    [WARNING] Primary key attributes in nullable list: {pk_nullable}")
            
            if not missing and not overlap:
                print(f"    [OK] All attributes classified correctly")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 2.11 tests passed!")
        else:
            print("[ERROR] Some Step 2.11 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 2.11 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_2_11())
    sys.exit(0 if success else 1)

