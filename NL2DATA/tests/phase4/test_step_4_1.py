"""Test script for Step 4.1: Functional Dependency Analysis."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase4 import step_4_1_functional_dependency_analysis_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_4_1():
    """Test Step 4.1: Functional Dependency Analysis."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 4.1: Functional Dependency Analysis")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Customer entity with zipcode → city dependency
        print("\n" + "-" * 80)
        print("Test Case 1: Customer entity with zipcode -> city dependency")
        print("-" * 80)
        nl_description = """A customer database. Customers have addresses with zipcodes and cities."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer who places orders"},
        ]
        
        entity_attributes_1 = {
            "Customer": [
                {"name": "customer_id", "description": "Unique customer identifier"},
                {"name": "name", "description": "Customer name"},
                {"name": "zipcode", "description": "ZIP code"},
                {"name": "city", "description": "City name"},
                {"name": "state", "description": "State name"},
            ],
        }
        
        entity_primary_keys_1 = {
            "Customer": ["customer_id"],
        }

        # Preferred: mine FDs from relational schema tables/columns (post ER->relational)
        relational_schema_1 = {
            "tables": [
                {
                    "name": "Customer",
                    "columns": [
                        {"name": "customer_id", "description": "Unique customer identifier", "is_primary_key": True},
                        {"name": "name", "description": "Customer name"},
                        {"name": "zipcode", "description": "ZIP code"},
                        {"name": "city", "description": "City name"},
                        {"name": "state", "description": "State name"},
                    ],
                    "primary_key": ["customer_id"],
                    "foreign_keys": [],
                }
            ]
        }
        
        result_1 = await step_4_1_functional_dependency_analysis_batch(
            entities=entities_1,
            entity_attributes=entity_attributes_1,
            entity_primary_keys=entity_primary_keys_1,
            relational_schema=relational_schema_1,
            nl_description=nl_description,
            domain="e-commerce",
            max_iterations=3,  # Limit iterations for testing
            max_time_sec=120,
        )
        
        entity_results_1 = result_1.get("entity_results", {})
        customer_result = entity_results_1.get("Customer", {})
        fds = customer_result.get("functional_dependencies", [])
        no_more_changes = customer_result.get("no_more_changes", False)
        
        print(f"[PASS] Step 4.1 completed: {len(fds)} functional dependencies identified for Customer")
        print(f"  - No more changes: {no_more_changes}")
        
        if fds:
            print(f"\n  Functional Dependencies Identified:")
            for i, fd in enumerate(fds[:5], 1):  # Show first 5
                lhs = fd.get("lhs", [])
                rhs = fd.get("rhs", [])
                reasoning = fd.get("reasoning", "")
                print(f"    {i}. {', '.join(lhs)} -> {', '.join(rhs)}")
                if reasoning:
                    print(f"       Reasoning: {reasoning}")
            if len(fds) > 5:
                print(f"    ... and {len(fds) - 5} more functional dependencies")
        
        reasoning_1 = customer_result.get("reasoning", "")
        if reasoning_1:
            print(f"\n  Reasoning: {reasoning_1}")
        
        if not fds:
            print(f"    [ERROR] No functional dependencies identified")
            all_passed = False
        else:
            print(f"    [OK] Functional dependencies identified successfully")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 4.1 tests passed!")
        else:
            print("[ERROR] Some Step 4.1 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 4.1 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_4_1())
    sys.exit(0 if success else 1)

