"""Test script for Step 3.1: Information Need Identification."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase3 import step_3_1_information_need_identification_with_loop
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_3_1():
    """Test Step 3.1: Information Need Identification."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 3.1: Information Need Identification (with loop)")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: E-commerce database
        print("\n" + "-" * 80)
        print("Test Case 1: E-commerce database")
        print("-" * 80)
        nl_description = """An e-commerce database for an online store. Customers can place orders for products. 
        Each order contains multiple order items. Products belong to categories. We need to track inventory levels."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer who places orders"},
            {"name": "Order", "description": "An order placed by a customer"},
            {"name": "Product", "description": "A product available for purchase"},
            {"name": "Category", "description": "Product category"},
        ]
        
        relations_1 = [
            {
                "entities": ["Customer", "Order"],
                "type": "one-to-many",
                "description": "A customer can place multiple orders",
                "arity": 2
            },
            {
                "entities": ["Order", "Product"],
                "type": "many-to-many",
                "description": "An order contains multiple products",
                "arity": 2
            },
            {
                "entities": ["Product", "Category"],
                "type": "many-to-one",
                "description": "A product belongs to a category",
                "arity": 2
            }
        ]
        
        attributes_1 = {
            "Customer": [
                {"name": "customer_id", "description": "Unique customer identifier"},
                {"name": "name", "description": "Customer name"},
                {"name": "email", "description": "Customer email"},
            ],
            "Order": [
                {"name": "order_id", "description": "Unique order identifier"},
                {"name": "order_date", "description": "Date when order was placed"},
                {"name": "total_amount", "description": "Total order amount"},
            ],
            "Product": [
                {"name": "product_id", "description": "Unique product identifier"},
                {"name": "name", "description": "Product name"},
                {"name": "price", "description": "Product price"},
                {"name": "inventory_level", "description": "Current inventory level"},
            ],
            "Category": [
                {"name": "category_id", "description": "Unique category identifier"},
                {"name": "name", "description": "Category name"},
            ],
        }
        
        primary_keys_1 = {
            "Customer": ["customer_id"],
            "Order": ["order_id"],
            "Product": ["product_id"],
            "Category": ["category_id"],
        }
        
        foreign_keys_1 = [
            {
                "from_entity": "Order",
                "to_entity": "Customer",
                "attributes": ["customer_id"],
            }
        ]
        
        result_1 = await step_3_1_information_need_identification_with_loop(
            nl_description=nl_description,
            entities=entities_1,
            relations=relations_1,
            attributes=attributes_1,
            primary_keys=primary_keys_1,
            foreign_keys=foreign_keys_1,
            domain="e-commerce",
            max_iterations=5,  # Limit iterations for testing
            max_time_sec=180,
        )
        
        final_result_1 = result_1.get("final_result", {})
        loop_metadata_1 = result_1.get("loop_metadata", {})
        information_needs_1 = final_result_1.get("information_needs", [])
        no_more_changes_1 = final_result_1.get("no_more_changes", False)
        
        print(f"[PASS] Step 3.1 completed: {len(information_needs_1)} information needs identified")
        print(f"  - Loop iterations: {loop_metadata_1.get('iterations', 0)}")
        print(f"  - Terminated by: {loop_metadata_1.get('terminated_by', 'unknown')}")
        print(f"  - No more changes: {no_more_changes_1}")
        
        if information_needs_1:
            print(f"\n  Information Needs Identified:")
            for i, info_need in enumerate(information_needs_1[:5], 1):  # Show first 5
                desc = info_need.get("description", "")
                freq = info_need.get("frequency", "")
                entities = info_need.get("entities_involved", [])
                print(f"    {i}. {desc}")
                print(f"       Frequency: {freq}, Entities: {', '.join(entities) if entities else 'None'}")
            if len(information_needs_1) > 5:
                print(f"    ... and {len(information_needs_1) - 5} more information needs")
        
        reasoning_1 = final_result_1.get("reasoning", "")
        if reasoning_1:
            print(f"\n  Reasoning: {reasoning_1}")
        
        if not information_needs_1:
            print(f"    [ERROR] No information needs identified")
            all_passed = False
        else:
            print(f"    [OK] Information needs identified successfully")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 3.1 tests passed!")
        else:
            print("[ERROR] Some Step 3.1 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 3.1 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_3_1())
    sys.exit(0 if success else 1)

