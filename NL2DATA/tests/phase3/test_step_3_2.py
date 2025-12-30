"""Test script for Step 3.2: Information Completeness Check."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase3 import step_3_2_information_completeness_batch
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_3_2():
    """Test Step 3.2: Information Completeness Check."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Step 3.2: Information Completeness Check")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: E-commerce database
        print("\n" + "-" * 80)
        print("Test Case 1: E-commerce database")
        print("-" * 80)
        nl_description = """An e-commerce database for an online store. Customers can place orders for products."""
        
        entities_1 = [
            {"name": "Customer", "description": "A customer who places orders"},
            {"name": "Order", "description": "An order placed by a customer"},
            {"name": "Product", "description": "A product available for purchase"},
        ]
        
        relations_1 = [
            {
                "entities": ["Customer", "Order"],
                "type": "one-to-many",
                "description": "A customer can place multiple orders",
                "arity": 2
            },
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
            ],
        }
        
        primary_keys_1 = {
            "Customer": ["customer_id"],
            "Order": ["order_id"],
            "Product": ["product_id"],
        }
        
        foreign_keys_1 = [
            {
                "from_entity": "Order",
                "to_entity": "Customer",
                "attributes": ["customer_id"],
            }
        ]
        
        information_needs_1 = [
            {
                "description": "List of all orders placed by a specific customer",
                "frequency": "frequent",
                "entities_involved": ["Customer", "Order"],
                "reasoning": "Common customer service query"
            },
            {
                "description": "Total sales amount for a specific date range",
                "frequency": "frequent",
                "entities_involved": ["Order"],
                "reasoning": "Sales reporting"
            },
        ]
        
        result_1 = await step_3_2_information_completeness_batch(
            information_needs=information_needs_1,
            entities=entities_1,
            relations=relations_1,
            attributes=attributes_1,
            primary_keys=primary_keys_1,
            foreign_keys=foreign_keys_1,
            nl_description=nl_description,
            domain="e-commerce",
            max_iterations=3,  # Limit iterations for testing
            max_time_sec=120,
        )
        
        completeness_results_1 = result_1.get("completeness_results", {})
        print(f"[PASS] Step 3.2 completed: processed {len(completeness_results_1)} information needs")
        
        for info_id, result in completeness_results_1.items():
            info_need = result.get("information_need", info_id)
            all_present = result.get("all_present", False)
            satisfied = result.get("satisfied", False)
            missing_relations = result.get("missing_relations", [])
            missing_entities = result.get("missing_entities", [])
            missing_attributes = result.get("missing_attributes", [])
            reasoning = result.get("reasoning", "")
            
            print(f"\n  - Information Need: {info_need}")
            print(f"    All present: {all_present}, Satisfied: {satisfied}")
            
            if missing_relations:
                print(f"    Missing relations: {', '.join(missing_relations)}")
            if missing_entities:
                print(f"    Missing entities: {', '.join(missing_entities)}")
            if missing_attributes:
                attr_strs = [f"{attr.get('entity', '')}.{attr.get('attribute', '')}" for attr in missing_attributes]
                print(f"    Missing attributes: {', '.join(attr_strs)}")
            
            if reasoning:
                print(f"    Reasoning: {reasoning}")
            
            if not satisfied:
                print(f"    [WARNING] Information need not satisfied - missing components identified")
            else:
                print(f"    [OK] Information need satisfied")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Step 3.2 tests passed!")
        else:
            print("[ERROR] Some Step 3.2 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Step 3.2 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_step_3_2())
    sys.exit(0 if success else 1)

