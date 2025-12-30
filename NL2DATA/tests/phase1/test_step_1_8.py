"""Test script for Phase 1 Step 1.8: Entity Cardinality & Table Type."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import step_1_8_entity_cardinality
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_1_8():
    """Test Step 1.8 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        {
            "name": "Financial Transactions",
            "entities": [
                {"name": "Transaction", "description": "Represents a financial transaction, which is the core fact table in the dataset."},
                {"name": "Customer", "description": "Represents the individuals or entities who are making transactions."},
                {"name": "Merchant", "description": "Represents the businesses or entities where transactions are made."},
                {"name": "Card", "description": "Represents the payment cards used for transactions."},
            ],
            "nl_description": "Generate a financial transactions dataset with a large transaction fact table (≥ 50M rows) and dimensions for customers, merchants, cards, and geography.",
            "domain": "finance"
        },
        {
            "name": "Simple Bookstore",
            "entities": [
                {"name": "Customer", "description": "An individual or entity that purchases books from the online bookstore."},
                {"name": "Order", "description": "A transaction in which a customer purchases one or more books."},
                {"name": "Book", "description": "An item available for purchase in the online bookstore."},
            ],
            "nl_description": "I need a database for an online bookstore. Customers can place orders for books.",
            "domain": "e-commerce"
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Step 1.8: Entity Cardinality & Table Type")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        entities = test_case["entities"]
        nl_description = test_case.get("nl_description")
        domain = test_case.get("domain")
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Entities ({len(entities)}): {', '.join([e['name'] for e in entities])}\n")
        
        # Step 1.8: Entity Cardinality
        print("-" * 60)
        print("Step 1.8: Entity Cardinality & Table Type")
        print("-" * 60)
        try:
            result_1_8 = await step_1_8_entity_cardinality(
                entities=entities,
                nl_description=nl_description,
                domain=domain
            )
            print(f"[PASS] Step 1.8 completed")
            
            entity_info = result_1_8.get('entity_info', [])
            print(f"  - Analyzed {len(entity_info)} entities:")
            
            for info in entity_info:
                entity_name = info.get('entity', 'Unknown')
                cardinality = info.get('cardinality', 'N/A')
                table_type = info.get('table_type', 'N/A')
                has_explicit = info.get('has_explicit_cardinality', False)
                reasoning = info.get('reasoning', 'No reasoning')
                
                print(f"\n    Entity: {entity_name}")
                print(f"      - Cardinality: {cardinality} (explicit: {has_explicit})")
                print(f"      - Table Type: {table_type}")
                print(f"      - Reasoning: {reasoning}")
            
        except Exception as e:
            print(f"[ERROR] Step 1.8 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 1.8 completed successfully for all test cases!")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_step_1_8())

