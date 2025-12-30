"""Test script for Phase 1 Step 1.9: Key Relations Extraction."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import step_1_9_key_relations_extraction
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_1_9():
    """Test Step 1.9 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        {
            "name": "Simple Bookstore",
            "entities": [
                {"name": "Customer", "description": "An individual or entity that purchases books from the online bookstore."},
                {"name": "Order", "description": "A transaction in which a customer purchases one or more books."},
                {"name": "Book", "description": "An item available for purchase in the online bookstore."},
            ],
            "nl_description": "I need a database for an online bookstore. Customers can place orders for books. Each order can contain multiple books.",
            "domain": "e-commerce",
            "mentioned_relations": ["Customers can place orders", "Each order can contain multiple books"]
        },
        {
            "name": "Financial Transactions",
            "entities": [
                {"name": "Transaction", "description": "Represents a financial transaction, which is the core fact table."},
                {"name": "Customer", "description": "Represents the individuals or entities who are making transactions."},
                {"name": "Merchant", "description": "Represents the businesses or entities where transactions are made."},
                {"name": "Card", "description": "Represents the payment cards used for transactions."},
            ],
            "nl_description": "Generate a financial transactions dataset with a large transaction fact table and dimensions for customers, merchants, cards, and geography.",
            "domain": "finance",
            "mentioned_relations": None
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Step 1.9: Key Relations Extraction")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        entities = test_case["entities"]
        nl_description = test_case["nl_description"]
        domain = test_case.get("domain")
        mentioned_relations = test_case.get("mentioned_relations")
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Entities ({len(entities)}): {', '.join([e['name'] for e in entities])}\n")
        
        # Step 1.9: Key Relations Extraction
        print("-" * 60)
        print("Step 1.9: Key Relations Extraction")
        print("-" * 60)
        try:
            result_1_9 = await step_1_9_key_relations_extraction(
                entities=entities,
                nl_description=nl_description,
                domain=domain,
                mentioned_relations=mentioned_relations
            )
            print(f"[PASS] Step 1.9 completed")
            
            relations = result_1_9.get('relations', [])
            print(f"  - Extracted {len(relations)} relations:")
            
            for i, relation in enumerate(relations, 1):
                entities_in_rel = relation.get('entities', [])
                rel_type = relation.get('type', 'unknown')
                description = relation.get('description', 'No description')
                arity = relation.get('arity', 0)
                reasoning = relation.get('reasoning', 'No reasoning')
                
                print(f"\n    Relation {i}: {', '.join(entities_in_rel)}")
                print(f"      - Type: {rel_type}")
                print(f"      - Arity: {arity}")
                print(f"      - Description: {description}")
                print(f"      - Reasoning: {reasoning}")
            
        except Exception as e:
            print(f"[ERROR] Step 1.9 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 1.9 completed successfully for all test cases!")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_step_1_9())

