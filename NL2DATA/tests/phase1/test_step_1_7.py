"""Test script for Phase 1 Step 1.7: Entity Consolidation."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import step_1_7_entity_consolidation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_step_1_7():
    """Test Step 1.7 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        {
            "name": "Simple Bookstore - With Synonyms",
            "key_entities": [
                {"name": "Customer", "description": "An individual or entity that purchases books from the online bookstore."},
                {"name": "Order", "description": "A transaction in which a customer purchases one or more books."},
                {"name": "Book", "description": "An item available for purchase in the online bookstore."}
            ],
            "auxiliary_entities": [
                {"name": "User", "description": "Represents a user account in the system."},
                {"name": "Customer", "description": "An individual or entity that purchases books."},
                {"name": "Author", "description": "Represents the author of a book."},
                {"name": "OrderItem", "description": "Represents the association between an order and the books it contains."}
            ],
            "domain": "e-commerce, retail, online store",
            "nl_description": "I need a database for an online bookstore. Customers can place orders for books."
        },
        {
            "name": "Complex Financial Transactions - No Duplicates",
            "key_entities": [
                {"name": "Transaction", "description": "Represents a financial transaction, which is the core fact table in the dataset."},
                {"name": "Customer", "description": "Represents the individuals or entities who are making transactions."},
                {"name": "Merchant", "description": "Represents the businesses or entities where transactions are made."},
                {"name": "Card", "description": "Represents the payment cards used for transactions."},
                {"name": "Geography", "description": "Represents the geographical locations related to transactions."},
                {"name": "FraudPattern", "description": "Represents patterns of fraudulent activity within transactions."}
            ],
            "auxiliary_entities": [
                {"name": "PaymentMethod", "description": "Represents the method used for a transaction, such as credit card, debit card, or digital wallet."},
                {"name": "Time", "description": "Represents the time dimension, capturing details like date, day of the week, month, and year."},
                {"name": "Location", "description": "Represents specific locations where transactions occur, including details like city, state, and country."}
            ],
            "domain": "finance",
            "nl_description": "Generate a financial transactions dataset with a large transaction fact table and dimensions for customers, merchants, cards, and geography."
        },
        {
            "name": "Library Management - Overlapping Entities",
            "key_entities": [
                {"name": "Book", "description": "Represents a book in the library's collection."},
                {"name": "Borrower", "description": "Represents a person who borrows books from the library."},
                {"name": "Loan", "description": "Represents the action of lending a book to a borrower."},
                {"name": "Return", "description": "Represents the action of returning a borrowed book to the library."},
                {"name": "Fine", "description": "Represents a penalty fee charged to a borrower for late return of a book."}
            ],
            "auxiliary_entities": [
                {"name": "UserAccount", "description": "Represents the user account for borrowers to access library services online."},
                {"name": "Borrower", "description": "Represents a person who borrows books from the library."},
                {"name": "Address", "description": "Represents the address details of borrowers and library branches."},
                {"name": "Location", "description": "Represents the location of library branches."}
            ],
            "domain": "library management",
            "nl_description": "Design a system to manage books and borrowers in a library. Track book loans, returns, and fines."
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Step 1.7: Entity Consolidation")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        key_entities = test_case["key_entities"]
        auxiliary_entities = test_case.get("auxiliary_entities", [])
        domain = test_case.get("domain")
        nl_description = test_case.get("nl_description")
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Key entities ({len(key_entities)}): {', '.join([e['name'] for e in key_entities])}")
        if auxiliary_entities:
            print(f"Auxiliary entities ({len(auxiliary_entities)}): {', '.join([e['name'] for e in auxiliary_entities])}")
        print()
        
        # Step 1.7: Entity Consolidation
        print("-" * 60)
        print("Step 1.7: Entity Consolidation")
        print("-" * 60)
        try:
            result_1_7 = await step_1_7_entity_consolidation(
                key_entities=key_entities,
                auxiliary_entities=auxiliary_entities,
                domain=domain,
                nl_description=nl_description
            )
            print(f"[PASS] Step 1.7 completed")
            
            duplicates = result_1_7.get('duplicates', [])
            merged_entities = result_1_7.get('merged_entities', [])
            final_entity_list = result_1_7.get('final_entity_list', [])
            
            print(f"  - Duplicates found: {len(duplicates)}")
            if duplicates:
                for dup in duplicates:
                    entity1 = dup.get('entity1', 'Unknown')
                    entity2 = dup.get('entity2', 'Unknown')
                    should_merge = dup.get('should_merge', False)
                    reasoning = dup.get('reasoning', 'No reasoning')
                    print(f"    * {entity1} <-> {entity2}: should_merge={should_merge}")
                    print(f"      Reasoning: {reasoning}")
            
            print(f"  - Merged entities: {len(merged_entities)}")
            if merged_entities:
                print(f"    {', '.join(merged_entities)}")
            
            print(f"  - Final entity count: {len(final_entity_list)}")
            print(f"  - Final entities: {', '.join(final_entity_list)}")
            
        except Exception as e:
            print(f"[ERROR] Step 1.7 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 1.7 completed successfully for all test cases!")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_step_1_7())

