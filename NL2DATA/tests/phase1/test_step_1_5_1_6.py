"""Test script for Phase 1 Steps 1.5 and 1.6."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import (
    step_1_5_relation_mention_detection,
    step_1_6_auxiliary_entity_suggestion,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

async def test_steps_1_5_1_6():
    """Test Steps 1.5 and 1.6 with multiple test cases."""
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
            "description": "I need a database for an online bookstore. Customers can place orders for books. Each order can contain multiple books. Books have titles, authors, and prices.",
            "key_entities": [
                {"name": "Customer", "description": "An individual or entity that purchases books from the online bookstore."},
                {"name": "Order", "description": "A transaction in which a customer purchases one or more books."},
                {"name": "Book", "description": "An item available for purchase in the online bookstore."}
            ],
            "domain": "e-commerce, retail, online store"
        },
        {
            "name": "Complex Financial Transactions",
            "description": """Generate a financial transactions dataset with a large transaction fact table (≥ 50M rows) and dimensions for customers, merchants, cards, and geography. Legit transactions form the majority, with card-level spending showing strong weekly seasonality and pay-day spikes around the 1st and 15th of each month. Inject multiple fraud patterns: low-value "test" transactions followed by high-value purchases; coordinated fraud rings: many cards hitting the same merchant in a short window; location anomalies: the same card in distant locations within impossible travel time. Label a small fraction of transactions as confirmed_fraud, leave many suspicious patterns unlabeled. Amounts follow a log-normal with heavy tail; electronics and travel are over-represented among high-value transactions. The dataset should support graph-based fraud detection, geo-temporal anomalies, and skewed group-bys on merchant_id.""",
            "key_entities": [
                {"name": "Transaction", "description": "Represents a financial transaction, which is the core fact table in the dataset."},
                {"name": "Customer", "description": "Represents the individuals or entities who are making transactions."},
                {"name": "Merchant", "description": "Represents the businesses or entities where transactions are made."},
                {"name": "Card", "description": "Represents the payment cards used for transactions."},
                {"name": "Geography", "description": "Represents the geographical locations related to transactions."},
                {"name": "FraudPattern", "description": "Represents patterns of fraudulent activity within transactions."}
            ],
            "domain": "finance"
        },
        {
            "name": "Library Management",
            "description": "Design a system to manage books and borrowers in a library. Track book loans, returns, and fines.",
            "key_entities": [
                {"name": "Book", "description": "Represents a book in the library's collection."},
                {"name": "Borrower", "description": "Represents a person who borrows books from the library."},
                {"name": "Loan", "description": "Represents the action of lending a book to a borrower."},
                {"name": "Return", "description": "Represents the action of returning a borrowed book to the library."},
                {"name": "Fine", "description": "Represents a penalty fee charged to a borrower for late return of a book."}
            ],
            "domain": "library management"
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Steps 1.5 and 1.6")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        test_description = test_case["description"]
        key_entities = test_case["key_entities"]
        domain = test_case["domain"]
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Description: {test_description}\n")
        
        # Step 1.5: Relation Mention Detection
        print("-" * 60)
        print("Step 1.5: Relation Mention Detection")
        print("-" * 60)
        try:
            result_1_5 = await step_1_5_relation_mention_detection(
                test_description,
                entities=key_entities
            )
            print(f"[PASS] Step 1.5 completed")
            print(f"  - Has explicit relations: {result_1_5.get('has_explicit_relations', False)}")
            mentioned_relations = result_1_5.get('mentioned_relations', [])
            if mentioned_relations:
                print(f"  - Mentioned relations ({len(mentioned_relations)}):")
                for relation in mentioned_relations:
                    print(f"    * {relation}")
            else:
                print(f"  - No explicit relations mentioned")
        except Exception as e:
            print(f"[ERROR] Step 1.5 failed: {e}")
            all_passed = False
            continue
        
        # Step 1.6: Auxiliary Entity Suggestion
        print("\n" + "-" * 60)
        print("Step 1.6: Auxiliary Entity Suggestion")
        print("-" * 60)
        try:
            result_1_6 = await step_1_6_auxiliary_entity_suggestion(
                test_description,
                key_entities=key_entities,
                domain=domain
            )
            print(f"[PASS] Step 1.6 completed")
            suggested_entities = result_1_6.get('suggested_entities', [])
            print(f"  - Suggested {len(suggested_entities)} auxiliary entities:")
            for entity in suggested_entities:
                entity_name = entity.get('name', 'Unknown') if isinstance(entity, dict) else getattr(entity, 'name', 'Unknown')
                entity_desc = entity.get('description', 'No description') if isinstance(entity, dict) else getattr(entity, 'description', 'No description')
                entity_reasoning = entity.get('reasoning', 'No reasoning') if isinstance(entity, dict) else getattr(entity, 'reasoning', 'No reasoning')
                print(f"    * {entity_name}: {entity_desc}")
                if entity_reasoning:
                    print(f"      Reasoning: {entity_reasoning}")
        except Exception as e:
            print(f"[ERROR] Step 1.6 failed: {e}")
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] All Phase 1 steps (1.5, 1.6) completed successfully for all test cases!")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_steps_1_5_1_6())

