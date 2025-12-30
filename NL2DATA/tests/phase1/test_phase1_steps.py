"""Test script for Phase 1 steps (1.2, 1.3, 1.4)."""

import asyncio
import sys
from pathlib import Path

# Add project root to path so we can import NL2DATA
# tests/phase1 -> tests -> NL2DATA -> Project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import (
    step_1_2_entity_mention_detection,
    step_1_3_domain_inference,
    step_1_4_key_entity_extraction,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_phase1_steps():
    """Test Phase 1 steps 1.2, 1.3, and 1.4."""
    logger = get_logger(__name__)
    
    # Setup logging
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    # Test cases with various complexity levels
    test_cases = [
        {
            "name": "Simple Bookstore",
            "description": "I need a database for an online bookstore. Customers can place orders for books. Each order can contain multiple books. Books have titles, authors, and prices."
        },
        {
            "name": "Complex Financial Transactions",
            "description": """Generate a financial transactions dataset with a large transaction fact table (≥ 50M rows) and dimensions for customers, merchants, cards, and geography. Legit transactions form the majority, with card-level spending showing strong weekly seasonality and pay-day spikes around the 1st and 15th of each month. Inject multiple fraud patterns: low-value "test" transactions followed by high-value purchases; coordinated fraud rings: many cards hitting the same merchant in a short window; location anomalies: the same card in distant locations within impossible travel time. Label a small fraction of transactions as confirmed_fraud, leave many suspicious patterns unlabeled. Amounts follow a log-normal with heavy tail; electronics and travel are over-represented among high-value transactions. The dataset should support graph-based fraud detection, geo-temporal anomalies, and skewed group-bys on merchant_id."""
        },
        {
            "name": "Library Management",
            "description": "Design a system to manage books and borrowers in a library. Track book loans, returns, and fines."
        }
    ]
    
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        test_description = test_case["description"]
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Description: {test_description}\n")
        
        # Step 1.2: Entity Mention Detection
        print("-" * 60)
        print("Step 1.2: Entity Mention Detection")
        print("-" * 60)
        try:
            result_1_2 = await step_1_2_entity_mention_detection(test_description)
            print(f"[PASS] Step 1.2 completed")
            print(f"  - Has explicit entities: {result_1_2['has_explicit_entities']}")
            if result_1_2.get('mentioned_entities'):
                print(f"  - Mentioned entities: {', '.join(result_1_2['mentioned_entities'])}")
        except Exception as e:
            print(f"[ERROR] Step 1.2 failed: {e}")
            logger.error(f"Step 1.2 failed for {test_name}", exc_info=True)
            all_passed = False
            continue
        
        # Step 1.3: Domain Inference
        print("\n" + "-" * 60)
        print("Step 1.3: Domain Inference")
        print("-" * 60)
        try:
            result_1_3 = await step_1_3_domain_inference(test_description)
            print(f"[PASS] Step 1.3 completed")
            print(f"  - Inferred domain: {result_1_3['domain']}")
            print(f"  - Confidence: {result_1_3['confidence']:.2f}")
            if result_1_3.get('reasoning'):
                print(f"  - Reasoning: {result_1_3['reasoning']}")
        except Exception as e:
            print(f"[ERROR] Step 1.3 failed: {e}")
            logger.error(f"Step 1.3 failed for {test_name}", exc_info=True)
            all_passed = False
            continue
        
        # Step 1.4: Key Entity Extraction
        print("\n" + "-" * 60)
        print("Step 1.4: Key Entity Extraction")
        print("-" * 60)
        try:
            result_1_4 = await step_1_4_key_entity_extraction(
                test_description,
                domain=result_1_3.get('domain'),
                mentioned_entities=result_1_2.get('mentioned_entities', [])
            )
            print(f"[PASS] Step 1.4 completed")
            entities = result_1_4.get('entities', [])
            print(f"  - Extracted {len(entities)} entities:")
            for entity in entities:
                print(f"    * {entity.get('name', 'Unknown')}: {entity.get('description', 'No description')}")
                if entity.get('reasoning'):
                    print(f"      Reasoning: {entity.get('reasoning')}")
        except Exception as e:
            print(f"[ERROR] Step 1.4 failed: {e}")
            logger.error(f"Step 1.4 failed for {test_name}", exc_info=True)
            all_passed = False
            continue
        
        print()  # Blank line between test cases
    
    print("=" * 60)
    if all_passed:
        print("[PASS] All Phase 1 steps (1.2, 1.3, 1.4) completed successfully for all test cases!")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_phase1_steps())

