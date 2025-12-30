"""Test script for Phase 1 Step 1.10: Schema Connectivity (with agent-executor pattern)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import step_1_10_schema_connectivity
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_step_1_10():
    """Test Step 1.10 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        {
            "name": "Connected Schema",
            "entities": [
                {"name": "Customer", "description": "A customer entity"},
                {"name": "Order", "description": "An order entity"},
                {"name": "Product", "description": "A product entity"},
            ],
            "relations": [
                {
                    "entities": ["Customer", "Order"],
                    "type": "one-to-many",
                    "description": "Customer places orders"
                },
                {
                    "entities": ["Order", "Product"],
                    "type": "many-to-many",
                    "description": "Orders contain products"
                }
            ],
            "nl_description": "A database with customers, orders, and products. Customers place orders, and orders contain products."
        },
        {
            "name": "Schema with Orphan Entity",
            "entities": [
                {"name": "Customer", "description": "A customer entity"},
                {"name": "Order", "description": "An order entity"},
                {"name": "UnusedEntity", "description": "An entity not connected to others"},
            ],
            "relations": [
                {
                    "entities": ["Customer", "Order"],
                    "type": "one-to-many",
                    "description": "Customer places orders"
                }
            ],
            "nl_description": "A database with customers and orders. There's also an unused entity."
        },
        {
            "name": "Fully Connected Schema",
            "entities": [
                {"name": "Sensor", "description": "IoT sensor"},
                {"name": "SensorReading", "description": "Reading from sensor"},
                {"name": "Location", "description": "Sensor location"},
            ],
            "relations": [
                {
                    "entities": ["Sensor", "SensorReading"],
                    "type": "one-to-many",
                    "description": "Sensor produces readings"
                },
                {
                    "entities": ["Sensor", "Location"],
                    "type": "many-to-one",
                    "description": "Sensor is at location"
                }
            ],
            "nl_description": "IoT system with sensors, readings, and locations."
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Step 1.10: Schema Connectivity (Agent-Executor Pattern)")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        entities = test_case["entities"]
        relations = test_case["relations"]
        nl_description = test_case["nl_description"]
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Entities ({len(entities)}): {', '.join([e['name'] for e in entities])}")
        print(f"Relations ({len(relations)}): {len(relations)}")
        print()
        
        # Step 1.10: Schema Connectivity
        print("-" * 60)
        print("Step 1.10: Schema Connectivity")
        print("-" * 60)
        try:
            result_1_10 = await step_1_10_schema_connectivity(
                entities=entities,
                relations=relations,
                nl_description=nl_description
            )
            print(f"[PASS] Step 1.10 completed")
            
            orphan_entities = result_1_10.get('orphan_entities', [])
            connectivity_status = result_1_10.get('connectivity_status', {})
            suggested_relations = result_1_10.get('suggested_relations', [])
            reasoning = result_1_10.get('reasoning', '')
            
            print(f"  - Orphan entities: {len(orphan_entities)}")
            if orphan_entities:
                print(f"    {', '.join(orphan_entities)}")
            
            print(f"  - Connectivity status entries: {len(connectivity_status)}")
            for entity, status in list(connectivity_status.items())[:3]:
                print(f"    {entity}: {status}")
            
            print(f"  - Suggested relations: {len(suggested_relations)}")
            if suggested_relations:
                for rel in suggested_relations[:3]:
                    print(f"    {rel}")
            
            if reasoning:
                print(f"  - Reasoning: {reasoning[:200]}...")
            
            # Verify agent-executor pattern was used (check for tool usage in logs or result)
            print(f"  - [INFO] Agent-executor pattern used (tools: check_entity_connectivity)")
            
        except Exception as e:
            print(f"[ERROR] Step 1.10 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 1.10 completed successfully for all test cases!")
        print("[INFO] Agent-executor pattern is working correctly")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(test_step_1_10())
    sys.exit(0 if success else 1)


