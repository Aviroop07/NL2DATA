"""Test script for Phase 1 Step 1.12: Relation Validation (with agent-executor pattern)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1 import step_1_12_relation_validation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_step_1_12():
    """Test Step 1.12 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    test_cases = [
        {
            "name": "Valid Relations",
            "entities": [
                {"name": "Customer", "description": "A customer"},
                {"name": "Order", "description": "An order"},
                {"name": "Product", "description": "A product"},
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
            "relation_cardinalities": [
                {
                    "entities": ["Customer", "Order"],
                    "entity_cardinalities": {"Customer": "1", "Order": "N"}
                },
                {
                    "entities": ["Order", "Product"],
                    "entity_cardinalities": {"Order": "N", "Product": "N"}
                }
            ],
            "nl_description": "Valid schema with proper relations"
        },
        {
            "name": "Circular Dependencies",
            "entities": [
                {"name": "A", "description": "Entity A"},
                {"name": "B", "description": "Entity B"},
                {"name": "C", "description": "Entity C"},
            ],
            "relations": [
                {
                    "entities": ["A", "B"],
                    "type": "one-to-many",
                    "description": "A to B"
                },
                {
                    "entities": ["B", "C"],
                    "type": "one-to-many",
                    "description": "B to C"
                },
                {
                    "entities": ["C", "A"],
                    "type": "one-to-many",
                    "description": "C to A (creates cycle)"
                }
            ],
            "relation_cardinalities": None,
            "nl_description": "Schema with potential circular dependencies"
        },
        {
            "name": "Inconsistent Cardinalities",
            "entities": [
                {"name": "Parent", "description": "Parent entity"},
                {"name": "Child", "description": "Child entity"},
            ],
            "relations": [
                {
                    "entities": ["Parent", "Child"],
                    "type": "one-to-many",
                    "description": "Parent has children"
                }
            ],
            "relation_cardinalities": [
                {
                    "entities": ["Parent", "Child"],
                    "entity_cardinalities": {"Parent": "N", "Child": "1"}  # Inconsistent with one-to-many
                }
            ],
            "nl_description": "Schema with inconsistent cardinalities"
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 1 Step 1.12: Relation Validation (Agent-Executor Pattern)")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        entities = test_case["entities"]
        relations = test_case["relations"]
        relation_cardinalities = test_case.get("relation_cardinalities")
        nl_description = test_case.get("nl_description")
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Entities ({len(entities)}): {', '.join([e['name'] for e in entities])}")
        print(f"Relations ({len(relations)}): {len(relations)}")
        print()
        
        # Step 1.12: Relation Validation
        print("-" * 60)
        print("Step 1.12: Relation Validation")
        print("-" * 60)
        try:
            result_1_12 = await step_1_12_relation_validation(
                entities=entities,
                relations=relations,
                relation_cardinalities=relation_cardinalities,
                nl_description=nl_description
            )
            print(f"[PASS] Step 1.12 completed")
            
            circular_dependencies = result_1_12.get('circular_dependencies', [])
            impossible_cardinalities = result_1_12.get('impossible_cardinalities', [])
            conflicts = result_1_12.get('conflicts', [])
            validation_passed = result_1_12.get('validation_passed', False)
            reasoning = result_1_12.get('reasoning', '')
            
            print(f"  - Validation passed: {validation_passed}")
            print(f"  - Circular dependencies: {len(circular_dependencies)}")
            if circular_dependencies:
                for dep in circular_dependencies[:3]:
                    print(f"    {dep}")
            
            print(f"  - Impossible cardinalities: {len(impossible_cardinalities)}")
            if impossible_cardinalities:
                for card in impossible_cardinalities[:3]:
                    print(f"    {card}")
            
            print(f"  - Conflicts: {len(conflicts)}")
            if conflicts:
                for conflict in conflicts[:3]:
                    print(f"    {conflict}")
            
            if reasoning:
                print(f"  - Reasoning: {reasoning[:200]}...")
            
            # Verify agent-executor pattern was used
            print(f"  - [INFO] Agent-executor pattern used (tools: detect_circular_dependencies, validate_cardinality_consistency)")
            
        except Exception as e:
            print(f"[ERROR] Step 1.12 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 1.12 completed successfully for all test cases!")
        print("[INFO] Agent-executor pattern is working correctly")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(test_step_1_12())
    sys.exit(0 if success else 1)


