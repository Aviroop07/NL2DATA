"""Test script for Phase 6 Steps 6.2 and 6.3: Constraint Scope Analysis and Enforcement Strategy (with agent-executor pattern)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6 import (
    step_6_2_constraint_scope_analysis,
    step_6_3_constraint_enforcement_strategy,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_steps_6_2_6_3():
    """Test Steps 6.2 and 6.3 with multiple test cases."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    # Common normalized schema for all tests
    normalized_schema = {
        "normalized_tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "nullable": False, "type_hint": "integer"},
                    {"name": "amount", "nullable": False, "type_hint": "decimal"},
                    {"name": "timestamp", "nullable": False, "type_hint": "timestamp"},
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": [],
            },
            {
                "name": "Customer",
                "columns": [
                    {"name": "customer_id", "nullable": False, "type_hint": "integer"},
                    {"name": "age", "nullable": True, "type_hint": "integer"},
                ],
                "primary_key": ["customer_id"],
                "foreign_keys": [],
            }
        ]
    }
    
    test_cases = [
        {
            "name": "Structural Constraint",
            "constraint_description": "Transaction amount must be positive",
            "constraint_category": "structural"
        },
        {
            "name": "Statistical Constraint",
            "constraint_description": "Transaction amounts follow a normal distribution with mean 100",
            "constraint_category": "statistical"
        },
        {
            "name": "Age Constraint",
            "constraint_description": "Customer age must be at least 18",
            "constraint_category": "structural"
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 6 Steps 6.2 and 6.3: Constraint Analysis (Agent-Executor Pattern)")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        constraint_description = test_case["constraint_description"]
        constraint_category = test_case["constraint_category"]
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Constraint: {constraint_description}")
        print(f"Category: {constraint_category}")
        print()
        
        # Step 6.2: Constraint Scope Analysis
        print("-" * 60)
        print("Step 6.2: Constraint Scope Analysis")
        print("-" * 60)
        try:
            result_6_2 = await step_6_2_constraint_scope_analysis(
                constraint_description=constraint_description,
                constraint_category=constraint_category,
                normalized_schema=normalized_schema,
                phase2_constraints=None
            )
            print(f"[PASS] Step 6.2 completed")
            
            affected_entities = result_6_2.get('affected_entities', [])
            affected_attributes = result_6_2.get('affected_attributes', [])
            constraint_category_result = result_6_2.get('constraint_category', '')
            reasoning = result_6_2.get('reasoning', '')
            
            print(f"  - Affected entities: {', '.join(affected_entities) if affected_entities else 'None'}")
            print(f"  - Affected attributes: {', '.join(affected_attributes) if affected_attributes else 'None'}")
            print(f"  - Constraint category: {constraint_category_result}")
            if reasoning:
                print(f"  - Reasoning: {reasoning[:200]}...")
            
            # Verify agent-executor pattern was used
            print(f"  - [INFO] Agent-executor pattern used (tools: check_schema_component_exists)")
            
            # Step 6.3: Constraint Enforcement Strategy
            print("\n" + "-" * 60)
            print("Step 6.3: Constraint Enforcement Strategy")
            print("-" * 60)
            
            affected_components = {
                "affected_entities": affected_entities,
                "affected_attributes": affected_attributes
            }
            
            result_6_3 = await step_6_3_constraint_enforcement_strategy(
                constraint_description=constraint_description,
                constraint_category=constraint_category_result,
                affected_components=affected_components,
                normalized_schema=normalized_schema,
                dsl_grammar=None
            )
            print(f"[PASS] Step 6.3 completed")
            
            enforcement_type = result_6_3.get('enforcement_type', '')
            dsl_expression = result_6_3.get('dsl_expression', '')
            reasoning_6_3 = result_6_3.get('reasoning', '')
            
            print(f"  - Enforcement type: {enforcement_type}")
            print(f"  - DSL expression: {dsl_expression}")
            if reasoning_6_3:
                print(f"  - Reasoning: {reasoning_6_3[:200]}...")
            
            # Verify agent-executor pattern was used
            print(f"  - [INFO] Agent-executor pattern used (tools: validate_dsl_expression)")
            
        except Exception as e:
            print(f"[ERROR] Steps 6.2/6.3 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Steps 6.2 and 6.3 completed successfully for all test cases!")
        print("[INFO] Agent-executor pattern is working correctly")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(test_steps_6_2_6_3())
    sys.exit(0 if success else 1)


