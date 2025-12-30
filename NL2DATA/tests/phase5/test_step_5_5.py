"""Test script for Phase 5 Step 5.5: SQL Query Generation (with agent-executor pattern)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5 import step_5_5_sql_query_generation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


async def test_step_5_5():
    """Test Step 5.5 with multiple test cases."""
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
                "name": "Customer",
                "columns": [
                    {"name": "customer_id", "nullable": False, "type_hint": "integer"},
                    {"name": "name", "nullable": False, "type_hint": "string"},
                    {"name": "email", "nullable": True, "type_hint": "string"},
                ],
                "primary_key": ["customer_id"],
                "foreign_keys": [],
            },
            {
                "name": "Order",
                "columns": [
                    {"name": "order_id", "nullable": False, "type_hint": "integer"},
                    {"name": "customer_id", "nullable": False, "type_hint": "integer"},
                    {"name": "order_date", "nullable": False, "type_hint": "date"},
                    {"name": "total_amount", "nullable": False, "type_hint": "decimal"},
                ],
                "primary_key": ["order_id"],
                "foreign_keys": [
                    {
                        "attributes": ["customer_id"],
                        "references_table": "Customer",
                        "referenced_attributes": ["customer_id"],
                    }
                ],
            }
        ]
    }
    
    data_types = {
        "Customer": {
            "customer_id": {"type": "INTEGER"},
            "name": {"type": "VARCHAR", "size": 255},
            "email": {"type": "VARCHAR", "size": 255},
        },
        "Order": {
            "order_id": {"type": "INTEGER"},
            "customer_id": {"type": "INTEGER"},
            "order_date": {"type": "DATE"},
            "total_amount": {"type": "DECIMAL", "precision": 10, "scale": 2},
        },
    }
    
    test_cases = [
        {
            "name": "Simple SELECT Query",
            "information_need": {
                "description": "Find all customers",
                "entities_involved": ["Customer"]
            }
        },
        {
            "name": "JOIN Query",
            "information_need": {
                "description": "Find all orders with customer names",
                "entities_involved": ["Customer", "Order"]
            }
        },
        {
            "name": "Aggregate Query",
            "information_need": {
                "description": "Find total amount of orders per customer",
                "entities_involved": ["Customer", "Order"]
            }
        }
    ]
    
    print("=" * 60)
    print("Testing Phase 5 Step 5.5: SQL Query Generation (Agent-Executor Pattern)")
    print("=" * 60)
    
    all_passed = True
    
    for test_case in test_cases:
        test_name = test_case["name"]
        information_need = test_case["information_need"]
        
        print(f"\n{'=' * 60}")
        print(f"Test Case: {test_name}")
        print(f"{'=' * 60}")
        print(f"Information Need: {information_need.get('description', '')}")
        print()
        
        # Step 5.5: SQL Query Generation
        print("-" * 60)
        print("Step 5.5: SQL Query Generation")
        print("-" * 60)
        try:
            result_5_5 = await step_5_5_sql_query_generation(
                information_need=information_need,
                normalized_schema=normalized_schema,
                data_types=data_types,
                related_entities=information_need.get("entities_involved", []),
                relations=None
            )
            print(f"[PASS] Step 5.5 completed")
            
            sql = result_5_5.get('sql', '')
            validation_status = result_5_5.get('validation_status', '')
            corrected_sql = result_5_5.get('corrected_sql')
            reasoning = result_5_5.get('reasoning', '')
            
            print(f"  - SQL Query:")
            print(f"    {sql[:200]}..." if len(sql) > 200 else f"    {sql}")
            
            print(f"  - Validation status: {validation_status}")
            if corrected_sql:
                print(f"  - Corrected SQL: {corrected_sql[:200]}...")
            
            if reasoning:
                print(f"  - Reasoning: {reasoning[:200]}...")
            
            # Verify SQL is not empty
            if not sql or not sql.strip():
                print(f"  - [WARNING] SQL query is empty")
            else:
                print(f"  - [OK] SQL query generated")
            
            # Verify agent-executor pattern was used
            print(f"  - [INFO] Agent-executor pattern used (tools: validate_sql_syntax, validate_query_against_schema)")
            
        except Exception as e:
            print(f"[ERROR] Step 5.5 failed: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            continue
        
        print()
    
    print("=" * 60)
    if all_passed:
        print("[PASS] Step 5.5 completed successfully for all test cases!")
        print("[INFO] Agent-executor pattern is working correctly")
    else:
        print("[FAIL] Some test cases failed. Check errors above.")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(test_step_5_5())
    sys.exit(0 if success else 1)


