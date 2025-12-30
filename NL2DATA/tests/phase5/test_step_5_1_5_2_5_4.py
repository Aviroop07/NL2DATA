"""Test script for Steps 5.1, 5.2, and 5.4: DDL Compilation, Validation, and Schema Creation.

These are deterministic steps that can be tested without LLM calls.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5 import (
    step_5_1_ddl_compilation,
    step_5_2_ddl_validation,
    step_5_4_schema_creation,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

def test_steps_5_1_5_2_5_4():
    """Test Steps 5.1, 5.2, and 5.4: DDL Compilation, Validation, and Schema Creation."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Steps 5.1, 5.2, and 5.4: DDL Compilation, Validation, and Schema Creation")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Simple normalized schema
        print("\n" + "-" * 80)
        print("Test Case 1: Simple normalized schema")
        print("-" * 80)
        
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
                            "on_delete": "CASCADE",
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
        
        # Step 5.1: DDL Compilation
        print("\nStep 5.1: DDL Compilation")
        result_5_1 = step_5_1_ddl_compilation(
            normalized_schema=normalized_schema,
            data_types=data_types,
        )
        
        ddl_statements = result_5_1.get("ddl_statements", [])
        
        print(f"[PASS] Step 5.1 completed")
        print(f"  - DDL statements: {len(ddl_statements)}")
        
        for i, ddl in enumerate(ddl_statements, 1):
            print(f"\n  Statement {i}:")
            # Show first 3 lines of each DDL
            lines = ddl.split("\n")[:3]
            for line in lines:
                print(f"    {line}")
            if len(ddl.split("\n")) > 3:
                print(f"    ... ({len(ddl.split('\n')) - 3} more lines)")
        
        if len(ddl_statements) != 2:
            print(f"    [ERROR] Expected 2 DDL statements, got {len(ddl_statements)}")
            all_passed = False
        else:
            print(f"    [OK] DDL compilation successful")
        
        # Step 5.2: DDL Validation
        print("\nStep 5.2: DDL Validation")
        result_5_2 = step_5_2_ddl_validation(
            ddl_statements=ddl_statements,
            validate_with_db=True,
        )
        
        validation_passed = result_5_2.get("validation_passed", False)
        syntax_errors = result_5_2.get("syntax_errors", [])
        naming_conflicts = result_5_2.get("naming_conflicts", [])
        
        print(f"[PASS] Step 5.2 completed")
        print(f"  - Validation passed: {validation_passed}")
        print(f"  - Syntax errors: {len(syntax_errors)}")
        print(f"  - Naming conflicts: {len(naming_conflicts)}")
        
        if not validation_passed:
            print(f"    [ERROR] DDL validation failed")
            for err in syntax_errors[:3]:
                print(f"      - {err.get('error', 'Unknown error')}")
            for conflict in naming_conflicts[:3]:
                print(f"      - {conflict}")
            all_passed = False
        else:
            print(f"    [OK] DDL validation successful")
        
        # Step 5.4: Schema Creation
        print("\nStep 5.4: Schema Creation")
        result_5_4 = step_5_4_schema_creation(
            ddl_statements=ddl_statements,
            database_path=None,  # Use in-memory database
        )
        
        success = result_5_4.get("success", False)
        errors = result_5_4.get("errors", [])
        tables_created = result_5_4.get("tables_created", [])
        
        print(f"[PASS] Step 5.4 completed")
        print(f"  - Success: {success}")
        print(f"  - Tables created: {', '.join(tables_created)}")
        print(f"  - Errors: {len(errors)}")
        
        if not success:
            print(f"    [ERROR] Schema creation failed")
            for err in errors[:3]:
                print(f"      - {err}")
            all_passed = False
        else:
            print(f"    [OK] Schema creation successful")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Steps 5.1, 5.2, and 5.4 tests passed!")
        else:
            print("[ERROR] Some Steps 5.1, 5.2, and 5.4 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Steps 5.1, 5.2, and 5.4 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_steps_5_1_5_2_5_4()
    sys.exit(0 if success else 1)

