"""Test script for Steps 3.4 and 3.5: ER Design and Relational Schema Compilation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase3 import step_3_4_er_design_compilation, step_3_5_relational_schema_compilation
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

def test_steps_3_4_3_5():
    """Test Steps 3.4 and 3.5: ER Design and Relational Schema Compilation."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Steps 3.4 and 3.5: ER Design and Relational Schema Compilation")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Simple e-commerce schema
        print("\n" + "-" * 80)
        print("Test Case 1: Simple e-commerce schema")
        print("-" * 80)
        
        entities = [
            {"name": "Customer", "description": "A customer who places orders"},
            {"name": "Order", "description": "An order placed by a customer"},
        ]
        
        relations = [
            {
                "entities": ["Customer", "Order"],
                "type": "one-to-many",
                "description": "A customer can place multiple orders",
                "arity": 2
            }
        ]
        
        attributes = {
            "Customer": [
                {"name": "customer_id", "description": "Unique customer identifier"},
                {"name": "name", "description": "Customer name"},
            ],
            "Order": [
                {"name": "order_id", "description": "Unique order identifier"},
                {"name": "order_date", "description": "Date when order was placed"},
            ],
        }
        
        primary_keys = {
            "Customer": ["customer_id"],
            "Order": ["order_id"],
        }
        
        foreign_keys = [
            {
                "from_entity": "Order",
                "to_entity": "Customer",
                "attributes": ["customer_id"],
            }
        ]
        
        # Step 3.4: ER Design Compilation
        print("\nStep 3.4: ER Design Compilation")
        er_design = step_3_4_er_design_compilation(
            entities=entities,
            relations=relations,
            attributes=attributes,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
        )
        
        compiled_entities = er_design.get("entities", [])
        compiled_relations = er_design.get("relations", [])
        compiled_attributes = er_design.get("attributes", {})
        
        print(f"[PASS] Step 3.4 completed")
        print(f"  - Entities: {len(compiled_entities)}")
        print(f"  - Relations: {len(compiled_relations)}")
        print(f"  - Attributes: {len(compiled_attributes)} entities with attributes")
        
        if len(compiled_entities) != 2:
            print(f"    [ERROR] Expected 2 entities, got {len(compiled_entities)}")
            all_passed = False
        else:
            print(f"    [OK] ER design compilation successful")
        
        # Step 3.5: Relational Schema Compilation
        print("\nStep 3.5: Relational Schema Compilation")
        relational_schema = step_3_5_relational_schema_compilation(
            er_design=er_design,
            foreign_keys=foreign_keys,
            primary_keys=primary_keys,
        )
        
        tables = relational_schema.get("tables", [])
        
        print(f"[PASS] Step 3.5 completed")
        print(f"  - Tables: {len(tables)}")
        
        for table in tables:
            table_name = table.get("name", "")
            columns = table.get("columns", [])
            pk = table.get("primary_key", [])
            fks = table.get("foreign_keys", [])
            print(f"    - {table_name}: {len(columns)} columns, PK: {', '.join(pk)}, FKs: {len(fks)}")
        
        if len(tables) != 2:
            print(f"    [ERROR] Expected 2 tables, got {len(tables)}")
            all_passed = False
        else:
            print(f"    [OK] Relational schema compilation successful")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Steps 3.4 and 3.5 tests passed!")
        else:
            print("[ERROR] Some Steps 3.4 and 3.5 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Steps 3.4 and 3.5 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_steps_3_4_3_5()
    sys.exit(0 if success else 1)


