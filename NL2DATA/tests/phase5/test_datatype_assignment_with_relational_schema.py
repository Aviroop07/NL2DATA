"""Unit test for datatype assignment with actual relational schema.

This test verifies that datatype assignment works correctly when given
a real relational schema structure from Phase 4.
"""

import asyncio
import sys
import os
from typing import Dict, Any, List

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from NL2DATA.phases.phase5.step_5_1_attribute_dependency_graph import step_5_1_attribute_dependency_graph
from NL2DATA.phases.phase5.step_5_2_independent_attribute_data_types import step_5_2_independent_attribute_data_types_batch
from NL2DATA.phases.phase5.step_5_3_deterministic_fk_data_types import step_5_3_deterministic_fk_data_types
from NL2DATA.phases.phase5.step_5_4_dependent_attribute_data_types import step_5_4_dependent_attribute_data_types_batch
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def create_test_relational_schema() -> Dict[str, Any]:
    """Create a realistic relational schema structure (as would come from Phase 4)."""
    return {
        "tables": [
            {
                "name": "Customer",
                "columns": [
                    {"name": "customer_id", "description": "Unique identifier for customer"},
                    {"name": "name", "description": "Customer full name"},
                    {"name": "email", "description": "Customer email address"},
                    {"name": "phone", "description": "Customer phone number"},
                    {"name": "registration_date", "description": "Date when customer registered"}
                ],
                "primary_key": ["customer_id"],
                "foreign_keys": [],
                "unique_constraints": []
            },
            {
                "name": "Order",
                "columns": [
                    {"name": "order_id", "description": "Unique identifier for order"},
                    {"name": "customer_id", "description": "Reference to customer"},
                    {"name": "order_date", "description": "Date when order was placed"},
                    {"name": "total_amount", "description": "Total order amount in currency"},
                    {"name": "status", "description": "Order status (pending, completed, cancelled)"}
                ],
                "primary_key": ["order_id"],
                "foreign_keys": [
                    {
                        "attributes": ["customer_id"],
                        "references_table": "Customer",
                        "referenced_attributes": ["customer_id"]
                    }
                ],
                "unique_constraints": []
            }
        ]
    }


def create_test_state_with_relational_schema() -> Dict[str, Any]:
    """Create test state that includes a relational schema."""
    return {
        "entities": [
            {
                "name": "Customer",
                "description": "A customer entity"
            },
            {
                "name": "Order",
                "description": "An order entity"
            }
        ],
        "attributes": {
            "Customer": [
                {
                    "name": "customer_id",
                    "description": "Unique identifier for customer",
                    "type_hint": "integer"
                },
                {
                    "name": "name",
                    "description": "Customer full name",
                    "type_hint": "string"
                },
                {
                    "name": "email",
                    "description": "Customer email address",
                    "type_hint": "string"
                },
                {
                    "name": "phone",
                    "description": "Customer phone number",
                    "type_hint": "string"
                },
                {
                    "name": "registration_date",
                    "description": "Date when customer registered",
                    "type_hint": "date"
                }
            ],
            "Order": [
                {
                    "name": "order_id",
                    "description": "Unique identifier for order",
                    "type_hint": "integer"
                },
                {
                    "name": "customer_id",
                    "description": "Reference to customer",
                    "type_hint": "integer"
                },
                {
                    "name": "order_date",
                    "description": "Date when order was placed",
                    "type_hint": "date"
                },
                {
                    "name": "total_amount",
                    "description": "Total order amount in currency",
                    "type_hint": "decimal"
                },
                {
                    "name": "status",
                    "description": "Order status (pending, completed, cancelled)",
                    "type_hint": "string"
                }
            ]
        },
        "primary_keys": {
            "Customer": ["customer_id"],
            "Order": ["order_id"]
        },
        "relations": [
            {
                "entities": ["Customer", "Order"],
                "description": "Customer places orders"
            }
        ],
        "relation_cardinalities": {
            "Customer+Order": {
                "Customer": "1",
                "Order": "N"
            }
        },
        "foreign_keys": [
            {
                "from_entity": "Order",
                "from_attributes": ["customer_id"],
                "to_entity": "Customer",
                "to_attributes": ["customer_id"]
            }
        ],
        "derived_formulas": {},
        "relational_schema": create_test_relational_schema(),
        "domain": "e-commerce",
        "nl_description": "A simple e-commerce system with customers and orders"
    }


async def test_datatype_assignment_with_relational_schema():
    """Test datatype assignment flow with actual relational schema."""
    print("=" * 80)
    print("DATATYPE ASSIGNMENT TEST WITH RELATIONAL SCHEMA")
    print("=" * 80)
    
    state = create_test_state_with_relational_schema()
    relational_schema = state["relational_schema"]
    
    print(f"\n[Setup] Relational schema has {len(relational_schema.get('tables', []))} tables")
    for table in relational_schema.get("tables", []):
        print(f"  - {table.get('name')}: {len(table.get('columns', []))} columns")
    
    # Step 5.1: Build attribute dependency graph
    print("\n[Step 5.1] Building attribute dependency graph...")
    try:
        result_5_1 = step_5_1_attribute_dependency_graph(
            entities=state["entities"],
            attributes=state["attributes"],
            primary_keys=state["primary_keys"],
            relations=state["relations"],
            relation_cardinalities=state["relation_cardinalities"],
            foreign_keys=state.get("foreign_keys"),
            derived_formulas=state.get("derived_formulas", {}),
        )
        
        print(f"[OK] Step 5.1 completed successfully")
        print(f"  - Created FKs: {len(result_5_1.get('created_foreign_keys', []))}")
        print(f"  - Independent attributes: {len(result_5_1.get('independent_attributes', []))}")
        print(f"  - Dependent attributes: {len(result_5_1.get('dependent_attributes', []))}")
        
        independent = result_5_1.get("independent_attributes", [])
        if independent:
            print(f"  - Independent attributes: {independent[:5]}..." if len(independent) > 5 else f"  - Independent attributes: {independent}")
        else:
            print(f"  [WARNING] No independent attributes found!")
        
    except Exception as e:
        print(f"[ERROR] Step 5.1 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Extract dependency information
    dependency_graph = result_5_1.get("dependency_graph", {})
    independent_attributes = result_5_1.get("independent_attributes", [])
    dependent_attributes = result_5_1.get("dependent_attributes", [])
    fk_dependencies = result_5_1.get("fk_dependencies", {})
    derived_dependencies = result_5_1.get("derived_dependencies", {})
    created_fks = result_5_1.get("created_foreign_keys", [])
    
    # Extract entity descriptions
    entity_descriptions = {}
    for entity in state["entities"]:
        entity_name = entity.get("name", "")
        entity_desc = entity.get("description", "")
        if entity_name:
            entity_descriptions[entity_name] = entity_desc
    
    # Step 5.2: Assign types to independent attributes
    print("\n[Step 5.2] Assigning types to independent attributes...")
    try:
        if not independent_attributes:
            print("  [WARNING] No independent attributes to process!")
            independent_types = {}
        else:
            result_5_2 = await step_5_2_independent_attribute_data_types_batch(
                independent_attributes=independent_attributes,
                attributes=state["attributes"],
                primary_keys=state["primary_keys"],
                domain=state.get("domain"),
                entity_descriptions=entity_descriptions,
                nl_description=state.get("nl_description", ""),
            )
            
            independent_types = result_5_2.get("data_types", {})
            print(f"[OK] Step 5.2 completed successfully")
            print(f"  - Assigned types to {len(independent_types)} attributes")
            if independent_types:
                print(f"  - Sample types assigned:")
                for i, (key, type_info) in enumerate(list(independent_types.items())[:3]):
                    print(f"    * {key}: {type_info.get('type', 'N/A')}")
            else:
                print(f"  [ERROR] No types were assigned!")
                print(f"  - Result keys: {list(result_5_2.keys())}")
                return None
    except Exception as e:
        print(f"[ERROR] Step 5.2 failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Step 5.3: Derive FK types from PK types
    print("\n[Step 5.3] Deriving FK types from PK types...")
    try:
        foreign_keys = created_fks if created_fks else state.get("foreign_keys", [])
        
        if not foreign_keys:
            print("  [INFO] No foreign keys to process")
            fk_types = {}
        else:
            result_5_3 = step_5_3_deterministic_fk_data_types(
                foreign_keys=foreign_keys,
                independent_attribute_data_types=independent_types,
            )
            
            fk_types = result_5_3.get("fk_data_types", {})
            print(f"[OK] Step 5.3 completed successfully")
            print(f"  - Assigned types to {len(fk_types)} FK attributes")
            if fk_types:
                print(f"  - FK types: {list(fk_types.keys())}")
    except Exception as e:
        print(f"[ERROR] Step 5.3 failed: {e}")
        import traceback
        traceback.print_exc()
        fk_types = {}
    
    # Step 5.4: Assign types to dependent attributes
    print("\n[Step 5.4] Assigning types to dependent attributes...")
    try:
        # Filter out FK attributes from dependent_attributes
        fk_keys = set(fk_dependencies.keys())
        non_fk_dependent = [(e, a) for e, a in dependent_attributes if f"{e}.{a}" not in fk_keys]
        
        if not non_fk_dependent:
            print("  [INFO] No non-FK dependent attributes to process")
            dependent_types = {}
        else:
            result_5_4 = await step_5_4_dependent_attribute_data_types_batch(
                dependent_attributes=non_fk_dependent,
                attributes=state["attributes"],
                dependency_graph=dependency_graph,
                fk_dependencies=fk_dependencies,
                derived_dependencies=derived_dependencies,
                independent_types=independent_types,
                fk_types=fk_types,
                primary_keys=state["primary_keys"],
                derived_formulas=state.get("derived_formulas") if state.get("derived_formulas") else None,
                domain=state.get("domain"),
                nl_description=state.get("nl_description", ""),
            )
            
            dependent_types = result_5_4.get("data_types", {})
            print(f"[OK] Step 5.4 completed successfully")
            print(f"  - Assigned types to {len(dependent_types)} dependent attributes")
            if dependent_types:
                print(f"  - Dependent types: {list(dependent_types.keys())}")
    except Exception as e:
        print(f"[ERROR] Step 5.4 failed: {e}")
        import traceback
        traceback.print_exc()
        dependent_types = {}
    
    # Combine all type assignments
    print("\n[Summary] Combining all type assignments...")
    all_data_types = {**independent_types, **fk_types, **dependent_types}
    print(f"  - Total types assigned: {len(all_data_types)}")
    print(f"  - Independent: {len(independent_types)}")
    print(f"  - FK: {len(fk_types)}")
    print(f"  - Dependent: {len(dependent_types)}")
    
    if not all_data_types:
        print(f"\n  [ERROR] No types were assigned at all!")
        print(f"  This indicates a critical problem in the datatype assignment flow.")
        return None
    
    print(f"\n  All assigned types:")
    for key, type_info in all_data_types.items():
        type_str = type_info.get("type", "N/A")
        size = type_info.get("size")
        precision = type_info.get("precision")
        scale = type_info.get("scale")
        if size:
            type_str = f"{type_str}({size})"
        elif precision is not None and scale is not None:
            type_str = f"{type_str}({precision},{scale})"
        print(f"    * {key}: {type_str}")
    
    # Convert to entity-organized format (as backend does)
    print("\n[Format Conversion] Converting to entity-organized format...")
    entity_data_types = {}
    for attr_key, type_info in all_data_types.items():
        if "." in attr_key:
            entity_name, attr_name = attr_key.split(".", 1)
            if entity_name not in entity_data_types:
                entity_data_types[entity_name] = {"attribute_types": {}}
            entity_data_types[entity_name]["attribute_types"][attr_name] = type_info
    
    print(f"  - Entities with types: {list(entity_data_types.keys())}")
    for entity_name, entity_types in entity_data_types.items():
        attr_types = entity_types.get("attribute_types", {})
        print(f"    * {entity_name}: {len(attr_types)} attributes")
        for attr_name in list(attr_types.keys())[:3]:
            print(f"      - {attr_name}: {attr_types[attr_name].get('type', 'N/A')}")
    
    # Verify against relational schema
    print("\n[Verification] Verifying types match relational schema...")
    schema_tables = relational_schema.get("tables", [])
    for table in schema_tables:
        table_name = table.get("name", "")
        if table_name in entity_data_types:
            table_columns = {col.get("name") for col in table.get("columns", [])}
            assigned_attrs = set(entity_data_types[table_name].get("attribute_types", {}).keys())
            missing = table_columns - assigned_attrs
            extra = assigned_attrs - table_columns
            if missing:
                print(f"  [WARNING] {table_name}: Missing types for columns: {missing}")
            if extra:
                print(f"  [WARNING] {table_name}: Extra types for attributes not in schema: {extra}")
            if not missing and not extra:
                print(f"  [OK] {table_name}: All columns have types assigned")
        else:
            print(f"  [WARNING] {table_name}: No types assigned (entity not found in data_types)")
    
    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
    
    return {
        "independent_types": independent_types,
        "fk_types": fk_types,
        "dependent_types": dependent_types,
        "all_data_types": all_data_types,
        "entity_data_types": entity_data_types
    }


if __name__ == "__main__":
    result = asyncio.run(test_datatype_assignment_with_relational_schema())
    if result and result["all_data_types"]:
        print("\n[PASS] Test passed: Types were assigned successfully")
        print(f"Total attributes with types: {len(result['all_data_types'])}")
        sys.exit(0)
    else:
        print("\n[FAIL] Test failed: No types were assigned")
        sys.exit(1)
