"""Integration tests for Phase 6 to diagnose data flow issues.

These tests verify that Phase 6 receives the correct data structure from Phase 4.
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_1_ddl_compilation import step_6_1_ddl_compilation
from NL2DATA.orchestration.graphs.phase6 import _wrap_step_6_1
from NL2DATA.orchestration.state import IRGenerationState
from NL2DATA.tests.utils.test_helpers import TestResultDisplay


async def test_step_6_1_with_relational_schema_structure():
    """Test Step 6.1 with the actual structure that Phase 4 produces."""
    TestResultDisplay.print_test_header("Phase 6 Integration", "6.1")
    TestResultDisplay.print_test_case(1, "Testing with relational_schema structure from Phase 4")
    
    # Simulate what Phase 4 produces - relational_schema with normalized_tables
    relational_schema = {
        "normalized_tables": [
            {
                "name": "Session",
                "columns": [
                    {"name": "session_id", "nullable": False},
                    {"name": "user_id", "nullable": False},
                    {"name": "device_id", "nullable": False},
                    {"name": "start_time", "nullable": False},
                    {"name": "end_time", "nullable": True}
                ],
                "primary_key": ["session_id"],
                "foreign_keys": [
                    {
                        "attributes": ["user_id"],
                        "references_table": "User",
                        "referenced_attributes": ["user_id"]
                    },
                    {
                        "attributes": ["device_id"],
                        "references_table": "Device",
                        "referenced_attributes": ["device_id"]
                    }
                ]
            },
            {
                "name": "User",
                "columns": [
                    {"name": "user_id", "nullable": False},
                    {"name": "username", "nullable": False},
                    {"name": "email", "nullable": True}
                ],
                "primary_key": ["user_id"]
            },
            {
                "name": "Device",
                "columns": [
                    {"name": "device_id", "nullable": False},
                    {"name": "device_type", "nullable": False},
                    {"name": "os", "nullable": True}
                ],
                "primary_key": ["device_id"]
            }
        ]
    }
    
    data_types = {
        "Session": {
            "session_id": {"type": "BIGINT"},
            "user_id": {"type": "BIGINT"},
            "device_id": {"type": "BIGINT"},
            "start_time": {"type": "TIMESTAMP"},
            "end_time": {"type": "TIMESTAMP"}
        },
        "User": {
            "user_id": {"type": "BIGINT"},
            "username": {"type": "VARCHAR(255)"},
            "email": {"type": "VARCHAR(255)"}
        },
        "Device": {
            "device_id": {"type": "BIGINT"},
            "device_type": {"type": "VARCHAR(50)"},
            "os": {"type": "VARCHAR(50)"}
        }
    }
    
    print("\nInput relational_schema structure:")
    print(f"  - Has 'normalized_tables': {'normalized_tables' in relational_schema}")
    print(f"  - Number of tables: {len(relational_schema.get('normalized_tables', []))}")
    
    result = step_6_1_ddl_compilation(
        normalized_schema=relational_schema,
        data_types=data_types
    )
    
    ddl_statements = result.ddl_statements if hasattr(result, 'ddl_statements') else result.get('ddl_statements', [])
    
    print(f"\nResult:")
    print(f"  - Number of DDL statements: {len(ddl_statements)}")
    print(f"  - Expected: 3")
    
    if len(ddl_statements) > 0:
        print(f"\nFirst DDL statement (first 200 chars):")
        print(ddl_statements[0][:200])
    
    success = len(ddl_statements) == 3
    if not success:
        print(f"\n[FAIL] Expected 3 DDL statements, got {len(ddl_statements)}")
    else:
        print(f"\n[PASS] Generated {len(ddl_statements)} DDL statements correctly")
    
    return success


async def test_wrap_step_6_1_with_metadata():
    """Test the wrapper function with actual state structure."""
    TestResultDisplay.print_test_header("Phase 6 Integration", "Wrapper")
    TestResultDisplay.print_test_case(2, "Testing _wrap_step_6_1 with metadata structure (tables format)")
    
    # Create state as it would come from Phase 4 (Phase 4 outputs "tables", not "normalized_tables")
    state: IRGenerationState = {
        "nl_description": "Test description",
        "phase": 6,
        "current_step": "",
        "previous_answers": {},
        "metadata": {
            "relational_schema": {
                "tables": [
                    {
                        "name": "Customer",
                        "columns": [
                            {"name": "customer_id", "nullable": False},
                            {"name": "name", "nullable": False}
                        ],
                        "primary_key": ["customer_id"]
                    }
                ]
            }
        },
        "data_types": {
            "Customer": {
                "customer_id": {"type": "INTEGER"},
                "name": {"type": "VARCHAR(255)"}
            }
        }
    }
    
    print("\nState structure:")
    print(f"  - Has metadata: {'metadata' in state}")
    print(f"  - Has relational_schema in metadata: {'relational_schema' in state.get('metadata', {})}")
    relational_schema = state.get('metadata', {}).get('relational_schema', {})
    print(f"  - Has 'tables': {'tables' in relational_schema}")
    print(f"  - Has 'normalized_tables': {'normalized_tables' in relational_schema}")
    print(f"  - Number of tables: {len(relational_schema.get('tables', relational_schema.get('normalized_tables', [])))}")
    
    # Import the step function
    from NL2DATA.phases.phase6.step_6_1_ddl_compilation import step_6_1_ddl_compilation
    
    # Create wrapper
    wrapped = _wrap_step_6_1(step_6_1_ddl_compilation)
    
    # Execute
    result = await wrapped(state)
    
    print(f"\nWrapper result:")
    print(f"  - Has current_step: {'current_step' in result}")
    print(f"  - Has previous_answers: {'previous_answers' in result}")
    print(f"  - Has metadata: {'metadata' in result}")
    
    metadata = result.get('metadata', {})
    ddl_statements = metadata.get('ddl_statements', [])
    
    print(f"  - DDL statements in metadata: {len(ddl_statements)}")
    
    if len(ddl_statements) > 0:
        print(f"\nFirst DDL statement (first 200 chars):")
        print(ddl_statements[0][:200])
        success = True
    else:
        print(f"\n[FAIL] No DDL statements generated!")
        success = False
    
    return success


async def test_empty_relational_schema():
    """Test what happens when relational_schema is empty or missing normalized_tables."""
    TestResultDisplay.print_test_header("Phase 6 Integration", "Empty Schema")
    TestResultDisplay.print_test_case(3, "Testing with empty/missing normalized_tables")
    
    # Test case 1: Empty relational_schema
    print("\nTest 1: Empty relational_schema")
    result1 = step_6_1_ddl_compilation(normalized_schema={})
    ddl1 = result1.ddl_statements if hasattr(result1, 'ddl_statements') else result1.get('ddl_statements', [])
    print(f"  - DDL statements: {len(ddl1)}")
    
    # Test case 2: relational_schema without normalized_tables
    print("\nTest 2: relational_schema without normalized_tables")
    result2 = step_6_1_ddl_compilation(normalized_schema={"tables": []})
    ddl2 = result2.ddl_statements if hasattr(result2, 'ddl_statements') else result2.get('ddl_statements', [])
    print(f"  - DDL statements: {len(ddl2)}")
    
    # Test case 3: relational_schema with empty normalized_tables
    print("\nTest 3: relational_schema with empty normalized_tables")
    result3 = step_6_1_ddl_compilation(normalized_schema={"normalized_tables": []})
    ddl3 = result3.ddl_statements if hasattr(result3, 'ddl_statements') else result3.get('ddl_statements', [])
    print(f"  - DDL statements: {len(ddl3)}")
    
    # All should return empty lists
    success = len(ddl1) == 0 and len(ddl2) == 0 and len(ddl3) == 0
    if success:
        print("\n[PASS] All empty schema cases handled correctly")
    else:
        print(f"\n[FAIL] Unexpected behavior with empty schemas")
    
    return success


async def test_phase_4_to_phase_6_data_flow():
    """Test the actual data flow from Phase 4 output to Phase 6 input."""
    TestResultDisplay.print_test_header("Phase 6 Integration", "Data Flow")
    TestResultDisplay.print_test_case(4, "Testing Phase 4 -> Phase 6 data flow")
    
    # Simulate Phase 4 output structure
    phase_4_output = {
        "relational_schema": {
            "normalized_tables": [
                {
                    "name": "Product",
                    "columns": [
                        {"name": "product_id", "nullable": False},
                        {"name": "name", "nullable": False},
                        {"name": "price", "nullable": False}
                    ],
                    "primary_key": ["product_id"]
                }
            ]
        }
    }
    
    # Simulate how Phase 4 stores this in state
    state_after_phase_4: IRGenerationState = {
        "nl_description": "Test",
        "phase": 4,
        "metadata": phase_4_output
    }
    
    print("\nPhase 4 output structure:")
    print(f"  - Keys: {list(phase_4_output.keys())}")
    print(f"  - Has relational_schema: {'relational_schema' in phase_4_output}")
    
    relational_schema = phase_4_output.get("relational_schema", {})
    print(f"  - relational_schema keys: {list(relational_schema.keys())}")
    print(f"  - Has normalized_tables: {'normalized_tables' in relational_schema}")
    
    # Now simulate Phase 6 reading from state
    metadata = state_after_phase_4.get("metadata", {})
    relational_schema_from_state = metadata.get("relational_schema", {})
    
    print(f"\nPhase 6 reading from state:")
    print(f"  - metadata keys: {list(metadata.keys())}")
    print(f"  - relational_schema from state: {type(relational_schema_from_state)}")
    print(f"  - Has normalized_tables: {'normalized_tables' in relational_schema_from_state}")
    
    # Test Step 6.1 with this structure
    result = step_6_1_ddl_compilation(
        normalized_schema=relational_schema_from_state,
        data_types={}
    )
    
    ddl_statements = result.ddl_statements if hasattr(result, 'ddl_statements') else result.get('ddl_statements', [])
    
    print(f"\nStep 6.1 result:")
    print(f"  - DDL statements: {len(ddl_statements)}")
    
    success = len(ddl_statements) == 1
    if success:
        print(f"\n[PASS] Data flow works correctly")
    else:
        print(f"\n[FAIL] Data flow issue - expected 1 DDL, got {len(ddl_statements)}")
    
    return success


async def main():
    """Run all Phase 6 integration tests."""
    print("=" * 80)
    print("PHASE 6 INTEGRATION TESTS")
    print("=" * 80)
    
    results = []
    results.append(await test_step_6_1_with_relational_schema_structure())
    results.append(await test_wrap_step_6_1_with_metadata())
    results.append(await test_empty_relational_schema())
    results.append(await test_phase_4_to_phase_6_data_flow())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Phase 6 integration tests passed!")
    else:
        print("[FAIL] Some Phase 6 integration tests failed")
        print(f"Results: {results}")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
