"""Test Phase 6 with actual pipeline state structure.

This test simulates the actual state structure from a real pipeline run
to diagnose why normalized_tables might be missing.
"""

import sys
import asyncio
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_1_ddl_compilation import step_6_1_ddl_compilation
from NL2DATA.orchestration.graphs.phase6 import _wrap_step_6_1
from NL2DATA.orchestration.state import IRGenerationState
from NL2DATA.tests.utils.test_helpers import TestResultDisplay


async def test_with_actual_pipeline_state():
    """Test with state structure from an actual pipeline run."""
    TestResultDisplay.print_test_header("Phase 6 Pipeline Simulation", "Real State")
    TestResultDisplay.print_test_case(1, "Testing with actual pipeline state structure")
    
    # Load actual state from a recent run
    run_dir = Path(__file__).parent.parent.parent.parent / "runs" / "phases_1_6_desc_001_20260105_164808"
    state_file = run_dir / "state.json"
    
    if not state_file.exists():
        print(f"\n[SKIP] State file not found: {state_file}")
        print("This test requires a recent pipeline run state.json file")
        return True
    
    print(f"\nLoading state from: {state_file}")
    with open(state_file, 'r', encoding='utf-8') as f:
        state_data = json.load(f)
    
    print(f"\nState structure analysis:")
    print(f"  - Top-level keys: {list(state_data.keys())}")
    
    metadata = state_data.get("metadata", {})
    print(f"  - Metadata keys: {list(metadata.keys())}")
    
    relational_schema = metadata.get("relational_schema", {})
    print(f"  - relational_schema type: {type(relational_schema)}")
    
    if isinstance(relational_schema, dict):
        print(f"  - relational_schema keys: {list(relational_schema.keys())}")
        print(f"  - Has normalized_tables: {'normalized_tables' in relational_schema}")
        if 'normalized_tables' in relational_schema:
            print(f"  - Number of normalized_tables: {len(relational_schema['normalized_tables'])}")
    elif isinstance(relational_schema, str):
        print(f"  - relational_schema is a string (length: {len(relational_schema)})")
        print(f"  - First 200 chars: {relational_schema[:200]}")
        # Try to parse as JSON
        try:
            relational_schema_parsed = json.loads(relational_schema)
            print(f"  - Parsed as JSON successfully")
            print(f"  - Parsed keys: {list(relational_schema_parsed.keys())}")
            if 'normalized_tables' in relational_schema_parsed:
                print(f"  - Number of normalized_tables: {len(relational_schema_parsed['normalized_tables'])}")
        except:
            print(f"  - Could not parse as JSON")
    else:
        print(f"  - relational_schema is {type(relational_schema)}")
    
    # Try to use this state with Step 6.1
    print(f"\nTesting Step 6.1 with this relational_schema:")
    
    if isinstance(relational_schema, dict) and 'normalized_tables' in relational_schema:
        result = step_6_1_ddl_compilation(
            normalized_schema=relational_schema,
            data_types=state_data.get("data_types", {})
        )
        ddl_statements = result.ddl_statements if hasattr(result, 'ddl_statements') else result.get('ddl_statements', [])
        print(f"  - Generated {len(ddl_statements)} DDL statements")
        success = len(ddl_statements) > 0
    else:
        print(f"  - [FAIL] relational_schema does not have normalized_tables")
        success = False
    
    return success


async def test_phase_4_output_structure():
    """Test what Phase 4 actually outputs."""
    TestResultDisplay.print_test_header("Phase 6 Pipeline Simulation", "Phase 4 Output")
    TestResultDisplay.print_test_case(2, "Testing Phase 4 output structure")
    
    # Import Phase 4 step to see what it outputs
    try:
        from NL2DATA.phases.phase4.step_4_1_relational_schema_compilation import (
            step_4_1_relational_schema_compilation,
            RelationalSchemaCompilationOutput
        )
        
        # Create a minimal ER design
        er_design = {
            "entities": [
                {
                    "name": "TestEntity",
                    "attributes": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR(255)"}
                    ]
                }
            ],
            "relations": []
        }
        
        print("\nTesting Phase 4 Step 4.1 with minimal input:")
        result = step_4_1_relational_schema_compilation(
            er_design=er_design,
            foreign_keys=[],
            primary_keys={"TestEntity": ["id"]},
            constraints=[],
            junction_table_names={}
        )
        
        print(f"\nPhase 4 output structure:")
        if hasattr(result, 'relational_schema'):
            relational_schema = result.relational_schema
            if hasattr(relational_schema, 'model_dump'):
                schema_dict = relational_schema.model_dump()
            elif hasattr(relational_schema, 'dict'):
                schema_dict = relational_schema.dict()
            else:
                schema_dict = relational_schema
        elif isinstance(result, dict):
            schema_dict = result.get('relational_schema', {})
        else:
            schema_dict = {}
        
        print(f"  - Type: {type(schema_dict)}")
        print(f"  - Keys: {list(schema_dict.keys()) if isinstance(schema_dict, dict) else 'N/A'}")
        has_tables = 'tables' in schema_dict if isinstance(schema_dict, dict) else False
        has_normalized_tables = 'normalized_tables' in schema_dict if isinstance(schema_dict, dict) else False
        print(f"  - Has 'tables': {has_tables}")
        print(f"  - Has 'normalized_tables': {has_normalized_tables}")
        
        if isinstance(schema_dict, dict) and (has_tables or has_normalized_tables):
            table_key = 'tables' if has_tables else 'normalized_tables'
            num_tables = len(schema_dict[table_key])
            print(f"  - Number of {table_key}: {num_tables}")
            
            # Test if this works with Step 6.1 (should handle both formats now)
            print(f"\nTesting Step 6.1 with Phase 4 output:")
            result_6_1 = step_6_1_ddl_compilation(
                normalized_schema=schema_dict,
                data_types={}
            )
            ddl_statements = result_6_1.ddl_statements if hasattr(result_6_1, 'ddl_statements') else result_6_1.get('ddl_statements', [])
            print(f"  - Generated {len(ddl_statements)} DDL statements")
            success = len(ddl_statements) > 0
            if not success:
                print(f"  - [FAIL] Step 6.1 should handle 'tables' format from Phase 4")
        else:
            print(f"  - [FAIL] Phase 4 output does not have 'tables' or 'normalized_tables'")
            success = False
        
    except Exception as e:
        print(f"\n[ERROR] Could not test Phase 4 output: {e}")
        import traceback
        traceback.print_exc()
        success = False
    
    return success


async def test_metadata_storage_simulation():
    """Simulate how metadata is stored and retrieved in the pipeline."""
    TestResultDisplay.print_test_header("Phase 6 Pipeline Simulation", "Metadata Storage")
    TestResultDisplay.print_test_case(3, "Testing metadata storage and retrieval")
    
    # Simulate Phase 4 storing relational_schema
    phase_4_result = {
        "relational_schema": {
            "normalized_tables": [
                {
                    "name": "TestTable",
                    "columns": [{"name": "id", "nullable": False}],
                    "primary_key": ["id"]
                }
            ]
        }
    }
    
    # Simulate state update (how Phase 4 wrapper does it)
    state: IRGenerationState = {
        "nl_description": "Test",
        "phase": 4,
        "metadata": {}
    }
    
    # Phase 4 wrapper would do something like this
    if hasattr(phase_4_result, 'relational_schema'):
        relational_schema = phase_4_result.relational_schema
        if hasattr(relational_schema, 'model_dump'):
            schema_dict = relational_schema.model_dump()
        else:
            schema_dict = relational_schema
    else:
        schema_dict = phase_4_result.get('relational_schema', {})
    
    # Update metadata
    state["metadata"] = {
        **state.get("metadata", {}),
        "relational_schema": schema_dict
    }
    
    print(f"\nState after Phase 4:")
    print(f"  - metadata keys: {list(state['metadata'].keys())}")
    print(f"  - relational_schema type: {type(state['metadata']['relational_schema'])}")
    print(f"  - Has normalized_tables: {'normalized_tables' in state['metadata']['relational_schema']}")
    
    # Now simulate Phase 6 reading
    metadata = state.get("metadata", {})
    relational_schema = metadata.get("relational_schema", {})
    
    print(f"\nPhase 6 reading:")
    print(f"  - relational_schema type: {type(relational_schema)}")
    print(f"  - Has normalized_tables: {'normalized_tables' in relational_schema if isinstance(relational_schema, dict) else False}")
    
    # Test Step 6.1
    result = step_6_1_ddl_compilation(
        normalized_schema=relational_schema if isinstance(relational_schema, dict) else {},
        data_types={}
    )
    
    ddl_statements = result.ddl_statements if hasattr(result, 'ddl_statements') else result.get('ddl_statements', [])
    print(f"  - Generated {len(ddl_statements)} DDL statements")
    
    success = len(ddl_statements) == 1
    return success


async def main():
    """Run all pipeline simulation tests."""
    print("=" * 80)
    print("PHASE 6 PIPELINE SIMULATION TESTS")
    print("=" * 80)
    
    results = []
    results.append(await test_with_actual_pipeline_state())
    results.append(await test_phase_4_output_structure())
    results.append(await test_metadata_storage_simulation())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Phase 6 pipeline simulation tests passed!")
    else:
        print("[FAIL] Some Phase 6 pipeline simulation tests failed")
        print(f"Results: {results}")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
