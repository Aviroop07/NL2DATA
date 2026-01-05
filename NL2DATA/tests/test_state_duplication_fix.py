"""Test script to verify state duplication fix.

This test verifies that:
1. Phase 7 and Phase 8 nodes don't cause exponential duplication
2. information_needs, constraints, and functional_dependencies remain stable
3. State validation detects any duplication issues
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NL2DATA.orchestration.state import create_initial_state, IRGenerationState
from NL2DATA.orchestration.graphs.phase7 import create_phase_7_graph
from NL2DATA.orchestration.graphs.phase8 import create_phase_8_graph
from NL2DATA.utils.validation.state_validation import validate_no_list_duplication
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


async def test_state_no_duplication():
    """Test that phases 7 and 8 don't cause state duplication."""
    print("=" * 80)
    print("Testing State Duplication Fix")
    print("=" * 80)
    
    # Create a minimal state that would have passed Phase 6
    initial_state = create_initial_state(
        "A simple database for tracking sessions. Each session has a duration and status."
    )
    
    # Populate minimal required state for Phase 7
    initial_state.update({
        "phase": 6,
        "current_step": "6.3",
        "entities": [
            {"name": "Session", "description": "A user session"}
        ],
        "relations": [],
        "attributes": {
            "Session": [
                {"name": "session_id", "description": "Unique session identifier"},
                {"name": "duration", "description": "Session duration in seconds"},
                {"name": "status", "description": "Session status"}
            ]
        },
        "primary_keys": {
            "Session": ["session_id"]
        },
        "foreign_keys": [],
        "data_types": {
            "Session": {
                "session_id": {"type": "INTEGER"},
                "duration": {"type": "INTEGER"},
                "status": {"type": "VARCHAR"}
            }
        },
        "metadata": {
            "relational_schema": {
                "Session": {
                    "columns": {
                        "session_id": {"type": "INTEGER", "primary_key": True},
                        "duration": {"type": "INTEGER"},
                        "status": {"type": "VARCHAR"}
                    }
                }
            }
        }
    })
    
    print("\nInitial state:")
    print(f"  information_needs: {len(initial_state.get('information_needs', []))} items")
    print(f"  constraints: {len(initial_state.get('constraints', []))} items")
    print(f"  functional_dependencies: {len(initial_state.get('functional_dependencies', []))} items")
    
    # Test Phase 7
    print("\n" + "-" * 80)
    print("Running Phase 7...")
    print("-" * 80)
    
    phase7_graph = create_phase_7_graph()
    config = {"configurable": {"thread_id": "test-7"}}
    
    phase7_state = initial_state.copy()
    try:
        async for state_update in phase7_graph.astream(phase7_state, config=config, stream_mode="values"):
            phase7_state = state_update
        
        print(f"\nAfter Phase 7:")
        print(f"  information_needs: {len(phase7_state.get('information_needs', []))} items")
        info_needs = phase7_state.get('information_needs', [])
        if info_needs:
            unique_descriptions = len(set(
                need.get('description', '') for need in info_needs if isinstance(need, dict)
            ))
            print(f"  Unique descriptions: {unique_descriptions}")
        
        # Check for duplication
        duplication_issues = validate_no_list_duplication(phase7_state, raise_on_error=False)
        if duplication_issues:
            print(f"\n[WARNING] Duplication detected after Phase 7:")
            for issue in duplication_issues:
                print(f"  - {issue}")
        else:
            print("\n[OK] No duplication detected after Phase 7")
    
    except Exception as e:
        print(f"\n[ERROR] Phase 7 failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test Phase 8
    print("\n" + "-" * 80)
    print("Running Phase 8...")
    print("-" * 80)
    
    phase8_graph = create_phase_8_graph()
    config = {"configurable": {"thread_id": "test-8"}}
    
    phase8_state = phase7_state.copy()
    try:
        async for state_update in phase8_graph.astream(phase8_state, config=config, stream_mode="values"):
            phase8_state = state_update
        
        print(f"\nAfter Phase 8:")
        print(f"  information_needs: {len(phase8_state.get('information_needs', []))} items")
        print(f"  constraints: {len(phase8_state.get('constraints', []))} items")
        print(f"  functional_dependencies: {len(phase8_state.get('functional_dependencies', []))} items")
        
        info_needs = phase8_state.get('information_needs', [])
        if info_needs:
            unique_descriptions = len(set(
                need.get('description', '') for need in info_needs if isinstance(need, dict)
            ))
            print(f"  Unique info need descriptions: {unique_descriptions}")
        
        constraints = phase8_state.get('constraints', [])
        if constraints:
            unique_constraint_ids = len(set(
                c.get('constraint_id', '') or c.get('id', '') 
                for c in constraints if isinstance(c, dict)
            ))
            print(f"  Unique constraint IDs: {unique_constraint_ids}")
        
        fds = phase8_state.get('functional_dependencies', [])
        if fds:
            unique_fd_signatures = len(set(
                (
                    tuple(sorted(fd.get('determinants', []))),
                    tuple(sorted(fd.get('dependents', []))),
                    fd.get('table', '')
                )
                for fd in fds if isinstance(fd, dict)
            ))
            print(f"  Unique FD signatures: {unique_fd_signatures}")
        
        # Check for duplication
        duplication_issues = validate_no_list_duplication(phase8_state, raise_on_error=False)
        if duplication_issues:
            print(f"\n[FAIL] Duplication detected after Phase 8:")
            for issue in duplication_issues:
                print(f"  - {issue}")
            return False
        else:
            print("\n[OK] No duplication detected after Phase 8")
        
        # Verify state didn't explode
        info_needs_count = len(info_needs)
        if info_needs_count > 100:
            print(f"\n[FAIL] information_needs exploded to {info_needs_count} items (expected < 100)")
            return False
        
        constraints_count = len(constraints)
        if constraints_count > 100:
            print(f"\n[FAIL] constraints exploded to {constraints_count} items (expected < 100)")
            return False
        
        fds_count = len(fds)
        if fds_count > 100:
            print(f"\n[FAIL] functional_dependencies exploded to {fds_count} items (expected < 100)")
            return False
        
        print("\n[SUCCESS] State remains stable - no exponential duplication detected!")
        return True
    
    except Exception as e:
        print(f"\n[ERROR] Phase 8 failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run the duplication test."""
    success = await test_state_no_duplication()
    
    print("\n" + "=" * 80)
    if success:
        print("[PASS] State duplication fix test passed!")
    else:
        print("[FAIL] State duplication fix test failed!")
    print("=" * 80)
    
    return success


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
