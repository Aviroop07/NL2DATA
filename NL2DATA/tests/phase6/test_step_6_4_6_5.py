"""Test script for Steps 6.4 and 6.5: Constraint Conflict Detection and Compilation.

These are deterministic steps that can be tested without LLM calls.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6 import (
    step_6_4_constraint_conflict_detection,
    step_6_5_constraint_compilation,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

def test_steps_6_4_6_5():
    """Test Steps 6.4 and 6.5: Constraint Conflict Detection and Compilation."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=get_config('logging')['log_to_file'],
        log_file=get_config('logging')['log_file']
    )
    
    print("=" * 80)
    print("Testing Steps 6.4 and 6.5: Constraint Conflict Detection and Compilation")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Constraints with conflicts
        print("\n" + "-" * 80)
        print("Test Case 1: Constraints with potential conflicts")
        print("-" * 80)
        
        constraints = [
            {
                "description": "Amount must be positive",
                "dsl_expression": "amount > 0",
                "affected_attributes": ["Transaction.amount"],
                "constraint_category": "structural",
                "enforcement_type": "check_constraint",
            },
            {
                "description": "Amount must be less than 1000",
                "dsl_expression": "amount < 1000",
                "affected_attributes": ["Transaction.amount"],
                "constraint_category": "structural",
                "enforcement_type": "check_constraint",
            },
            {
                "description": "Age must be at least 18",
                "dsl_expression": "age >= 18",
                "affected_attributes": ["Customer.age"],
                "constraint_category": "structural",
                "enforcement_type": "check_constraint",
            },
        ]
        
        # Step 6.4: Constraint Conflict Detection
        print("\nStep 6.4: Constraint Conflict Detection")
        result_6_4 = step_6_4_constraint_conflict_detection(
            constraints=constraints,
        )
        
        validation_passed = result_6_4.get("validation_passed", False)
        conflicts = result_6_4.get("conflicts", [])
        
        print(f"[PASS] Step 6.4 completed")
        print(f"  - Validation passed: {validation_passed}")
        print(f"  - Conflicts detected: {len(conflicts)}")
        
        for conflict in conflicts:
            print(f"    - {conflict.get('constraint1', '')} vs {conflict.get('constraint2', '')}")
            print(f"      Type: {conflict.get('conflict_type', '')}")
        
        print(f"    [OK] Conflict detection completed")
        
        # Step 6.5: Constraint Compilation
        print("\nStep 6.5: Constraint Compilation")
        result_6_5 = step_6_5_constraint_compilation(
            constraints=constraints,
        )
        
        statistical = result_6_5.get("statistical_constraints", [])
        structural = result_6_5.get("structural_constraints", [])
        distribution = result_6_5.get("distribution_constraints", [])
        other = result_6_5.get("other_constraints", [])
        
        print(f"[PASS] Step 6.5 completed")
        print(f"  - Statistical constraints: {len(statistical)}")
        print(f"  - Structural constraints: {len(structural)}")
        print(f"  - Distribution constraints: {len(distribution)}")
        print(f"  - Other constraints: {len(other)}")
        
        if len(structural) != 3:
            print(f"    [ERROR] Expected 3 structural constraints, got {len(structural)}")
            all_passed = False
        else:
            print(f"    [OK] Constraint compilation successful")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All Steps 6.4 and 6.5 tests passed!")
        else:
            print("[ERROR] Some Steps 6.4 and 6.5 tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Steps 6.4 and 6.5 test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_steps_6_4_6_5()
    sys.exit(0 if success else 1)

