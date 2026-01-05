"""Unit tests for Step 8.7: Constraint Conflict Detection.

Note: This step currently has placeholder implementation.
Tests are prepared for when full implementation is available.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_7_constraint_conflict_detection import (
    step_8_7_constraint_conflict_detection,
    ConstraintConflictDetectionOutput,
    ConstraintConflict,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_7_basic():
    """Test Step 8.7 with basic input (no conflicts)."""
    TestResultDisplay.print_test_header("Constraint Conflict Detection", "8.7")
    TestResultDisplay.print_test_case(1, "Basic conflict detection with no conflicts")
    
    constraints = [
        {
            "constraint_id": "c1",
            "constraint_type": "range",
            "table": "Transaction",
            "column": "amount",
            "min_value": 0.01,
            "max_value": 1000000,
        },
        {
            "constraint_id": "c2",
            "constraint_type": "categorical",
            "table": "Transaction",
            "column": "status",
            "allowed_values": ["pending", "completed", "cancelled", "failed"],
        },
        {
            "constraint_id": "c3",
            "constraint_type": "not_null",
            "table": "Card",
            "column": "card_id",
        }
    ]
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = ConstraintConflictDetectionOutput(
        conflicts=[],
        conflict_count=0,
        resolved_constraints=None,
        resolution_applied=False,
    )
    
    with patch('NL2DATA.phases.phase8.step_8_7_constraint_conflict_detection.step_8_7_constraint_conflict_detection') as mock_step:
        mock_step.return_value = mock_response
        result = await step_8_7_constraint_conflict_detection(
            constraints=constraints,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["conflicts", "conflict_count", "resolution_applied"]
    )
    validations.append(struct_validation)
    
    # Validate types
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "conflicts": list,
            "conflict_count": int,
            "resolution_applied": bool,
        }
    )
    validations.append(type_validation)
    
    # Check conflict count matches conflicts list length
    if hasattr(result, 'conflict_count'):
        conflict_count = result.conflict_count
        conflicts_list = result.conflicts if hasattr(result, 'conflicts') else []
    elif isinstance(result, dict):
        conflict_count = result.get("conflict_count", 0)
        conflicts_list = result.get("conflicts", [])
    else:
        conflict_count = 0
        conflicts_list = []
    
    if conflict_count == len(conflicts_list):
        validations.append({
            "name": "Conflict count consistency",
            "valid": True,
            "message": f"Conflict count ({conflict_count}) matches conflicts list length ({len(conflicts_list)})"
        })
    else:
        validations.append({
            "name": "Conflict count consistency",
            "valid": False,
            "message": f"Conflict count ({conflict_count}) does not match conflicts list length ({len(conflicts_list)})"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.7")
    return all_valid


async def test_step_8_7_with_conflicts():
    """Test Step 8.7 with conflicting constraints - tests actual LLM call."""
    TestResultDisplay.print_test_header("Constraint Conflict Detection (With Conflicts)", "8.7")
    TestResultDisplay.print_test_case(2, "Conflict detection with overlapping/contradictory constraints - testing LLM resolution")
    
    constraints = [
        {
            "constraint_id": "c1",
            "constraint_type": "range",
            "table": "Transaction",
            "column": "amount",
            "min_value": 0.01,
            "max_value": 1000,
            "description": "Transaction amount must be between 0.01 and 1000",
        },
        {
            "constraint_id": "c2",
            "constraint_type": "range",
            "table": "Transaction",
            "column": "amount",
            "min_value": 5000,
            "max_value": 1000000,
            "description": "Transaction amount must be between 5000 and 1000000",
        },
        {
            "constraint_id": "c3",
            "constraint_type": "categorical",
            "table": "Transaction",
            "column": "status",
            "allowed_values": ["pending", "completed"],
            "description": "Transaction status must be pending or completed",
        },
        {
            "constraint_id": "c4",
            "constraint_type": "categorical",
            "table": "Transaction",
            "column": "status",
            "allowed_values": ["cancelled", "failed"],
            "description": "Transaction status must be cancelled or failed",
        }
    ]
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock the LLM call for conflict resolution
    from NL2DATA.phases.phase8.step_8_7_constraint_conflict_detection import (
        ConstraintConflictResolutionOutput,
        ConstraintResolution,
        ResolvedConstraint,
    )
    
    # Create mock resolved constraints - LLM should reconcile or remove conflicts
    mock_resolved_constraints = [
        ResolvedConstraint(
            constraint_id="c2",
            constraint_type="range",
            table="Transaction",
            column="amount",
            min_value=0.01,
            max_value=1000000,
            description="Reconciled range constraint: Transaction amount must be between 0.01 and 1000000",
        ),
        ResolvedConstraint(
            constraint_id="c3",
            constraint_type="categorical",
            table="Transaction",
            column="status",
            allowed_values=["pending", "completed", "cancelled", "failed"],
            description="Reconciled categorical constraint: Transaction status can be any of the specified values",
        ),
    ]
    
    mock_llm_response = ConstraintConflictResolutionOutput(
        resolutions=[
            ConstraintResolution(
                conflict_id="conflict_0",
                resolution_type="reconcile",
                constraint_to_remove=None,
                reconciled_constraint=mock_resolved_constraints[0],
                reasoning="Reconciled the two range constraints by taking the union of ranges (0.01-1000000)"
            ),
            ConstraintResolution(
                conflict_id="conflict_1",
                resolution_type="reconcile",
                constraint_to_remove=None,
                reconciled_constraint=mock_resolved_constraints[1],
                reasoning="Reconciled the two categorical constraints by combining allowed values"
            ),
        ],
        resolved_constraints=mock_resolved_constraints,
    )
    
    # Call the real function but mock only the LLM call inside _resolve_conflicts_with_llm
    with patch('NL2DATA.phases.phase8.step_8_7_constraint_conflict_detection.standardized_llm_call') as mock_llm:
        mock_llm.return_value = mock_llm_response
        result = await step_8_7_constraint_conflict_detection(
            constraints=constraints,
            categorical_values=None,
            derived_formulas=None,
            multivalued_derived=None,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["conflicts", "conflict_count", "resolution_applied"]
    )
    validations.append(struct_validation)
    
    # Check that conflicts were detected (deterministic detection should work)
    if hasattr(result, 'conflicts'):
        conflicts_list = result.conflicts
        conflict_count = result.conflict_count if hasattr(result, 'conflict_count') else len(conflicts_list)
        resolution_applied = result.resolution_applied if hasattr(result, 'resolution_applied') else False
        resolved_constraints = result.resolved_constraints if hasattr(result, 'resolved_constraints') else None
    elif isinstance(result, dict):
        conflicts_list = result.get("conflicts", [])
        conflict_count = result.get("conflict_count", len(conflicts_list))
        resolution_applied = result.get("resolution_applied", False)
        resolved_constraints = result.get("resolved_constraints")
    else:
        conflicts_list = []
        conflict_count = 0
        resolution_applied = False
        resolved_constraints = None
    
    # Verify conflicts were detected
    if len(conflicts_list) >= 2:
        validations.append({
            "name": "Conflicts detected",
            "valid": True,
            "message": f"Detected {len(conflicts_list)} conflicts as expected (should detect multiple constraints on same column)"
        })
    else:
        validations.append({
            "name": "Conflicts detected",
            "valid": False,
            "message": f"Expected at least 2 conflicts but detected {len(conflicts_list)}"
        })
    
    # Verify LLM was called for resolution
    if resolution_applied:
        validations.append({
            "name": "LLM resolution applied",
            "valid": True,
            "message": "LLM conflict resolution was successfully applied"
        })
        
        # Verify resolved constraints were returned
        if resolved_constraints and len(resolved_constraints) > 0:
            validations.append({
                "name": "Resolved constraints returned",
                "valid": True,
                "message": f"LLM returned {len(resolved_constraints)} resolved constraints"
            })
            
            # Verify resolved constraints have proper structure
            for i, resolved in enumerate(resolved_constraints):
                if isinstance(resolved, dict):
                    required_fields = ["constraint_type", "table", "column"]
                    missing = [f for f in required_fields if f not in resolved]
                    if not missing:
                        validations.append({
                            "name": f"Resolved constraint {i+1} structure",
                            "valid": True,
                            "message": f"Resolved constraint {i+1} has required fields"
                        })
                    else:
                        validations.append({
                            "name": f"Resolved constraint {i+1} structure",
                            "valid": False,
                            "message": f"Resolved constraint {i+1} missing fields: {missing}"
                        })
        else:
            validations.append({
                "name": "Resolved constraints returned",
                "valid": False,
                "message": "LLM resolution was applied but no resolved constraints were returned"
            })
    else:
        validations.append({
            "name": "LLM resolution applied",
            "valid": False,
            "message": "LLM conflict resolution was not applied (expected when conflicts are detected)"
        })
    
    # Validate conflict structure
    for i, conflict in enumerate(conflicts_list):
        if hasattr(conflict, 'constraint1_id') and hasattr(conflict, 'constraint2_id') and hasattr(conflict, 'conflict_type'):
            validations.append({
                "name": f"Conflict {i+1} structure",
                "valid": True,
                "message": f"Conflict {i+1} has required fields"
            })
        elif isinstance(conflict, dict):
            required_fields = ["constraint1_id", "constraint2_id", "conflict_type", "affected_column"]
            missing = [f for f in required_fields if f not in conflict]
            if not missing:
                validations.append({
                    "name": f"Conflict {i+1} structure",
                    "valid": True,
                    "message": f"Conflict {i+1} has required fields"
                })
            else:
                validations.append({
                    "name": f"Conflict {i+1} structure",
                    "valid": False,
                    "message": f"Conflict {i+1} missing fields: {missing}"
                })
        else:
            validations.append({
                "name": f"Conflict {i+1} structure",
                "valid": False,
                "message": f"Conflict {i+1} is not a valid structure"
            })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.7 (conflicts)")
    return all_valid


async def test_step_8_7_empty_constraints():
    """Test Step 8.7 with empty constraints list."""
    TestResultDisplay.print_test_header("Constraint Conflict Detection (Empty)", "8.7")
    TestResultDisplay.print_test_case(3, "Conflict detection with empty constraints")
    
    constraints = []
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = ConstraintConflictDetectionOutput(
        conflicts=[],
        conflict_count=0,
        resolved_constraints=None,
        resolution_applied=False,
    )
    
    with patch('NL2DATA.phases.phase8.step_8_7_constraint_conflict_detection.step_8_7_constraint_conflict_detection') as mock_step:
        mock_step.return_value = mock_response
        result = await step_8_7_constraint_conflict_detection(
            constraints=constraints,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["conflicts", "conflict_count", "resolution_applied"]
    )
    validations.append(struct_validation)
    
    # Check that no conflicts were detected
    if hasattr(result, 'conflict_count'):
        conflict_count = result.conflict_count
    elif isinstance(result, dict):
        conflict_count = result.get("conflict_count", 0)
    else:
        conflict_count = -1
    
    if conflict_count == 0:
        validations.append({
            "name": "Empty constraints check",
            "valid": True,
            "message": "Correctly returned 0 conflicts for empty constraints"
        })
    else:
        validations.append({
            "name": "Empty constraints check",
            "valid": False,
            "message": f"Expected 0 conflicts but got {conflict_count}"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.7 (empty)")
    return all_valid


async def main():
    """Run all tests for Step 8.7."""
    results = []
    results.append(await test_step_8_7_basic())
    results.append(await test_step_8_7_with_conflicts())
    results.append(await test_step_8_7_empty_constraints())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.7 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.7 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
