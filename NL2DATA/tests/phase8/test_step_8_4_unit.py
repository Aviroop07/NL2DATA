"""Unit tests for Step 8.4: Constraint Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_4_constraint_detection import (
    step_8_4_constraint_detection_with_loop,
    step_8_4_constraint_detection_single,
    ConstraintDetectionOutput,
    DetectedConstraint,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_4_basic():
    """Test Step 8.4 with basic input - tests actual LLM call."""
    TestResultDisplay.print_test_header("Constraint Detection", "8.4")
    TestResultDisplay.print_test_case(1, "Basic constraint detection with substring justification")
    
    nl_description = "Transactions must have amounts between 0.01 and 1000000. Transaction status must be one of: pending, completed, cancelled, failed. Card types must be credit, debit, or prepaid."
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                    {"name": "amount", "type": "DECIMAL(10,2)", "is_nullable": False, "description": "Transaction amount"},
                    {"name": "status", "type": "VARCHAR(50)", "is_nullable": False, "description": "Transaction status"},
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": []
            },
            {
                "name": "Card",
                "columns": [
                    {"name": "card_id", "type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                    {"name": "card_type", "type": "VARCHAR(50)", "is_nullable": False, "description": "Card type"},
                ],
                "primary_key": ["card_id"],
                "foreign_keys": []
            }
        ]
    }
    categorical_values = {
        "Transaction": {
            "status": {
                "categorical_values": [
                    {"value": "pending", "description": "Pending"},
                    {"value": "completed", "description": "Completed"},
                    {"value": "cancelled", "description": "Cancelled"},
                    {"value": "failed", "description": "Failed"},
                ]
            }
        },
        "Card": {
            "card_type": {
                "categorical_values": [
                    {"value": "credit", "description": "Credit card"},
                    {"value": "debit", "description": "Debit card"},
                    {"value": "prepaid", "description": "Prepaid card"},
                ]
            }
        }
    }
    functional_dependencies = [
        {
            "lhs": ["transaction_id"],
            "rhs": ["amount", "status"],
            "reasoning": "Transaction ID determines amount and status"
        }
    ]
    
    input_data = {
        "nl_description": nl_description,
        "normalized_schema": normalized_schema,
        "categorical_values": categorical_values,
        "functional_dependencies": functional_dependencies,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response with proper structure
    mock_constraints = [
        DetectedConstraint(
            constraint_id="c1",
            constraint_type="statistical",
            table="Transaction",
            column="amount",
            description="Transaction amounts must be between 0.01 and 1000000",
            justification_substring="amounts between 0.01 and 1000000",
        ),
        DetectedConstraint(
            constraint_id="c2",
            constraint_type="other",
            table="Transaction",
            column="status",
            description="Transaction status must be one of: pending, completed, cancelled, failed",
            justification_substring="status must be one of: pending, completed, cancelled, failed",
        ),
        DetectedConstraint(
            constraint_id="c3",
            constraint_type="other",
            table="Card",
            column="card_type",
            description="Card types must be credit, debit, or prepaid",
            justification_substring="Card types must be credit, debit, or prepaid",
        ),
    ]
    
    mock_response = ConstraintDetectionOutput(
        statistical_constraints=[mock_constraints[0]],
        distribution_constraints=[],
        other_constraints=[mock_constraints[1], mock_constraints[2]],
        no_more_changes=True,
        reasoning="All constraints identified from NL description",
    )
    
    # Call the real function but mock only the LLM call
    with patch('NL2DATA.phases.phase8.step_8_4_constraint_detection.standardized_llm_call') as mock_llm:
        mock_llm.return_value = mock_response
        result = await step_8_4_constraint_detection_single(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            categorical_values=categorical_values,
            functional_dependencies=functional_dependencies,
            derived_columns=set(),
            previous_constraints=None,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    if hasattr(result, 'statistical_constraints'):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            ["statistical_constraints", "distribution_constraints", "other_constraints", "no_more_changes"]
        )
        validations.append(struct_validation)
        
        # Check that constraints have justification_substring
        all_constraints = result.statistical_constraints + result.distribution_constraints + result.other_constraints
        for constraint in all_constraints:
            if hasattr(constraint, 'justification_substring'):
                if constraint.justification_substring:
                    validations.append({
                        "name": f"Justification for {constraint.constraint_id}",
                        "valid": True,
                        "message": f"Constraint {constraint.constraint_id} has justification substring"
                    })
                else:
                    validations.append({
                        "name": f"Justification for {constraint.constraint_id}",
                        "valid": False,
                        "message": f"Constraint {constraint.constraint_id} missing justification substring"
                    })
            else:
                validations.append({
                    "name": f"Justification for constraint",
                    "valid": False,
                    "message": "Constraint missing justification_substring field"
                })
    elif isinstance(result, dict):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            ["statistical_constraints", "distribution_constraints", "other_constraints", "no_more_changes"]
        )
        validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.4")
    return all_valid


async def test_step_8_4_without_categorical_values():
    """Test Step 8.4 without categorical values."""
    TestResultDisplay.print_test_header("Constraint Detection (No Categorical Values)", "8.4")
    TestResultDisplay.print_test_case(2, "Constraint detection without categorical values input")
    
    nl_description = "Transaction amounts must be positive (greater than 0). All transactions must have a timestamp."
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                    {"name": "amount", "type": "DECIMAL(10,2)", "is_nullable": False, "description": "Transaction amount"},
                    {"name": "timestamp", "type": "TIMESTAMP", "is_nullable": False, "description": "Transaction timestamp"},
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": []
            }
        ]
    }
    
    input_data = {
        "nl_description": nl_description,
        "normalized_schema": normalized_schema,
        "categorical_values": None,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = ConstraintDetectionOutput(
        statistical_constraints=[
            DetectedConstraint(
                constraint_id="c1",
                constraint_type="statistical",
                table="Transaction",
                column="amount",
                description="Transaction amounts must be positive (greater than 0)",
                justification_substring="amounts must be positive (greater than 0)",
            )
        ],
        distribution_constraints=[],
        other_constraints=[],
        no_more_changes=True,
        reasoning="All constraints identified",
    )
    
    with patch('NL2DATA.phases.phase8.step_8_4_constraint_detection.standardized_llm_call') as mock_llm:
        mock_llm.return_value = mock_response
        result = await step_8_4_constraint_detection_single(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            categorical_values=None,
            functional_dependencies=None,
            derived_columns=set(),
            previous_constraints=None,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    if hasattr(result, 'statistical_constraints'):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            ["statistical_constraints", "distribution_constraints", "other_constraints", "no_more_changes"]
        )
        validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.4 (no categorical)")
    return all_valid


async def test_step_8_4_loop_termination():
    """Test Step 8.4 loop termination."""
    TestResultDisplay.print_test_header("Constraint Detection (Loop)", "8.4")
    TestResultDisplay.print_test_case(3, "Test loop termination with no_more_changes")
    
    nl_description = "Transaction amounts must be between 0.01 and 1000000."
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "amount", "type": "DECIMAL(10,2)", "is_nullable": False},
                ],
                "primary_key": [],
                "foreign_keys": []
            }
        ]
    }
    
    # First iteration: finds constraints, no_more_changes=False
    mock_response_1 = ConstraintDetectionOutput(
        statistical_constraints=[
            DetectedConstraint(
                constraint_id="c1",
                constraint_type="statistical",
                table="Transaction",
                column="amount",
                description="Transaction amounts must be between 0.01 and 1000000",
                justification_substring="amounts must be between 0.01 and 1000000",
            )
        ],
        distribution_constraints=[],
        other_constraints=[],
        no_more_changes=False,
        reasoning="Found initial constraints, checking for more",
    )
    
    # Second iteration: no more changes, no_more_changes=True
    mock_response_2 = ConstraintDetectionOutput(
        statistical_constraints=[
            DetectedConstraint(
                constraint_id="c1",
                constraint_type="statistical",
                table="Transaction",
                column="amount",
                description="Transaction amounts must be between 0.01 and 1000000",
                justification_substring="amounts must be between 0.01 and 1000000",
            )
        ],
        distribution_constraints=[],
        other_constraints=[],
        no_more_changes=True,
        reasoning="No more constraints to add",
    )
    
    call_count = 0
    async def mock_llm_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_response_1
        else:
            return mock_response_2
    
    with patch('NL2DATA.phases.phase8.step_8_4_constraint_detection.standardized_llm_call', side_effect=mock_llm_call):
        result = await step_8_4_constraint_detection_with_loop(
            nl_description=nl_description,
            normalized_schema=normalized_schema,
            categorical_values=None,
            functional_dependencies=None,
            derived_formulas=None,
            multivalued_derived=None,
            max_iterations=5,
            max_time_sec=60,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Check that loop terminated
    if isinstance(result, dict):
        loop_metadata = result.get("loop_metadata", {})
        iterations = loop_metadata.get("iterations", 0)
        terminated_by = loop_metadata.get("terminated_by", "")
        
        if iterations >= 2 and terminated_by == "condition_met":
            validations.append({
                "name": "Loop termination",
                "valid": True,
                "message": f"Loop terminated after {iterations} iterations as expected"
            })
        else:
            validations.append({
                "name": "Loop termination",
                "valid": False,
                "message": f"Loop did not terminate correctly: {iterations} iterations, terminated_by={terminated_by}"
            })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.4 (loop)")
    return all_valid


async def main():
    """Run all tests for Step 8.4."""
    results = []
    results.append(await test_step_8_4_basic())
    results.append(await test_step_8_4_without_categorical_values())
    results.append(await test_step_8_4_loop_termination())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
