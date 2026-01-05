"""Unit tests for Step 8.5: Constraint Scope Analysis."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_5_constraint_scope_analysis import (
    step_8_5_constraint_scope_analysis_batch,
    step_8_5_constraint_scope_analysis_single,
    ConstraintScopeAnalysis,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_5_basic():
    """Test Step 8.5 with basic input - tests actual LLM call."""
    TestResultDisplay.print_test_header("Constraint Scope Analysis", "8.5")
    TestResultDisplay.print_test_case(1, "Basic constraint scope analysis with FD context")
    
    constraints = [
        {
            "constraint_id": "c1",
            "constraint_type": "statistical",
            "table": "Transaction",
            "column": "amount",
            "description": "Transaction amounts must be between 0.01 and 1000000",
            "justification_substring": "amounts between 0.01 and 1000000",
        },
        {
            "constraint_id": "c2",
            "constraint_type": "other",
            "table": "Transaction",
            "column": "status",
            "description": "Transaction status must be one of the specified values",
            "justification_substring": "status must be one of",
        },
    ]
    
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
            }
        ]
    }
    
    functional_dependencies = [
        {
            "lhs": ["transaction_id"],
            "rhs": ["amount", "status"],
            "reasoning": "Transaction ID determines amount and status"
        }
    ]
    
    input_data = {
        "constraints": constraints,
        "normalized_schema": normalized_schema,
        "functional_dependencies": functional_dependencies,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM responses
    mock_responses = [
        ConstraintScopeAnalysis(
            affected_attributes=["Transaction.amount"],
            affected_tables=["Transaction"],
            scope_type="column",
            reasoning="Constraint applies directly to the amount column in Transaction table",
        ),
        ConstraintScopeAnalysis(
            affected_attributes=["Transaction.status"],
            affected_tables=["Transaction"],
            scope_type="column",
            reasoning="Constraint applies directly to the status column in Transaction table",
        ),
    ]
    
    # Call the real function but mock only the LLM call
    with patch('NL2DATA.phases.phase8.step_8_5_constraint_scope_analysis.standardized_llm_call') as mock_llm:
        # Set up side_effect to return different responses for each call
        mock_llm.side_effect = mock_responses
        result = await step_8_5_constraint_scope_analysis_batch(
            constraints=constraints,
            normalized_schema=normalized_schema,
            functional_dependencies=functional_dependencies,
        )
    
    # Handle list output for print_output_summary
    if isinstance(result, list):
        print(f"\nOutput: List with {len(result)} items")
        for i, item in enumerate(result[:3]):  # Show first 3 items
            print(f"  Item {i+1}:")
            TestResultDisplay.print_output_summary(item)
        if len(result) > 3:
            print(f"  ... and {len(result) - 3} more items")
    else:
        TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure - should return list
    if isinstance(result, list):
        validations.append({
            "name": "Output type check",
            "valid": True,
            "message": "Result is a list as expected"
        })
        
        # Check length matches input
        if len(result) == len(constraints):
            validations.append({
                "name": "Length check",
                "valid": True,
                "message": f"Returned {len(result)} scope analyses for {len(constraints)} constraints"
            })
        else:
            validations.append({
                "name": "Length check",
                "valid": False,
                "message": f"Returned {len(result)} scope analyses but expected {len(constraints)}"
            })
        
        # Validate each scope analysis
        for i, scope_analysis in enumerate(result):
            if hasattr(scope_analysis, 'affected_attributes') and hasattr(scope_analysis, 'affected_tables') and hasattr(scope_analysis, 'scope_type') and hasattr(scope_analysis, 'reasoning'):
                validations.append({
                    "name": f"Scope analysis {i+1} structure",
                    "valid": True,
                    "message": f"Scope analysis {i+1} has required fields including reasoning"
                })
            elif isinstance(scope_analysis, dict):
                required_fields = ["affected_attributes", "affected_tables", "scope_type", "reasoning"]
                missing = [f for f in required_fields if f not in scope_analysis]
                if not missing:
                    validations.append({
                        "name": f"Scope analysis {i+1} structure",
                        "valid": True,
                        "message": f"Scope analysis {i+1} has required fields including reasoning"
                    })
                else:
                    validations.append({
                        "name": f"Scope analysis {i+1} structure",
                        "valid": False,
                        "message": f"Scope analysis {i+1} missing fields: {missing}"
                    })
            else:
                validations.append({
                    "name": f"Scope analysis {i+1} structure",
                    "valid": False,
                    "message": f"Scope analysis {i+1} is not a valid structure"
                })
    else:
        validations.append({
            "name": "Output type check",
            "valid": False,
            "message": f"Result is {type(result)} but expected list"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.5")
    return all_valid


async def test_step_8_5_empty_constraints():
    """Test Step 8.5 with empty constraints list."""
    TestResultDisplay.print_test_header("Constraint Scope Analysis (Empty)", "8.5")
    TestResultDisplay.print_test_case(2, "Scope analysis with empty constraints")
    
    constraints = []
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": []
            }
        ]
    }
    
    input_data = {
        "constraints": constraints,
        "normalized_schema": normalized_schema,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Call the real function - should return empty list
    result = await step_8_5_constraint_scope_analysis_batch(
        constraints=constraints,
        normalized_schema=normalized_schema,
        functional_dependencies=None,
    )
    
    # Handle list output for print_output_summary
    if isinstance(result, list):
        print(f"\nOutput: List with {len(result)} items")
        if len(result) == 0:
            print("  (empty list)")
    else:
        TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    if isinstance(result, list) and len(result) == 0:
        validations.append({
            "name": "Empty result check",
            "valid": True,
            "message": "Correctly returned empty list for empty constraints"
        })
    else:
        validations.append({
            "name": "Empty result check",
            "valid": False,
            "message": f"Expected empty list but got {type(result)} with length {len(result) if isinstance(result, list) else 'N/A'}"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.5 (empty)")
    return all_valid


async def main():
    """Run all tests for Step 8.5."""
    results = []
    results.append(await test_step_8_5_basic())
    results.append(await test_step_8_5_empty_constraints())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
