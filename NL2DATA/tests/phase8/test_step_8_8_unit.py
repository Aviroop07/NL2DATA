"""Unit tests for Step 8.8: Constraint Compilation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_8_constraint_compilation import (
    step_8_8_constraint_compilation,
    ConstraintCompilationOutput,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_8_basic():
    """Test Step 8.8 with basic input."""
    TestResultDisplay.print_test_header("Constraint Compilation", "8.8")
    TestResultDisplay.print_test_case(1, "Basic constraint compilation")
    
    constraints = [
        {
            "constraint_id": "c1",
            "constraint_category": "statistical",
            "constraint_type": "statistical",
            "table": "Transaction",
            "column": "amount",
            "min_value": 0.01,
            "max_value": 1000000,
            "enforcement_strategy": "database",
            "enforcement_level": "strict",
            "column_dsl_expressions": {"Transaction.amount": "amount >= 0.01 AND amount <= 1000000"},
        },
        {
            "constraint_id": "c2",
            "constraint_category": "distribution",
            "constraint_type": "distribution",
            "table": "Transaction",
            "column": "amount",
            "distribution_type": "normal",
            "enforcement_strategy": "application",
            "enforcement_level": "warning",
        },
        {
            "constraint_id": "c3",
            "constraint_category": "other",
            "constraint_type": "other",
            "table": "Transaction",
            "description": "Custom business rule",
            "enforcement_strategy": "hybrid",
            "enforcement_level": "soft",
        }
    ]
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Call the real function (deterministic, no mocking needed)
    result = await step_8_8_constraint_compilation(
        constraints=constraints,
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    if hasattr(result, 'statistical_constraints'):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            ["statistical_constraints", "distribution_constraints", "other_constraints"]
        )
        validations.append(struct_validation)
        
        # Check that all constraints were categorized
        stat_count = len(result.statistical_constraints)
        dist_count = len(result.distribution_constraints)
        other_count = len(result.other_constraints)
        total_compiled = stat_count + dist_count + other_count
        
        if total_compiled == len(constraints):
            validations.append({
                "name": "Completeness check",
                "valid": True,
                "message": f"All {len(constraints)} constraints were compiled and categorized: {stat_count} statistical, {dist_count} distribution, {other_count} other"
            })
        else:
            validations.append({
                "name": "Completeness check",
                "valid": False,
                "message": f"Only {total_compiled}/{len(constraints)} constraints were compiled"
            })
    elif isinstance(result, dict):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            ["statistical_constraints", "distribution_constraints", "other_constraints"]
        )
        validations.append(struct_validation)
        
        stat_count = len(result.get("statistical_constraints", []))
        dist_count = len(result.get("distribution_constraints", []))
        other_count = len(result.get("other_constraints", []))
        total_compiled = stat_count + dist_count + other_count
        
        if total_compiled == len(constraints):
            validations.append({
                "name": "Completeness check",
                "valid": True,
                "message": f"All {len(constraints)} constraints were compiled and categorized"
            })
        else:
            validations.append({
                "name": "Completeness check",
                "valid": False,
                "message": f"Only {total_compiled}/{len(constraints)} constraints were compiled"
            })
    
    # Validate types
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "statistical_constraints": list,
            "distribution_constraints": list,
            "other_constraints": list,
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.8")
    return all_valid


async def test_step_8_8_categorization():
    """Test Step 8.8 constraint categorization."""
    TestResultDisplay.print_test_header("Constraint Compilation (Categorization)", "8.8")
    TestResultDisplay.print_test_case(2, "Test proper categorization of constraints")
    
    constraints = [
        {
            "constraint_id": "c1",
            "constraint_category": "statistical",
            "constraint_type": "statistical",
        },
        {
            "constraint_id": "c2",
            "constraint_category": "statistical",
            "constraint_type": "statistical",
        },
        {
            "constraint_id": "c3",
            "constraint_category": "distribution",
            "constraint_type": "distribution",
        },
        {
            "constraint_id": "c4",
            "constraint_category": "other",
            "constraint_type": "other",
        }
    ]
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Call the real function
    result = await step_8_8_constraint_compilation(
        constraints=constraints,
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Check categorization
    if hasattr(result, 'statistical_constraints'):
        stat_count = len(result.statistical_constraints)
        dist_count = len(result.distribution_constraints)
        other_count = len(result.other_constraints)
    elif isinstance(result, dict):
        stat_count = len(result.get("statistical_constraints", []))
        dist_count = len(result.get("distribution_constraints", []))
        other_count = len(result.get("other_constraints", []))
    else:
        stat_count = dist_count = other_count = 0
    
    if stat_count == 2 and dist_count == 1 and other_count == 1:
        validations.append({
            "name": "Categorization check",
            "valid": True,
            "message": f"Constraints correctly categorized: {stat_count} statistical, {dist_count} distribution, {other_count} other"
        })
    else:
        validations.append({
            "name": "Categorization check",
            "valid": False,
            "message": f"Expected 2/1/1 but got {stat_count}/{dist_count}/{other_count}"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.8 (categorization)")
    return all_valid


async def test_step_8_8_empty_constraints():
    """Test Step 8.8 with empty constraints list."""
    TestResultDisplay.print_test_header("Constraint Compilation (Empty)", "8.8")
    TestResultDisplay.print_test_case(3, "Compilation with empty constraints")
    
    constraints = []
    
    input_data = {
        "constraints": constraints,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Call the real function
    result = await step_8_8_constraint_compilation(
        constraints=constraints,
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["statistical_constraints", "distribution_constraints", "other_constraints"]
    )
    validations.append(struct_validation)
    
    # Check that all categories are empty
    if hasattr(result, 'statistical_constraints'):
        all_empty = (
            len(result.statistical_constraints) == 0 and
            len(result.distribution_constraints) == 0 and
            len(result.other_constraints) == 0
        )
    elif isinstance(result, dict):
        all_empty = (
            len(result.get("statistical_constraints", [])) == 0 and
            len(result.get("distribution_constraints", [])) == 0 and
            len(result.get("other_constraints", [])) == 0
        )
    else:
        all_empty = False
    
    if all_empty:
        validations.append({
            "name": "Empty constraints check",
            "valid": True,
            "message": "Correctly returned empty categories for empty constraints"
        })
    else:
        validations.append({
            "name": "Empty constraints check",
            "valid": False,
            "message": "Expected all categories to be empty"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.8 (empty)")
    return all_valid


async def main():
    """Run all tests for Step 8.8."""
    results = []
    results.append(await test_step_8_8_basic())
    results.append(await test_step_8_8_categorization())
    results.append(await test_step_8_8_empty_constraints())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.8 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.8 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
