"""Unit tests for Step 6.2: DDL Validation.

This is a deterministic step that validates DDL statements for syntax errors.
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_2_ddl_validation import (
    step_6_2_ddl_validation,
    DDLValidationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_2_basic():
    """Test Step 6.2 with valid DDL statements."""
    TestResultDisplay.print_test_header("DDL Validation", "6.2")
    TestResultDisplay.print_test_case(1, "Basic DDL validation with valid statements")
    
    ddl_statements = [
        "CREATE TABLE Transaction (\n"
        "    transaction_id BIGINT NOT NULL,\n"
        "    card_id BIGINT NOT NULL,\n"
        "    merchant_id BIGINT NOT NULL,\n"
        "    transaction_datetime TIMESTAMP NOT NULL,\n"
        "    amount_usd DECIMAL(12,2) NOT NULL,\n"
        "    exchange_rate DECIMAL(8,4) NULL,\n"
        "    fraud_risk_score DECIMAL(5,2) NULL,\n"
        "    PRIMARY KEY (transaction_id)\n"
        ");",
        "CREATE TABLE Card (\n"
        "    card_id BIGINT NOT NULL,\n"
        "    card_type VARCHAR(50) NULL,\n"
        "    PRIMARY KEY (card_id)\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # This is a deterministic step, no LLM mocking needed
    result = step_6_2_ddl_validation(
        ddl_statements=ddl_statements
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["validation_passed", "syntax_errors", "naming_conflicts", "warnings"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "validation_passed": bool,
            "syntax_errors": list,
            "naming_conflicts": list,
            "warnings": list
        }
    )
    validations.append(type_validation)
    
    # Validate that validation passed for valid DDL
    validation_result = {"valid": True, "errors": []}
    if not result.get("validation_passed", False):
        validation_result["valid"] = False
        validation_result["errors"].append("Validation should pass for valid DDL statements")
    if len(result.get("syntax_errors", [])) > 0:
        validation_result["valid"] = False
        validation_result["errors"].append(f"Should have no syntax errors, got {len(result.get('syntax_errors', []))}")
    validations.append(validation_result)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.2")
    return all_valid


async def test_step_6_2_with_errors():
    """Test Step 6.2 with invalid DDL statements."""
    TestResultDisplay.print_test_header("DDL Validation", "6.2")
    TestResultDisplay.print_test_case(2, "DDL validation with syntax errors")
    
    ddl_statements = [
        "CREATE TABLE Transaction (\n"
        "    transaction_id BIGINT NOT NULL,\n"
        "    card_id BIGINT NOT NULL\n"  # Missing comma and closing
        ";",  # Invalid syntax
        "CREATE TABLE Card (\n"
        "    card_id BIGINT NOT NULL,\n"
        "    PRIMARY KEY (card_id)\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_6_2_ddl_validation(
        ddl_statements=ddl_statements
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["validation_passed", "syntax_errors", "naming_conflicts", "warnings"]
    )
    validations.append(struct_validation)
    
    # Validate that validation failed for invalid DDL
    validation_result = {"valid": True, "errors": []}
    if result.get("validation_passed", True):
        validation_result["valid"] = False
        validation_result["errors"].append("Validation should fail for invalid DDL statements")
    if len(result.get("syntax_errors", [])) == 0:
        validation_result["valid"] = False
        validation_result["errors"].append("Should have syntax errors for invalid DDL")
    validations.append(validation_result)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.2")
    return all_valid


async def test_step_6_2_with_naming_conflicts():
    """Test Step 6.2 with duplicate table names."""
    TestResultDisplay.print_test_header("DDL Validation", "6.2")
    TestResultDisplay.print_test_case(3, "DDL validation with naming conflicts")
    
    ddl_statements = [
        "CREATE TABLE Transaction (\n"
        "    transaction_id BIGINT NOT NULL,\n"
        "    PRIMARY KEY (transaction_id)\n"
        ");",
        "CREATE TABLE Transaction (\n"  # Duplicate table name
        "    card_id BIGINT NOT NULL,\n"
        "    PRIMARY KEY (card_id)\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_6_2_ddl_validation(
        ddl_statements=ddl_statements
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    # Validate that naming conflicts are detected
    conflict_validation = {"valid": True, "errors": []}
    naming_conflicts = result.get("naming_conflicts", [])
    if len(naming_conflicts) == 0:
        conflict_validation["valid"] = False
        conflict_validation["errors"].append("Should detect naming conflicts for duplicate table names")
    validations.append(conflict_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.2")
    return all_valid


async def main():
    """Run all tests for Step 6.2."""
    results = []
    results.append(await test_step_6_2_basic())
    results.append(await test_step_6_2_with_errors())
    results.append(await test_step_6_2_with_naming_conflicts())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
