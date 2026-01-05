"""Unit tests for Step 6.4: Schema Creation.

This is a deterministic step that executes DDL statements to create database schema.
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_4_schema_creation import (
    step_6_4_schema_creation,
    SchemaCreationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_4_basic():
    """Test Step 6.4 with valid DDL statements."""
    TestResultDisplay.print_test_header("Schema Creation", "6.4")
    TestResultDisplay.print_test_case(1, "Basic schema creation with valid DDL")
    
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
        ");",
        "CREATE TABLE Merchant (\n"
        "    merchant_id BIGINT NOT NULL,\n"
        "    merchant_name VARCHAR(255) NULL,\n"
        "    PRIMARY KEY (merchant_id)\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # This is a deterministic step, no LLM mocking needed
    # Use in-memory database (database_path=None)
    result = step_6_4_schema_creation(
        ddl_statements=ddl_statements,
        database_path=None  # In-memory for testing
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["success", "errors", "tables_created"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "success": bool,
            "errors": list,
            "tables_created": list
        }
    )
    validations.append(type_validation)
    
    # Validate that schema creation succeeded
    success_validation = {"valid": True, "errors": []}
    if not result.get("success", False):
        success_validation["valid"] = False
        success_validation["errors"].append("Schema creation should succeed for valid DDL")
    if len(result.get("errors", [])) > 0:
        success_validation["valid"] = False
        success_validation["errors"].append(f"Should have no errors, got {result.get('errors', [])}")
    validations.append(success_validation)
    
    # Validate that tables were created
    tables_validation = {"valid": True, "errors": []}
    tables_created = result.get("tables_created", [])
    expected_tables = ["Transaction", "Card", "Merchant"]
    for expected_table in expected_tables:
        # Check if table name appears in any created table (case-insensitive)
        found = any(expected_table.lower() in table.lower() for table in tables_created)
        if not found:
            tables_validation["valid"] = False
            tables_validation["errors"].append(f"Expected table '{expected_table}' to be created")
    validations.append(tables_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.4")
    return all_valid


async def test_step_6_4_with_errors():
    """Test Step 6.4 with invalid DDL statements."""
    TestResultDisplay.print_test_header("Schema Creation", "6.4")
    TestResultDisplay.print_test_case(2, "Schema creation with invalid DDL")
    
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
    
    result = step_6_4_schema_creation(
        ddl_statements=ddl_statements,
        database_path=None  # In-memory for testing
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["success", "errors", "tables_created"]
    )
    validations.append(struct_validation)
    
    # Validate that schema creation failed for invalid DDL
    error_validation = {"valid": True, "errors": []}
    errors = result.get("errors", [])
    if len(errors) == 0:
        error_validation["valid"] = False
        error_validation["errors"].append("Should have errors for invalid DDL statements")
    validations.append(error_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.4")
    return all_valid


async def main():
    """Run all tests for Step 6.4."""
    results = []
    results.append(await test_step_6_4_basic())
    results.append(await test_step_6_4_with_errors())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
