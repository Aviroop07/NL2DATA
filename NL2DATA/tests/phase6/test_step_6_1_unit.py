"""Unit tests for Step 6.1: DDL Compilation.

This is a deterministic step that converts normalized schema to SQL DDL statements.
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_1_ddl_compilation import (
    step_6_1_ddl_compilation,
    DDLCompilationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_1_basic():
    """Test Step 6.1 with basic input."""
    TestResultDisplay.print_test_header("DDL Compilation", "6.1")
    TestResultDisplay.print_test_case(1, "Basic DDL compilation")
    
    normalized_schema = {
        "normalized_tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "nullable": False},
                    {"name": "card_id", "type": "BIGINT", "nullable": False},
                    {"name": "merchant_id", "type": "BIGINT", "nullable": False},
                    {"name": "transaction_datetime", "type": "TIMESTAMP", "nullable": False},
                    {"name": "amount_usd", "type": "DECIMAL(12,2)", "nullable": False},
                    {"name": "exchange_rate", "type": "DECIMAL(8,4)", "nullable": True},
                    {"name": "fraud_risk_score", "type": "DECIMAL(5,2)", "nullable": True}
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": [
                    {"from_attributes": ["card_id"], "to_entity": "Card", "to_attributes": ["card_id"]},
                    {"from_attributes": ["merchant_id"], "to_entity": "Merchant", "to_attributes": ["merchant_id"]}
                ]
            },
            {
                "name": "Card",
                "columns": [
                    {"name": "card_id", "type": "BIGINT", "nullable": False},
                    {"name": "card_type", "type": "VARCHAR(50)", "nullable": True}
                ],
                "primary_key": ["card_id"]
            },
            {
                "name": "Merchant",
                "columns": [
                    {"name": "merchant_id", "type": "BIGINT", "nullable": False},
                    {"name": "merchant_name", "type": "VARCHAR(255)", "nullable": True}
                ],
                "primary_key": ["merchant_id"]
            }
        ]
    }
    input_data = {
        "normalized_schema": normalized_schema
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # This is a deterministic step, no LLM mocking needed
    result = step_6_1_ddl_compilation(
        normalized_schema=normalized_schema
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["ddl_statements"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "ddl_statements": list
        }
    )
    validations.append(type_validation)
    
    # Validate DDL statements
    ddl_validation = {"valid": True, "errors": []}
    ddl_statements = result.get("ddl_statements", [])
    if not ddl_statements or len(ddl_statements) == 0:
        ddl_validation["valid"] = False
        ddl_validation["errors"].append("ddl_statements must be a non-empty list")
    else:
        # Check that each statement contains CREATE TABLE
        for i, ddl in enumerate(ddl_statements):
            if not isinstance(ddl, str):
                ddl_validation["valid"] = False
                ddl_validation["errors"].append(f"DDL statement {i+1} must be a string")
            elif "CREATE TABLE" not in ddl.upper():
                ddl_validation["valid"] = False
                ddl_validation["errors"].append(f"DDL statement {i+1} must contain CREATE TABLE")
    
    validations.append(ddl_validation)
    
    # Validate that we have DDL for all tables
    table_count_validation = {"valid": True, "errors": []}
    expected_tables = len(normalized_schema.get("normalized_tables", []))
    if len(ddl_statements) != expected_tables:
        table_count_validation["valid"] = False
        table_count_validation["errors"].append(
            f"Expected {expected_tables} DDL statements, got {len(ddl_statements)}"
        )
    validations.append(table_count_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.1")
    return all_valid


async def test_step_6_1_with_data_types():
    """Test Step 6.1 with data types provided."""
    TestResultDisplay.print_test_header("DDL Compilation", "6.1")
    TestResultDisplay.print_test_case(2, "DDL compilation with data types")
    
    normalized_schema = {
        "normalized_tables": [
            {
                "name": "Customer",
                "columns": [
                    {"name": "customer_id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR(255)", "nullable": False}
                ],
                "primary_key": ["customer_id"]
            }
        ]
    }
    data_types = {
        "Customer": {
            "customer_id": {"sql_type": "INTEGER", "precision": None, "scale": None},
            "name": {"sql_type": "VARCHAR", "precision": 255, "scale": None}
        }
    }
    input_data = {
        "normalized_schema": normalized_schema,
        "data_types": data_types
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_6_1_ddl_compilation(
        normalized_schema=normalized_schema,
        data_types=data_types
    )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["ddl_statements"]
    )
    validations.append(struct_validation)
    
    ddl_statements = result.get("ddl_statements", [])
    if ddl_statements:
        # Check that the DDL includes the correct types
        ddl_str = ddl_statements[0].upper()
        type_validation = {"valid": True, "errors": []}
        if "INTEGER" not in ddl_str and "BIGINT" not in ddl_str:
            type_validation["valid"] = False
            type_validation["errors"].append("DDL should include INTEGER or BIGINT for customer_id")
        if "VARCHAR" not in ddl_str:
            type_validation["valid"] = False
            type_validation["errors"].append("DDL should include VARCHAR for name")
        validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.1")
    return all_valid


async def main():
    """Run all tests for Step 6.1."""
    results = []
    results.append(await test_step_6_1_basic())
    results.append(await test_step_6_1_with_data_types())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
