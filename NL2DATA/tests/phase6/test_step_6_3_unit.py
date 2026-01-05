"""Unit tests for Step 6.3: Schema Creation.

This is a deterministic step that executes DDL statements to create database schema.
"""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_3_schema_creation import (
    step_6_3_schema_creation,
    SchemaCreationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_3_basic():
    """Test Step 6.3 with valid DDL statements."""
    TestResultDisplay.print_test_header("Schema Creation", "6.3")
    TestResultDisplay.print_test_case(1, "Basic schema creation with valid DDL")
    
    ddl_statements = [
        "CREATE TABLE \"Transaction\" (\n"
        "    \"transaction_id\" BIGINT NOT NULL,\n"
        "    \"card_id\" BIGINT NOT NULL,\n"
        "    \"merchant_id\" BIGINT NOT NULL,\n"
        "    \"transaction_datetime\" TIMESTAMP NOT NULL,\n"
        "    \"amount_usd\" DECIMAL(12,2) NOT NULL,\n"
        "    \"exchange_rate\" DECIMAL(8,4) NULL,\n"
        "    \"fraud_risk_score\" DECIMAL(5,2) NULL,\n"
        "    PRIMARY KEY (\"transaction_id\")\n"
        ");",
        "CREATE TABLE \"Card\" (\n"
        "    \"card_id\" BIGINT NOT NULL,\n"
        "    \"card_type\" VARCHAR(50) NULL,\n"
        "    PRIMARY KEY (\"card_id\")\n"
        ");",
        "CREATE TABLE \"Merchant\" (\n"
        "    \"merchant_id\" BIGINT NOT NULL,\n"
        "    \"merchant_name\" VARCHAR(255) NULL,\n"
        "    PRIMARY KEY (\"merchant_id\")\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # This is a deterministic step, no LLM mocking needed
    # Use in-memory database (database_path=None)
    result = step_6_3_schema_creation(
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
    if not result.success:
        success_validation["valid"] = False
        success_validation["errors"].append("Schema creation should succeed for valid DDL")
    if len(result.errors) > 0:
        success_validation["valid"] = False
        success_validation["errors"].append(f"Should have no errors, got {result.errors}")
    validations.append(success_validation)
    
    # Validate that tables were created
    tables_validation = {"valid": True, "errors": []}
    tables_created = result.tables_created
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
    
    TestResultDisplay.print_test_summary(all_valid, "6.3")
    return all_valid


async def test_step_6_3_with_errors():
    """Test Step 6.3 with invalid DDL statements."""
    TestResultDisplay.print_test_header("Schema Creation", "6.3")
    TestResultDisplay.print_test_case(2, "Schema creation with invalid DDL")
    
    ddl_statements = [
        "CREATE TABLE \"Transaction\" (\n"
        "    \"transaction_id\" BIGINT NOT NULL,\n"
        "    \"card_id\" BIGINT NOT NULL\n"  # Missing comma and closing
        ";",  # Invalid syntax
        "CREATE TABLE \"Card\" (\n"
        "    \"card_id\" BIGINT NOT NULL,\n"
        "    PRIMARY KEY (\"card_id\")\n"
        ");"
    ]
    input_data = {
        "ddl_statements": ddl_statements
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_6_3_schema_creation(
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
    errors = result.errors
    if len(errors) == 0:
        error_validation["valid"] = False
        error_validation["errors"].append("Should have errors for invalid DDL statements")
    validations.append(error_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.3")
    return all_valid


async def test_step_6_3_with_file_database():
    """Test Step 6.3 with file-based database."""
    TestResultDisplay.print_test_header("Schema Creation", "6.3")
    TestResultDisplay.print_test_case(3, "Schema creation with file-based database")
    
    import tempfile
    import os
    from pathlib import Path
    
    ddl_statements = [
        "CREATE TABLE \"Customer\" (\n"
        "    \"customer_id\" INTEGER NOT NULL,\n"
        "    \"name\" VARCHAR(255) NOT NULL,\n"
        "    PRIMARY KEY (\"customer_id\")\n"
        ");"
    ]
    
    # Create temporary database file
    temp_dir = tempfile.mkdtemp()
    database_path = str(Path(temp_dir) / "test_schema.db")
    
    result = step_6_3_schema_creation(
        ddl_statements=ddl_statements,
        database_path=database_path
    )
    
    # Verify database file was created
    file_exists = os.path.exists(database_path)
    
    # Verify database can be opened and table exists
    import sqlite3
    table_exists = False
    if file_exists:
        try:
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Customer';")
            table_exists = cursor.fetchone() is not None
            conn.close()
        except Exception as e:
            print(f"Error checking database: {e}")
    
    # Cleanup
    try:
        if os.path.exists(database_path):
            os.remove(database_path)
        os.rmdir(temp_dir)
    except:
        pass
    
    success = result.success and file_exists and table_exists
    
    if success:
        print("\n[PASS] File-based database creation works correctly")
    else:
        print(f"\n[FAIL] File-based database creation failed: success={result.success}, file_exists={file_exists}, table_exists={table_exists}")
    
    return success


async def main():
    """Run all tests for Step 6.3."""
    results = []
    results.append(await test_step_6_3_basic())
    results.append(await test_step_6_3_with_errors())
    results.append(await test_step_6_3_with_file_database())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.3 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.3 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
