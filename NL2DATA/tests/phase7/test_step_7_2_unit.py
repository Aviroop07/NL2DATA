"""Unit tests for Step 7.2: SQL Generation and Validation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.step_7_2_sql_generation_and_validation import (
    step_7_2_sql_generation_and_validation,
    SQLGenerationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_7_2_basic():
    """Test Step 7.2 with basic input."""
    TestResultDisplay.print_test_header("SQL Generation and Validation", "7.2")
    TestResultDisplay.print_test_case(1, "Basic SQL generation and validation")
    
    information_need = {
        "description": "Find all customers",
        "entities_involved": ["Customer"]
    }
    relational_schema = {
        "tables": [
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
    nl_description = "I need to find all customers"
    domain = "e-commerce"
    input_data = {
        "information_need": information_need,
        "relational_schema": relational_schema,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = SQLGenerationOutput(
        sql_query="SELECT * FROM Customer",
        reasoning="Simple query to retrieve all customers"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("7.2"):
            result = await step_7_2_sql_generation_and_validation(
                information_need=information_need,
                relational_schema=relational_schema,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["sql_query", "is_valid"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "sql_query": str,
            "is_valid": bool
        }
    )
    validations.append(type_validation)
    
    sql_validation = {"valid": True, "errors": []}
    sql_query = result.get("sql_query", "")
    if not sql_query or not isinstance(sql_query, str):
        sql_validation["valid"] = False
        sql_validation["errors"].append("sql_query must be a non-empty string")
    elif "SELECT" not in sql_query.upper():
        sql_validation["valid"] = False
        sql_validation["errors"].append("sql_query should contain SELECT statement")
    validations.append(sql_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "7.2")
    return all_valid


async def main():
    """Run all tests for Step 7.2."""
    results = []
    results.append(await test_step_7_2_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 7.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 7.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
