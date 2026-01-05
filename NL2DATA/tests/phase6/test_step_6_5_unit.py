"""Unit tests for Step 6.5: SQL Query Generation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_5_sql_query_generation import (
    step_6_5_sql_query_generation,
    SQLQueryGenerationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_5_basic():
    """Test Step 6.5 with basic input."""
    TestResultDisplay.print_test_header("SQL Query Generation", "6.5")
    TestResultDisplay.print_test_case(1, "Basic SQL query generation")
    
    information_need = {
        "description": "Find all transactions with high fraud risk scores above 75.0, including card and merchant information",
        "entities_involved": ["Transaction", "Card", "Merchant"],
        "conditions": {"fraud_risk_score": "> 75.0"}
    }
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
        "information_need": information_need,
        "normalized_schema": normalized_schema
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = SQLQueryGenerationOutput(
        sql="SELECT t.transaction_id, t.transaction_datetime, t.amount_usd, t.fraud_risk_score, c.card_type, m.merchant_name FROM Transaction t INNER JOIN Card c ON t.card_id = c.card_id INNER JOIN Merchant m ON t.merchant_id = m.merchant_id WHERE t.fraud_risk_score > 75.0 ORDER BY t.fraud_risk_score DESC",
        validation_status="valid",
        corrected_sql=None,
        reasoning="Complex query joining Transaction, Card, and Merchant tables with fraud risk score filter (> 75.0) and ordering by risk score descending"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("6.5"):
            result = await step_6_5_sql_query_generation(
                information_need=information_need,
                normalized_schema=normalized_schema
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["sql", "validation_status", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "sql": str,
            "validation_status": str,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    sql_validation = {"valid": True, "errors": []}
    sql = result.get("sql", "")
    if not sql or not isinstance(sql, str):
        sql_validation["valid"] = False
        sql_validation["errors"].append("sql must be a non-empty string")
    elif "SELECT" not in sql.upper():
        sql_validation["valid"] = False
        sql_validation["errors"].append("sql should contain SELECT statement")
    validations.append(sql_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.5")
    return all_valid


async def main():
    """Run all tests for Step 6.5."""
    results = []
    results.append(await test_step_6_5_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
