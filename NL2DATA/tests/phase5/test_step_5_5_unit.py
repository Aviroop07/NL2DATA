"""Unit tests for Step 5.5: Nullability Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5.step_5_5_nullability_detection import (
    step_5_5_nullability_detection,
    NullabilityOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_5_5_basic():
    """Test Step 5.5 with basic input."""
    TestResultDisplay.print_test_header("Nullability Detection", "5.5")
    TestResultDisplay.print_test_case(1, "Basic nullability detection")
    
    table_name = "Transaction"
    columns = [
        {"name": "transaction_id", "type": "BIGINT", "nullable": False},
        {"name": "card_id", "type": "BIGINT", "nullable": False},
        {"name": "merchant_id", "type": "BIGINT", "nullable": False},
        {"name": "transaction_datetime", "type": "TIMESTAMP", "nullable": False},
        {"name": "amount_usd", "type": "DECIMAL(12,2)", "nullable": False},
        {"name": "exchange_rate", "type": "DECIMAL(8,4)", "nullable": None},
        {"name": "fraud_risk_score", "type": "DECIMAL(5,2)", "nullable": None},
        {"name": "velocity_score", "type": "DECIMAL(5,2)", "nullable": None},
        {"name": "days_since_previous_txn", "type": "INTEGER", "nullable": None}
    ]
    primary_key = ["transaction_id"]
    foreign_keys = [
        {"from_attributes": ["card_id"], "to_entity": "Card", "to_attributes": ["card_id"]},
        {"from_attributes": ["merchant_id"], "to_entity": "Merchant", "to_attributes": ["merchant_id"]}
    ]
    nl_description = "Transactions require transaction_id, card_id, merchant_id, transaction_datetime, and amount_usd. Exchange_rate is required for cross-border transactions but optional for domestic. Fraud risk score and velocity score are optional derived attributes. Days since previous transaction is optional and may be null for first transaction per card."
    domain = "financial"
    input_data = {
        "table_name": table_name,
        "columns": columns,
        "primary_key": primary_key,
        "foreign_keys": foreign_keys,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = NullabilityOutput(
        nullable_columns=["exchange_rate", "fraud_risk_score", "velocity_score", "days_since_previous_txn"],
        not_nullable_columns=["transaction_id", "card_id", "merchant_id", "transaction_datetime", "amount_usd"],
        reasoning="Core transaction attributes (transaction_id, card_id, merchant_id, transaction_datetime, amount_usd) are required. Exchange_rate is conditionally required (required for cross-border, optional for domestic). Fraud and velocity scores are derived and optional. Days_since_previous_txn is null for first transaction per card."
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("5.5"):
            result = await step_5_5_nullability_detection(
                table_name=table_name,
                columns=columns,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["nullable_columns", "not_nullable_columns", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "nullable_columns": list,
            "not_nullable_columns": list,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "5.5")
    return all_valid


async def main():
    """Run all tests for Step 5.5."""
    results = []
    results.append(await test_step_5_5_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 5.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 5.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
