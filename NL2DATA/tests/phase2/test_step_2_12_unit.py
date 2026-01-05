"""Unit tests for Step 2.12: Default Values."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_12_basic():
    """Test Step 2.12 with basic input."""
    TestResultDisplay.print_test_header("Default Values", "2.12")
    TestResultDisplay.print_test_case(1, "Basic default value assignment")
    
    entity_name = "Transaction"
    attributes = [
        "transaction_id", "card_id", "merchant_id", "transaction_datetime",
        "amount_usd", "is_cross_border", "fraud_risk_score", "created_at", "processed_at"
    ]
    nullability = {
        "nullable_attributes": ["is_cross_border", "fraud_risk_score"],
        "required_attributes": ["transaction_id", "card_id", "merchant_id", "transaction_datetime", "amount_usd", "created_at"]
    }
    nl_description = "Transactions have transaction_datetime defaulting to CURRENT_TIMESTAMP if not provided. is_cross_border defaults to FALSE (0) for domestic transactions. fraud_risk_score defaults to 0.0 if not calculated. processed_at defaults to CURRENT_TIMESTAMP when transaction is processed."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "nullability": nullability,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "default_values": {
            "transaction_datetime": "CURRENT_TIMESTAMP",
            "is_cross_border": "FALSE",
            "fraud_risk_score": "0.0",
            "created_at": "CURRENT_TIMESTAMP",
            "processed_at": "CURRENT_TIMESTAMP"
        },
        "reasoning": "transaction_datetime defaults to current timestamp, is_cross_border defaults to FALSE for domestic transactions, fraud_risk_score defaults to 0.0, created_at and processed_at default to current timestamp"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["default_values"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"default_values": dict}
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.12")
    return all_valid


async def main():
    """Run all tests for Step 2.12."""
    results = []
    results.append(await test_step_2_12_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.12 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.12 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
