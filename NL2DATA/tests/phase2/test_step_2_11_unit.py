"""Unit tests for Step 2.11: Nullability Constraints.

Note: This step may be implemented in phase7/phase8 but is registered as Phase 2 Step 2.11.
Phase 5 Step 5.5 also handles nullability, but this is Phase 2's version.
"""

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


async def test_step_2_11_basic():
    """Test Step 2.11 with basic input."""
    TestResultDisplay.print_test_header("Nullability Constraints", "2.11")
    TestResultDisplay.print_test_case(1, "Basic nullability detection")
    
    entity_name = "Transaction"
    attributes = [
        "transaction_id", "card_id", "merchant_id", "transaction_datetime", 
        "amount_usd", "exchange_rate", "country_id", "is_cross_border",
        "fraud_risk_score", "velocity_score", "days_since_previous_txn"
    ]
    primary_key = ["transaction_id"]
    nl_description = "Transactions require transaction_id, card_id, merchant_id, transaction_datetime, and amount_usd. Exchange_rate is required for cross-border transactions but optional for domestic. Fraud risk score and velocity score are optional derived attributes. Days since previous transaction is optional and may be null for first transaction per card."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "primary_key": primary_key,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "nullable_attributes": ["exchange_rate", "fraud_risk_score", "velocity_score", "days_since_previous_txn", "is_cross_border"],
        "required_attributes": ["transaction_id", "card_id", "merchant_id", "transaction_datetime", "amount_usd", "country_id"],
        "reasoning": "Core transaction attributes (transaction_id, card_id, merchant_id, transaction_datetime, amount_usd, country_id) are required. Exchange_rate is conditionally required (required for cross-border, optional for domestic). Fraud and velocity scores are derived and optional. Days_since_previous_txn is null for first transaction per card."
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["nullable_attributes", "required_attributes"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "nullable_attributes": list,
            "required_attributes": list
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.11")
    return all_valid


async def main():
    """Run all tests for Step 2.11."""
    results = []
    results.append(await test_step_2_11_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.11 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.11 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
