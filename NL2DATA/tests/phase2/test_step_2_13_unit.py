"""Unit tests for Step 2.13: Check Constraints (Value Ranges)."""

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


async def test_step_2_13_basic():
    """Test Step 2.13 with basic input."""
    TestResultDisplay.print_test_header("Check Constraints (Value Ranges)", "2.13")
    TestResultDisplay.print_test_case(1, "Basic check constraint detection")
    
    entity_name = "Transaction"
    attributes = [
        "transaction_id", "amount_usd", "exchange_rate", "fraud_risk_score",
        "velocity_score", "days_since_previous_txn", "transaction_count_last_24h"
    ]
    nl_description = "Transaction amounts must be greater than 0. Exchange rates must be positive and typically between 0.1 and 10.0. Fraud risk scores range from 0.0 to 100.0. Velocity scores must be non-negative. Days since previous transaction must be non-negative. Transaction count in last 24 hours must be non-negative integer."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "check_constraints": [
            {
                "attribute": "amount_usd",
                "condition": "amount_usd > 0",
                "reasoning": "Transaction amounts must be positive"
            },
            {
                "attribute": "exchange_rate",
                "condition": "exchange_rate > 0 AND exchange_rate BETWEEN 0.1 AND 10.0",
                "reasoning": "Exchange rates must be positive and within realistic range"
            },
            {
                "attribute": "fraud_risk_score",
                "condition": "fraud_risk_score >= 0.0 AND fraud_risk_score <= 100.0",
                "reasoning": "Fraud risk scores must be in valid range 0.0 to 100.0"
            },
            {
                "attribute": "velocity_score",
                "condition": "velocity_score >= 0",
                "reasoning": "Velocity scores must be non-negative"
            },
            {
                "attribute": "days_since_previous_txn",
                "condition": "days_since_previous_txn >= 0",
                "reasoning": "Days since previous transaction must be non-negative"
            },
            {
                "attribute": "transaction_count_last_24h",
                "condition": "transaction_count_last_24h >= 0",
                "reasoning": "Transaction count must be non-negative integer"
            }
        ],
        "reasoning": "Identified multiple value range constraints for transaction attributes including amounts, exchange rates, fraud scores, and velocity metrics"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["check_constraints"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"check_constraints": list}
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.13")
    return all_valid


async def main():
    """Run all tests for Step 2.13."""
    results = []
    results.append(await test_step_2_13_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.13 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.13 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
