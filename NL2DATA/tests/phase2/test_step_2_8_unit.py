"""Unit tests for Step 2.8: Multivalued/Derived Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_8_multivalued_derived_detection import (
    step_2_8_multivalued_derived_detection,
    MultivaluedDerivedOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_8_basic():
    """Test Step 2.8 with basic input."""
    TestResultDisplay.print_test_header("Multivalued/Derived Detection", "2.8")
    TestResultDisplay.print_test_case(1, "Basic multivalued/derived detection")
    
    entity_name = "Transaction"
    entity_description = "A financial transaction record with fraud detection features, velocity metrics, and derived risk scores"
    entity_attributes = [
        "transaction_id", "card_id", "merchant_id", "transaction_datetime", "amount_usd",
        "exchange_rate", "amount_local_currency", "is_cross_border", "days_since_previous_txn",
        "avg_amount_last_7d", "velocity_score", "high_risk_mcc_flag", "fraud_risk_score",
        "transaction_count_last_24h", "total_amount_last_24h"
    ]
    primary_key = ["transaction_id"]
    nl_description = "Transactions have base attributes (amount_usd, exchange_rate) and derived attributes (amount_local_currency = amount_usd * exchange_rate, is_cross_border flag, days_since_previous_txn, avg_amount_last_7d, velocity_score). Velocity features include transaction_count_last_24h and total_amount_last_24h calculated over rolling windows. Fraud risk score combines multiple signals."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "entity_description": entity_description,
        "entity_attributes": entity_attributes,
        "primary_key": primary_key,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = MultivaluedDerivedOutput(
        multivalued=[],
        derived=[
            "amount_local_currency",
            "is_cross_border",
            "days_since_previous_txn",
            "avg_amount_last_7d",
            "velocity_score",
            "fraud_risk_score"
        ],
        multivalued_handling={},
        reasoning="amount_local_currency is derived (amount_usd * exchange_rate), is_cross_border is derived boolean, days_since_previous_txn is temporal derived, avg_amount_last_7d is rolling window derived, velocity_score combines transaction_count_last_24h and total_amount_last_24h, fraud_risk_score combines multiple fraud signals"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.8"):
            result = await step_2_8_multivalued_derived_detection(
                entity_name=entity_name,
                entity_description=entity_description,
                entity_attributes=entity_attributes,
                primary_key=primary_key,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["multivalued", "derived", "multivalued_handling", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "multivalued": list,
            "derived": list,
            "multivalued_handling": dict,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.8")
    return all_valid


async def main():
    """Run all tests for Step 2.8."""
    results = []
    results.append(await test_step_2_8_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.8 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.8 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
