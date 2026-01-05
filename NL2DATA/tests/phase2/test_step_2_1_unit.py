"""Unit tests for Step 2.1: Attribute Count Detection.

These tests provide proper input structure, mock LLM responses,
perform deterministic validations, and display outputs.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_1_attribute_count_detection import (
    step_2_1_attribute_count_detection,
    AttributeCountOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_1_with_explicit_count():
    """Test Step 2.1 with explicit attribute count."""
    TestResultDisplay.print_test_header("Attribute Count Detection", "2.1")
    TestResultDisplay.print_test_case(1, "Explicit attribute count")
    
    # Input - Complex Transaction entity
    entity_name = "Transaction"
    nl_description = "Transaction fact table contains transaction_id, card_id, merchant_id, transaction_datetime, amount_usd, exchange_rate, country_id, card_home_country_id, risk_profile_id, is_chip_transaction, is_cross_border, days_since_previous_txn, avg_amount_last_7d, velocity_score, high_risk_mcc_flag, and fraud_risk_score"
    entity_description = "A financial transaction record with fraud detection features and temporal attributes"
    input_data = {
        "entity_name": entity_name,
        "nl_description": nl_description,
        "entity_description": entity_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = AttributeCountOutput(
        has_explicit_count=True,
        count=16,
        explicit_attributes=["transaction_id", "card_id", "merchant_id", "transaction_datetime", "amount_usd", "exchange_rate", "country_id", "card_home_country_id", "risk_profile_id", "is_chip_transaction", "is_cross_border", "days_since_previous_txn", "avg_amount_last_7d", "velocity_score", "high_risk_mcc_flag", "fraud_risk_score"],
        explicit_column_names=[]
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.1"):
            result = await step_2_1_attribute_count_detection(
                entity_name=entity_name,
                nl_description=nl_description,
                entity_description=entity_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_count", "explicit_attributes", "explicit_column_names"]
    )
    validations.append(struct_validation)
    
    # Validate types
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "has_explicit_count": bool,
            "explicit_attributes": list,
            "explicit_column_names": list
        }
    )
    validations.append(type_validation)
    
    # Validate explicit count logic
    count_validation = {"valid": True, "errors": []}
    if result_dict.get("has_explicit_count"):
        count = result_dict.get("count")
        if count is None or not isinstance(count, int) or count <= 0:
            count_validation["valid"] = False
            count_validation["errors"].append(
                "When has_explicit_count is True, count must be a positive integer"
            )
        explicit_attrs = result_dict.get("explicit_attributes", [])
        if not isinstance(explicit_attrs, list):
            count_validation["valid"] = False
            count_validation["errors"].append(
                "explicit_attributes must be a list"
            )
    else:
        if result_dict.get("count") is not None:
            count_validation["valid"] = False
            count_validation["errors"].append(
                "When has_explicit_count is False, count should be None"
            )
    validations.append(count_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.1")
    return all_valid


async def test_step_2_1_with_explicit_attributes():
    """Test Step 2.1 with explicit attribute names but no count."""
    TestResultDisplay.print_test_case(2, "Explicit attribute names without count")
    
    # Input - Card entity with multiple attributes
    entity_name = "Card"
    nl_description = "Card dimension table includes card_id, customer_id, card_type, card_number_hash, expiry_date, issue_date, card_status, credit_limit, available_balance, and card_home_country_id"
    input_data = {
        "entity_name": entity_name,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = AttributeCountOutput(
        has_explicit_count=False,
        count=None,
        explicit_attributes=["card_id", "customer_id", "card_type", "card_number_hash", "expiry_date", "issue_date", "card_status", "credit_limit", "available_balance", "card_home_country_id"],
        explicit_column_names=["card_id", "customer_id", "card_type", "card_number_hash", "expiry_date", "issue_date", "card_status", "credit_limit", "available_balance", "card_home_country_id"]
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.1"):
            result = await step_2_1_attribute_count_detection(
                entity_name=entity_name,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_count", "explicit_attributes", "explicit_column_names"]
    )
    validations.append(struct_validation)
    
    # Validate explicit attributes
    attrs_validation = {"valid": True, "errors": []}
    explicit_attrs = result.get("explicit_attributes", [])
    if not isinstance(explicit_attrs, list):
        attrs_validation["valid"] = False
        attrs_validation["errors"].append("explicit_attributes must be a list")
    else:
        for attr in explicit_attrs:
            if not isinstance(attr, str) or not attr.strip():
                attrs_validation["valid"] = False
                attrs_validation["errors"].append(
                    "All items in explicit_attributes must be non-empty strings"
                )
    validations.append(attrs_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.1")
    return all_valid


async def main():
    """Run all tests for Step 2.1."""
    results = []
    results.append(await test_step_2_1_with_explicit_count())
    results.append(await test_step_2_1_with_explicit_attributes())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
