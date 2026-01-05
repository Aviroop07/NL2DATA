"""Unit tests for Step 2.7: Primary Key Identification."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_7_primary_key_identification import (
    step_2_7_primary_key_identification,
    PrimaryKeyOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_7_basic():
    """Test Step 2.7 with basic input."""
    TestResultDisplay.print_test_header("Primary Key Identification", "2.7")
    TestResultDisplay.print_test_case(1, "Basic primary key identification")
    
    entity_name = "Transaction"
    attributes = [
        "transaction_id", "card_id", "merchant_id", "transaction_datetime", 
        "amount_usd", "exchange_rate", "country_id", "card_home_country_id", 
        "risk_profile_id", "is_chip_transaction", "is_cross_border", 
        "days_since_previous_txn", "avg_amount_last_7d", "velocity_score", 
        "high_risk_mcc_flag", "fraud_risk_score"
    ]
    nl_description = "Transaction fact table has transaction_id as unique identifier, with foreign keys to card_id, merchant_id, country_id, and risk_profile_id. Transaction amounts follow log-normal distribution with heavy tail, and fraud patterns include velocity features and cross-border flags."
    entity_description = "A financial transaction record with fraud detection features, temporal attributes, and derived risk scores"
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "nl_description": nl_description,
        "entity_description": entity_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = PrimaryKeyOutput(
        primary_key=["transaction_id"],
        reasoning="transaction_id uniquely identifies each transaction record in the fact table",
        alternative_keys=[["card_id", "transaction_datetime"], ["merchant_id", "transaction_datetime"]]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.7"):
            result = await step_2_7_primary_key_identification(
                entity_name=entity_name,
                attributes=attributes,
                nl_description=nl_description,
                entity_description=entity_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["primary_key", "reasoning"]
    )
    validations.append(struct_validation)
    
    list_validation = ValidationHelper.validate_non_empty_lists(
        result,
        ["primary_key"]
    )
    validations.append(list_validation)
    
    pk_validation = {"valid": True, "errors": []}
    primary_key = result_dict.get("primary_key", [])
    if primary_key:
        for pk_attr in primary_key:
            if pk_attr not in attributes:
                pk_validation["valid"] = False
                pk_validation["errors"].append(
                    f"Primary key attribute '{pk_attr}' must be in the attributes list"
                )
    validations.append(pk_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.7")
    return all_valid


async def main():
    """Run all tests for Step 2.7."""
    results = []
    results.append(await test_step_2_7_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.7 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.7 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
