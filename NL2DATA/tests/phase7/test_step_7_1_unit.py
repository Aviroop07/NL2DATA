"""Unit tests for Step 7.1: Information Need Identification.

Note: This is the same as Phase 3 Step 3.1, already tested.
This file tests it as Phase 7 Step 7.1.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.step_7_1_information_need_identification import (
    step_7_1_information_need_identification,
    InformationNeedOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_7_1_basic():
    """Test Step 7.1 with basic input."""
    TestResultDisplay.print_test_header("Information Need Identification", "7.1")
    TestResultDisplay.print_test_case(1, "Basic information need identification")
    
    nl_description = "Generate a financial transactions dataset. I need to identify fraud patterns including: find all transactions with high fraud risk scores above 75.0, identify coordinated fraud rings where many cards transact with the same merchant in a short time window, detect location anomalies where the same card has transactions in geographically distant locations within impossible travel time, and analyze card-level spending patterns showing weekly seasonality and pay-day spikes around the 1st and 15th of each month."
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Customer", "description": "A customer who owns cards"},
        {"name": "Card", "description": "A payment card"},
        {"name": "Merchant", "description": "A merchant where transactions occur"},
        {"name": "Geography", "description": "Geographic location information"}
    ]
    relations = [
        {"entities": ["Customer", "Card"], "type": "one-to-many"},
        {"entities": ["Card", "Transaction"], "type": "one-to-many"},
        {"entities": ["Merchant", "Transaction"], "type": "one-to-many"},
        {"entities": ["Geography", "Merchant"], "type": "one-to-many"}
    ]
    attributes = {
        "Transaction": [
            {"name": "transaction_id"}, {"name": "card_id"}, {"name": "merchant_id"},
            {"name": "transaction_datetime"}, {"name": "amount_usd"}, {"name": "fraud_risk_score"},
            {"name": "country_id"}, {"name": "card_home_country_id"}
        ],
        "Card": [{"name": "card_id"}, {"name": "customer_id"}],
        "Customer": [{"name": "customer_id"}, {"name": "name"}],
        "Merchant": [{"name": "merchant_id"}, {"name": "merchant_name"}],
        "Geography": [{"name": "country_id"}, {"name": "country_name"}]
    }
    primary_keys = {
        "Transaction": ["transaction_id"],
        "Card": ["card_id"],
        "Customer": ["customer_id"],
        "Merchant": ["merchant_id"],
        "Geography": ["country_id"]
    }
    foreign_keys = [
        {
            "from_entity": "Transaction",
            "from_attributes": ["card_id"],
            "to_entity": "Card",
            "to_attributes": ["card_id"]
        },
        {
            "from_entity": "Transaction",
            "from_attributes": ["merchant_id"],
            "to_entity": "Merchant",
            "to_attributes": ["merchant_id"]
        },
        {
            "from_entity": "Card",
            "from_attributes": ["customer_id"],
            "to_entity": "Customer",
            "to_attributes": ["customer_id"]
        }
    ]
    input_data = {
        "nl_description": nl_description,
        "entities": entities,
        "relations": relations,
        "attributes": attributes,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = InformationNeedOutput(
        information_needs=[
            {
                "description": "Find all transactions with high fraud risk scores above 75.0",
                "entities_involved": ["Transaction"],
                "conditions": {"fraud_risk_score": "> 75.0"},
                "reasoning": "Explicitly mentioned fraud detection query"
            },
            {
                "description": "Identify coordinated fraud rings where many cards transact with the same merchant in a short time window",
                "entities_involved": ["Transaction", "Card", "Merchant"],
                "conditions": {"time_window": "short", "merchant_grouping": "same"},
                "reasoning": "Fraud pattern detection query"
            },
            {
                "description": "Detect location anomalies where the same card has transactions in geographically distant locations within impossible travel time",
                "entities_involved": ["Transaction", "Card", "Geography"],
                "conditions": {"geographic_distance": "distant", "time_interval": "impossible"},
                "reasoning": "Location-based fraud anomaly detection"
            },
            {
                "description": "Analyze card-level spending patterns showing weekly seasonality and pay-day spikes around the 1st and 15th of each month",
                "entities_involved": ["Transaction", "Card"],
                "conditions": {"temporal_pattern": "weekly_seasonality", "pay_day": "1st_or_15th"},
                "reasoning": "Temporal pattern analysis query"
            }
        ],
        additions=[
            "Find all transactions with high fraud risk scores above 75.0",
            "Identify coordinated fraud rings",
            "Detect location anomalies",
            "Analyze card-level spending patterns"
        ],
        deletions=[],
        no_more_changes=True,
        reasoning="Four complex information needs identified: fraud risk filtering, fraud ring detection, location anomaly detection, and temporal pattern analysis"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("7.1"):
            result = await step_7_1_information_need_identification(
                nl_description=nl_description,
                entities=entities,
                relations=relations,
                attributes=attributes,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["information_needs", "additions", "deletions", "no_more_changes"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "information_needs": list,
            "additions": list,
            "deletions": list,
            "no_more_changes": bool
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "7.1")
    return all_valid


async def main():
    """Run all tests for Step 7.1."""
    results = []
    results.append(await test_step_7_1_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 7.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 7.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
