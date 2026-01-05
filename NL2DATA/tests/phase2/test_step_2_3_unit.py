"""Unit tests for Step 2.3: Attribute Synonym Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_3_attribute_synonym_detection import (
    step_2_3_attribute_synonym_detection,
    AttributeSynonymOutput,
    AttributeSynonymInfo
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_3_basic():
    """Test Step 2.3 with basic input."""
    TestResultDisplay.print_test_header("Attribute Synonym Detection", "2.3")
    TestResultDisplay.print_test_case(1, "Basic synonym detection")
    
    entity_name = "Transaction"
    attributes = [
        {"name": "transaction_datetime", "description": "Transaction timestamp"},
        {"name": "transaction_timestamp", "description": "Transaction timestamp"},
        {"name": "txn_date", "description": "Transaction date"},
        {"name": "amount_usd", "description": "Transaction amount in USD"},
        {"name": "transaction_amount", "description": "Transaction amount"},
        {"name": "fraud_risk_score", "description": "Fraud risk score"},
        {"name": "risk_score", "description": "Risk score for fraud detection"}
    ]
    nl_description = "Transaction fact table contains transaction_datetime (also called transaction_timestamp or txn_date), amount_usd (also transaction_amount), and fraud_risk_score (also risk_score). Transactions include fraud detection patterns with velocity features and risk scoring."
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = AttributeSynonymOutput(
        synonyms=[
            AttributeSynonymInfo(
                attr1="transaction_datetime",
                attr2="transaction_timestamp",
                should_merge=True,
                preferred_name="transaction_datetime",
                reasoning="transaction_datetime and transaction_timestamp are synonyms for the same temporal attribute"
            ),
            AttributeSynonymInfo(
                attr1="transaction_datetime",
                attr2="txn_date",
                should_merge=True,
                preferred_name="transaction_datetime",
                reasoning="txn_date is an abbreviation for transaction_datetime"
            ),
            AttributeSynonymInfo(
                attr1="amount_usd",
                attr2="transaction_amount",
                should_merge=True,
                preferred_name="amount_usd",
                reasoning="amount_usd and transaction_amount refer to the same monetary value"
            ),
            AttributeSynonymInfo(
                attr1="fraud_risk_score",
                attr2="risk_score",
                should_merge=True,
                preferred_name="fraud_risk_score",
                reasoning="fraud_risk_score and risk_score are synonyms for fraud detection scoring"
            )
        ],
        merged_attributes=["transaction_timestamp", "txn_date", "transaction_amount", "risk_score"],
        final_attribute_list=["transaction_datetime", "amount_usd", "fraud_risk_score"]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.3"):
            result = await step_2_3_attribute_synonym_detection(
                entity_name=entity_name,
                attributes=attributes,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["synonyms", "merged_attributes", "final_attribute_list"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "synonyms": list,
            "merged_attributes": list,
            "final_attribute_list": list
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.3")
    return all_valid


async def main():
    """Run all tests for Step 2.3."""
    results = []
    results.append(await test_step_2_3_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.3 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.3 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
