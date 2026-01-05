"""Unit tests for Step 3.2: Information Completeness Check.

Note: This is actually Phase 7 Step 7.2 (information_completeness), but registry shows it as Phase 3 Step 3.2.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.step_7_2_information_completeness import (
    step_6_2_information_completeness,
    InformationCompletenessOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_3_2_completeness_basic():
    """Test Step 3.2 (Phase 7.2 completeness) with basic input."""
    TestResultDisplay.print_test_header("Information Completeness Check", "3.2")
    TestResultDisplay.print_test_case(1, "Basic completeness check")
    
    information_need = {
        "description": "Identify coordinated fraud rings where many cards transact with the same merchant in a short time window, including card and merchant details",
        "entities_involved": ["Transaction", "Card", "Merchant"],
        "conditions": {"time_window": "short", "merchant_grouping": "same"}
    }
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Card", "description": "A payment card"},
        {"name": "Merchant", "description": "A merchant where transactions occur"}
    ]
    relations = [
        {"entities": ["Card", "Transaction"], "type": "one-to-many"},
        {"entities": ["Merchant", "Transaction"], "type": "one-to-many"}
    ]
    attributes = {
        "Transaction": [
            {"name": "transaction_id", "description": "Transaction identifier"},
            {"name": "card_id", "description": "Foreign key to Card"},
            {"name": "merchant_id", "description": "Foreign key to Merchant"},
            {"name": "transaction_datetime", "description": "Transaction timestamp"},
            {"name": "amount_usd", "description": "Transaction amount"},
            {"name": "fraud_risk_score", "description": "Fraud risk score"}
        ],
        "Card": [
            {"name": "card_id", "description": "Card identifier"},
            {"name": "card_type", "description": "Card type"},
            {"name": "customer_id", "description": "Foreign key to Customer"}
        ],
        "Merchant": [
            {"name": "merchant_id", "description": "Merchant identifier"},
            {"name": "merchant_name", "description": "Merchant business name"},
            {"name": "merchant_category_code", "description": "MCC code"}
        ]
    }
    domain = "financial"
    input_data = {
        "information_need": information_need,
        "entities": entities,
        "relations": relations,
        "attributes": attributes,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = InformationCompletenessOutput(
        is_complete=True,
        missing_attributes=[],
        missing_entities=[],
        missing_relations=[],
        suggestions=[],
        reasoning="Schema is complete for fraud ring detection: Transaction has card_id and merchant_id foreign keys, transaction_datetime for time window analysis, and fraud_risk_score for filtering. Card and Merchant entities provide necessary dimension details."
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("3.2"):
            result = await step_6_2_information_completeness(
                information_need=information_need,
                entities=entities,
                relations=relations,
                attributes=attributes,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["is_complete", "missing_attributes", "missing_entities", "missing_relations"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "is_complete": bool,
            "missing_attributes": list,
            "missing_entities": list,
            "missing_relations": list
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "3.2")
    return all_valid


async def main():
    """Run all tests for Step 3.2."""
    results = []
    results.append(await test_step_3_2_completeness_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 3.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 3.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
