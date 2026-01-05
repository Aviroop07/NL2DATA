"""Unit tests for Step 4.4: Categorical Detection.

Note: This is actually Phase 8 Step 8.2, but registry shows it as Phase 4 Step 4.4.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_2_categorical_column_identification import (
    step_8_2_categorical_column_identification_batch
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_4_4_basic():
    """Test Step 4.4 (Phase 8.2) with basic input."""
    TestResultDisplay.print_test_header("Categorical Detection", "4.4")
    TestResultDisplay.print_test_case(1, "Basic categorical detection")
    
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Card", "description": "A payment card"},
        {"name": "Merchant", "description": "A merchant where transactions occur"}
    ]
    entity_attributes = {
        "Transaction": [
            {"name": "transaction_id", "description": "Transaction ID", "type": "BIGINT"},
            {"name": "is_cross_border", "description": "Cross-border flag", "type": "BOOLEAN"},
            {"name": "high_risk_mcc_flag", "description": "High-risk MCC flag", "type": "BOOLEAN"},
            {"name": "transaction_type", "description": "Transaction type", "type": "VARCHAR"},
            {"name": "fraud_risk_score", "description": "Fraud risk score", "type": "DECIMAL"}
        ],
        "Card": [
            {"name": "card_id", "description": "Card ID", "type": "BIGINT"},
            {"name": "card_type", "description": "Card type", "type": "VARCHAR"},
            {"name": "card_status", "description": "Card status", "type": "VARCHAR"}
        ],
        "Merchant": [
            {"name": "merchant_id", "description": "Merchant ID", "type": "BIGINT"},
            {"name": "merchant_category_code", "description": "MCC code", "type": "VARCHAR"},
            {"name": "merchant_type", "description": "Merchant type", "type": "VARCHAR"}
        ]
    }
    nl_description = "Transactions have transaction_type values: purchase, refund, chargeback, reversal. Cards have card_type: credit, debit, prepaid. Card status values: active, blocked, expired, closed. Merchant category codes are categorical with values like retail, travel, electronics, groceries. High-risk MCC flags indicate fraud-prone categories."
    domain = "financial"
    input_data = {
        "entities": entities,
        "entity_attributes": entity_attributes,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response_dict = {
        "entity_results": {
            "Transaction": {
                "categorical_attributes": ["transaction_type", "is_cross_border", "high_risk_mcc_flag"],
                "reasoning": "transaction_type is categorical with limited values (purchase, refund, chargeback, reversal), is_cross_border and high_risk_mcc_flag are boolean categorical attributes"
            },
            "Card": {
                "categorical_attributes": ["card_type", "card_status"],
                "reasoning": "card_type and card_status are categorical attributes with limited value sets"
            },
            "Merchant": {
                "categorical_attributes": ["merchant_category_code", "merchant_type"],
                "reasoning": "merchant_category_code (MCC) and merchant_type are categorical classification attributes"
            }
        }
    }
    
    with patch('NL2DATA.phases.phase8.step_8_2_categorical_column_identification.step_8_2_categorical_column_identification_batch') as mock_step:
        mock_step.return_value = mock_response_dict
        result = await step_8_2_categorical_column_identification_batch(
            entities=entities,
            entity_attributes=entity_attributes,
            nl_description=nl_description,
            domain=domain
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["entity_results"]
    )
    validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.4")
    return all_valid


async def main():
    """Run all tests for Step 4.4."""
    results = []
    results.append(await test_step_4_4_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
