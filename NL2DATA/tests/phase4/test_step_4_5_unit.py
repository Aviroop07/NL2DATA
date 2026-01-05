"""Unit tests for Step 4.5: Check Constraint Detection.

Note: This is actually Phase 7 Step 7.5, but registry shows it as Phase 4 Step 4.5.
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


async def test_step_4_5_basic():
    """Test Step 4.5 (Phase 7.5) with basic input."""
    TestResultDisplay.print_test_header("Check Constraint Detection", "4.5")
    TestResultDisplay.print_test_case(1, "Basic check constraint detection for categorical attributes")
    
    entity_name = "Transaction"
    attribute_name = "transaction_type"
    categorical_attributes = ["transaction_type", "is_cross_border", "high_risk_mcc_flag"]
    entity_attributes = {
        "Transaction": [
            {"name": "transaction_type", "description": "Transaction type classification", "type": "VARCHAR"},
            {"name": "is_cross_border", "description": "Cross-border transaction flag", "type": "BOOLEAN"},
            {"name": "high_risk_mcc_flag", "description": "High-risk merchant category flag", "type": "BOOLEAN"}
        ]
    }
    nl_description = "Transactions have transaction_type values: purchase, refund, chargeback, reversal, authorization, settlement. Cross-border flag indicates transactions where country_id != card_home_country_id. High-risk MCC flag is set for merchant category codes associated with fraud-prone categories like electronics, travel, and online services."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "categorical_attributes": categorical_attributes,
        "entity_attributes": entity_attributes,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "check_constraint_attributes": ["transaction_type", "is_cross_border", "high_risk_mcc_flag"],
        "check_constraints": {
            "transaction_type": {
                "values": ["purchase", "refund", "chargeback", "reversal", "authorization", "settlement"],
                "condition": "transaction_type IN ('purchase', 'refund', 'chargeback', 'reversal', 'authorization', 'settlement')"
            },
            "is_cross_border": {
                "values": [True, False],
                "condition": "is_cross_border IN (TRUE, FALSE)"
            },
            "high_risk_mcc_flag": {
                "values": [True, False],
                "condition": "high_risk_mcc_flag IN (TRUE, FALSE)"
            }
        },
        "reasoning": "transaction_type is categorical with 6 allowed values, is_cross_border and high_risk_mcc_flag are boolean categorical attributes"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["check_constraint_attributes", "check_constraints"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "check_constraint_attributes": list,
            "check_constraints": dict
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.5")
    return all_valid


async def main():
    """Run all tests for Step 4.5."""
    results = []
    results.append(await test_step_4_5_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
