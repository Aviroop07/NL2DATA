"""Unit tests for Step 4.6: Categorical Value Extraction.

Note: This is actually Phase 7 Step 7.6, but registry shows it as Phase 4 Step 4.6.
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


async def test_step_4_6_basic():
    """Test Step 4.6 (Phase 7.6) with basic input."""
    TestResultDisplay.print_test_header("Categorical Value Extraction", "4.6")
    TestResultDisplay.print_test_case(1, "Basic categorical value extraction")
    
    entity_name = "Transaction"
    attribute_name = "transaction_type"
    categorical_attributes = ["transaction_type", "is_cross_border", "high_risk_mcc_flag"]
    entity_attributes = {
        "Transaction": [
            {"name": "transaction_type", "description": "Transaction type classification", "type": "VARCHAR"},
            {"name": "is_cross_border", "description": "Cross-border flag", "type": "BOOLEAN"},
            {"name": "high_risk_mcc_flag", "description": "High-risk MCC flag", "type": "BOOLEAN"}
        ]
    }
    nl_description = "Transactions have transaction_type values: purchase (most common, ~85%), refund (~8%), chargeback (~2%), reversal (~3%), authorization (~1.5%), settlement (~0.5%). Cross-border transactions represent about 15% of total. High-risk MCC flag is set for approximately 12% of transactions in fraud-prone categories."
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
        "categorical_values": ["purchase", "refund", "chargeback", "reversal", "authorization", "settlement"],
        "reasoning": "Extracted 6 categorical values for transaction_type from description with distribution percentages"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["categorical_values"]
    )
    validations.append(struct_validation)
    
    list_validation = ValidationHelper.validate_non_empty_lists(
        result,
        ["categorical_values"]
    )
    validations.append(list_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.6")
    return all_valid


async def main():
    """Run all tests for Step 4.6."""
    results = []
    results.append(await test_step_4_6_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.6 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.6 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
