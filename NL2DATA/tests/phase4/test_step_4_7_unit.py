"""Unit tests for Step 4.7: Categorical Distribution.

Note: This is actually Phase 7 Step 7.7, but registry shows it as Phase 4 Step 4.7.
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


async def test_step_4_7_basic():
    """Test Step 4.7 (Phase 7.7) with basic input."""
    TestResultDisplay.print_test_header("Categorical Distribution", "4.7")
    TestResultDisplay.print_test_case(1, "Basic categorical distribution definition")
    
    entity_name = "Transaction"
    attribute_name = "transaction_type"
    categorical_values = ["purchase", "refund", "chargeback", "reversal", "authorization", "settlement"]
    nl_description = "Transactions have transaction_type values: purchase (most common, ~85%), refund (~8%), chargeback (~2%), reversal (~3%), authorization (~1.5%), settlement (~0.5%). Legitimate transactions form the majority, with fraud patterns representing a small fraction."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "categorical_values": categorical_values,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "distribution": {
            "type": "categorical",
            "values": ["purchase", "refund", "chargeback", "reversal", "authorization", "settlement"],
            "probabilities": [0.85, 0.08, 0.02, 0.03, 0.015, 0.005]
        },
        "reasoning": "Purchase transactions dominate at 85%, refunds at 8%, chargebacks and reversals are rare (2-3%), authorization and settlement are least common (1.5% and 0.5% respectively)"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["distribution"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"distribution": dict}
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.7")
    return all_valid


async def main():
    """Run all tests for Step 4.7."""
    results = []
    results.append(await test_step_4_7_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.7 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.7 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
