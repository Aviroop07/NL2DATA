"""Unit tests for Step 4.3: Data Type Assignment.

Note: This maps to Phase 5 Step 5.2 (independent) and 5.4 (dependent).
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5.step_5_2_independent_attribute_data_types import (
    step_5_2_independent_attribute_data_types
)
from NL2DATA.utils.data_types.type_assignment import (
    DataTypeAssignmentOutput,
    AttributeTypeInfo
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_4_3_basic():
    """Test Step 4.3 (Phase 5.2) with basic input."""
    TestResultDisplay.print_test_header("Data Type Assignment", "4.3")
    TestResultDisplay.print_test_case(1, "Basic data type assignment")
    
    entity_name = "Transaction"
    attribute_name = "fraud_risk_score"
    attributes = {
        "Transaction": [
            {"name": "transaction_id", "description": "Transaction identifier", "type_hint": "integer"},
            {"name": "amount_usd", "description": "Transaction amount in USD", "type_hint": "decimal"},
            {"name": "fraud_risk_score", "description": "Combined fraud risk score (0.0-100.0)", "type_hint": "decimal"},
            {"name": "velocity_score", "description": "Velocity score from transaction frequency", "type_hint": "decimal"},
            {"name": "transaction_datetime", "description": "Transaction timestamp", "type_hint": "timestamp"}
        ]
    }
    primary_keys = {"Transaction": ["transaction_id"]}
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "attributes": attributes,
        "primary_keys": primary_keys,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = DataTypeAssignmentOutput(
        attribute_types={
            "fraud_risk_score": AttributeTypeInfo(
                type="DECIMAL",
                precision=5,
                scale=2,
                reasoning="Fraud risk score is a decimal value ranging from 0.0 to 100.0, requiring DECIMAL(5,2) to accommodate the full range with 2 decimal places for precision"
            )
        }
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("4.3"):
            result = await step_5_2_independent_attribute_data_types(
                entity_name=entity_name,
                attribute_name=attribute_name,
                attributes=attributes,
                primary_keys=primary_keys,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["data_types"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"data_types": dict}
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.3")
    return all_valid


async def main():
    """Run all tests for Step 4.3."""
    results = []
    results.append(await test_step_4_3_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.3 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.3 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
