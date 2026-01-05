"""Unit tests for Step 2.5: Temporal Attributes Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_5_temporal_attributes_detection import (
    step_2_5_temporal_attributes_detection,
    TemporalAttributesOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_5_basic():
    """Test Step 2.5 with basic input."""
    TestResultDisplay.print_test_header("Temporal Attributes Detection", "2.5")
    TestResultDisplay.print_test_case(1, "Basic temporal attribute detection")
    
    entity_name = "Transaction"
    nl_description = "Transactions occur over time with transaction_datetime timestamps. Card-level spending shows strong weekly seasonality and pay-day spikes around the 1st and 15th of each month. Fraud patterns include velocity features calculated over rolling time windows (last 24 hours, last 7 days). Transaction records need temporal tracking for fraud detection and time-series analysis."
    entity_description = "A financial transaction record with temporal attributes for fraud detection and time-series analysis"
    existing_attributes = ["transaction_id", "card_id", "merchant_id", "amount_usd", "transaction_datetime"]
    input_data = {
        "entity_name": entity_name,
        "nl_description": nl_description,
        "entity_description": entity_description,
        "existing_attributes": existing_attributes
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = TemporalAttributesOutput(
        needs_temporal=True,
        temporal_attributes=["transaction_datetime", "created_at", "updated_at", "processed_at"],
        reasoning="Transactions require temporal tracking for fraud detection (velocity features over time windows), seasonality analysis (weekly patterns, pay-day spikes), and audit trails (created_at, updated_at, processed_at timestamps)"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.5"):
            result = await step_2_5_temporal_attributes_detection(
                entity_name=entity_name,
                nl_description=nl_description,
                entity_description=entity_description,
                existing_attributes=existing_attributes
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["needs_temporal", "temporal_attributes", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "needs_temporal": bool,
            "temporal_attributes": list,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    temporal_validation = {"valid": True, "errors": []}
    if result_dict.get("needs_temporal"):
        temporal_attrs = result_dict.get("temporal_attributes", [])
        if not isinstance(temporal_attrs, list) or len(temporal_attrs) == 0:
            temporal_validation["valid"] = False
            temporal_validation["errors"].append(
                "When needs_temporal is True, temporal_attributes should be non-empty"
            )
    validations.append(temporal_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.5")
    return all_valid


async def main():
    """Run all tests for Step 2.5."""
    results = []
    results.append(await test_step_2_5_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
