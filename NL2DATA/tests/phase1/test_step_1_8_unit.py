"""Unit tests for Step 1.8: Entity Cardinality & Table Type."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_8_entity_cardinality import (
    step_1_8_entity_cardinality_single,
    EntityCardinalityOutput,
    EntityCardinalityInfo
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_8_basic():
    """Test Step 1.8 with basic input."""
    TestResultDisplay.print_test_header("Entity Cardinality & Table Type", "1.8")
    TestResultDisplay.print_test_case(1, "Basic cardinality detection")
    
    entity_name = "Transaction"
    entity_description = "A financial transaction fact table with at least 50 million rows containing transaction details, fraud indicators, and temporal attributes"
    nl_description = "Generate a financial transactions dataset with a large transaction fact table (at least 50 million rows) and dimension tables for customers, merchants, cards, and geography. Legitimate transactions form the majority, with card-level spending showing strong weekly seasonality and pay-day spikes around the 1st and 15th of each month."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "entity_description": entity_description,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = EntityCardinalityOutput(
        entity_info=[
            EntityCardinalityInfo(
                entity="Transaction",
                has_explicit_cardinality=True,
                cardinality="very_large",
                cardinality_hint="at least 50 million rows",
                table_type="fact",
                reasoning="Transaction is explicitly described as a large fact table with at least 50 million rows, indicating very large cardinality"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.8"):
            result = await step_1_8_entity_cardinality_single(
                entity_name=entity_name,
                entity_description=entity_description,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["cardinality", "table_type"]
    )
    validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.8")
    return all_valid


async def main():
    """Run all tests for Step 1.8."""
    results = []
    results.append(await test_step_1_8_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.8 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.8 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
