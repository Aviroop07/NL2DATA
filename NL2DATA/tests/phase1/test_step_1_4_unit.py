"""Unit tests for Step 1.4: Key Entity Extraction."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_4_key_entity_extraction import (
    step_1_4_key_entity_extraction,
    EntityExtractionOutput
)
from NL2DATA.ir.models.state import EntityInfo
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_4_basic():
    """Test Step 1.4 with basic input."""
    TestResultDisplay.print_test_header("Key Entity Extraction", "1.4")
    TestResultDisplay.print_test_case(1, "Basic entity extraction")
    
    nl_description = "Generate a financial transactions dataset with a large transaction fact table (at least 50 million rows) and dimension tables for customers, merchants, cards, and geography. Legitimate transactions should form the majority, with card-level spending showing strong weekly seasonality and pay-day spikes around the 1st and 15th of each month. Inject multiple fraud patterns: low-value test transactions followed by one or more high-value purchases; coordinated fraud rings where many cards transact with the same merchant in a short time window; and location anomalies where the same card has transactions in geographically distant locations within an impossible travel time."
    domain = "financial"
    input_data = {
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = EntityExtractionOutput(
        entities=[
            EntityInfo(
                name="Transaction",
                description="A financial transaction record containing transaction details, amounts, timestamps, and fraud indicators",
                mention_type="explicit",
                evidence="transaction fact table",
                confidence=1.0,
                reasoning="Explicitly mentioned as the main fact table"
            ),
            EntityInfo(
                name="Customer",
                description="A customer who owns cards and makes transactions",
                mention_type="explicit",
                evidence="customers",
                confidence=1.0,
                reasoning="Explicitly mentioned in dimension tables"
            ),
            EntityInfo(
                name="Merchant",
                description="A merchant or business where transactions occur",
                mention_type="explicit",
                evidence="merchants",
                confidence=1.0,
                reasoning="Explicitly mentioned in dimension tables"
            ),
            EntityInfo(
                name="Card",
                description="A payment card associated with a customer used for transactions",
                mention_type="explicit",
                evidence="cards",
                confidence=1.0,
                reasoning="Explicitly mentioned in dimension tables"
            ),
            EntityInfo(
                name="Geography",
                description="Geographic location information for transactions and merchants",
                mention_type="explicit",
                evidence="geography",
                confidence=1.0,
                reasoning="Explicitly mentioned in dimension tables"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.4"):
            result = await step_1_4_key_entity_extraction(
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["entities"]
    )
    validations.append(struct_validation)
    
    list_validation = ValidationHelper.validate_non_empty_lists(
        result,
        ["entities"]
    )
    validations.append(list_validation)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    entity_validation = {"valid": True, "errors": []}
    entities = result_dict.get("entities", [])
    if entities:
        for entity in entities:
            # Handle Pydantic models in the list
            if hasattr(entity, 'model_dump'):
                entity = entity.model_dump()
            if not isinstance(entity, dict):
                entity_validation["valid"] = False
                entity_validation["errors"].append("Each entity must be a dictionary")
                continue
            if "name" not in entity:
                entity_validation["valid"] = False
                entity_validation["errors"].append("Each entity must have a 'name' field")
            if "description" not in entity:
                entity_validation["valid"] = False
                entity_validation["errors"].append("Each entity must have a 'description' field")
    validations.append(entity_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.4")
    return all_valid


async def main():
    """Run all tests for Step 1.4."""
    results = []
    results.append(await test_step_1_4_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
