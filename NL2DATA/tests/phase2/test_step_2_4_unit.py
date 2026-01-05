"""Unit tests for Step 2.4: Composite Attribute Handling."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_4_composite_attribute_handling import (
    step_2_4_composite_attribute_handling,
    CompositeAttributeOutput,
    CompositeAttributeInfo
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_4_basic():
    """Test Step 2.4 with basic input."""
    TestResultDisplay.print_test_header("Composite Attribute Handling", "2.4")
    TestResultDisplay.print_test_case(1, "Basic composite attribute handling")
    
    entity_name = "Merchant"
    attributes = [
        {"name": "merchant_location", "description": "Merchant location information"},
        {"name": "merchant_contact", "description": "Merchant contact details"},
        {"name": "merchant_id", "description": "Merchant identifier"},
        {"name": "merchant_name", "description": "Merchant business name"},
        {"name": "merchant_category_code", "description": "MCC code"}
    ]
    nl_description = "Merchants have merchant_location (composite: country_id, city, postal_code), merchant_contact (composite: phone, email, contact_person), merchant_id, merchant_name, and merchant_category_code. Location and contact information are composite attributes that should be decomposed for normalization."
    input_data = {
        "entity_name": entity_name,
        "attributes": attributes,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = CompositeAttributeOutput(
        composite_attributes=[
            CompositeAttributeInfo(
                name="merchant_location",
                should_decompose=True,
                decomposition=["merchant_country_id", "merchant_city", "merchant_postal_code"],
                reasoning="merchant_location is a composite attribute containing geographic information (country, city, postal code) that should be decomposed for normalization and to support geographic queries"
            ),
            CompositeAttributeInfo(
                name="merchant_contact",
                should_decompose=True,
                decomposition=["merchant_phone", "merchant_email", "merchant_contact_person"],
                reasoning="merchant_contact is a composite attribute containing multiple contact methods that should be decomposed for better data organization and query flexibility"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.4"):
            result = await step_2_4_composite_attribute_handling(
                entity_name=entity_name,
                attributes=attributes,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["composite_attributes"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"composite_attributes": list}
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.4")
    return all_valid


async def main():
    """Run all tests for Step 2.4."""
    results = []
    results.append(await test_step_2_4_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
