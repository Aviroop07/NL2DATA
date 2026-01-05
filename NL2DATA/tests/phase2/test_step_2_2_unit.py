"""Unit tests for Step 2.2: Intrinsic Attributes."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_2_intrinsic_attributes import (
    step_2_2_intrinsic_attributes,
    IntrinsicAttributesOutput
)
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_2_basic():
    """Test Step 2.2 with basic input."""
    TestResultDisplay.print_test_header("Intrinsic Attributes", "2.2")
    TestResultDisplay.print_test_case(1, "Basic intrinsic attribute extraction")
    
    entity_name = "Merchant"
    nl_description = "Merchants have merchant_id, merchant_name, merchant_category_code (MCC), merchant_country_id, merchant_city, merchant_postal_code, merchant_type, registration_date, and business_status"
    entity_description = "A merchant or business entity where financial transactions occur, with geographic and category classification"
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "nl_description": nl_description,
        "entity_description": entity_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = IntrinsicAttributesOutput(
        attributes=[
            AttributeInfo(
                name="merchant_id",
                description="Unique identifier for the merchant",
                type_hint="integer",
                reasoning="Primary key identifier for Merchant entity"
            ),
            AttributeInfo(
                name="merchant_name",
                description="Business name of the merchant",
                type_hint="string",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="merchant_category_code",
                description="MCC code classifying the merchant's business type",
                type_hint="string",
                reasoning="Explicitly mentioned as merchant_category_code (MCC)"
            ),
            AttributeInfo(
                name="merchant_country_id",
                description="Country where the merchant is located",
                type_hint="integer",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="merchant_city",
                description="City where the merchant operates",
                type_hint="string",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="merchant_postal_code",
                description="Postal code of merchant location",
                type_hint="string",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="merchant_type",
                description="Type classification of the merchant business",
                type_hint="string",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="registration_date",
                description="Date when merchant was registered in the system",
                type_hint="date",
                reasoning="Explicitly mentioned in description"
            ),
            AttributeInfo(
                name="business_status",
                description="Current operational status of the merchant business",
                type_hint="string",
                reasoning="Explicitly mentioned in description"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.2"):
            result = await step_2_2_intrinsic_attributes(
                entity_name=entity_name,
                nl_description=nl_description,
                entity_description=entity_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["attributes"]
    )
    validations.append(struct_validation)
    
    list_validation = ValidationHelper.validate_non_empty_lists(
        result,
        ["attributes"]
    )
    validations.append(list_validation)
    
    attribute_validation = {"valid": True, "errors": []}
    attributes = result_dict.get("attributes", [])
    if attributes:
        for attr in attributes:
            # Handle Pydantic models in the list
            if hasattr(attr, 'model_dump'):
                attr = attr.model_dump()
            if not isinstance(attr, dict):
                attribute_validation["valid"] = False
                attribute_validation["errors"].append("Each attribute must be a dictionary")
                continue
            if "name" not in attr:
                attribute_validation["valid"] = False
                attribute_validation["errors"].append("Each attribute must have a 'name' field")
            if "description" not in attr:
                attribute_validation["valid"] = False
                attribute_validation["errors"].append("Each attribute must have a 'description' field")
    validations.append(attribute_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.2")
    return all_valid


async def main():
    """Run all tests for Step 2.2."""
    results = []
    results.append(await test_step_2_2_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
