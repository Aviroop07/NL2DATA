"""Unit tests for Step 4.1: Functional Dependency Analysis.

Note: This is actually Phase 8 Step 8.1, but registry shows it as Phase 4 Step 4.1.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_1_functional_dependency_analysis import (
    step_8_1_functional_dependency_analysis_single,
    FunctionalDependencyAnalysisOutput,
    FunctionalDependency
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_4_1_basic():
    """Test Step 4.1 (Phase 8.1) with basic input."""
    TestResultDisplay.print_test_header("Functional Dependency Analysis", "4.1")
    TestResultDisplay.print_test_case(1, "Basic functional dependency analysis")
    
    entity_name = "Merchant"
    entity_description = "A merchant or business entity where financial transactions occur, with geographic and category classification"
    attributes = [
        {"name": "merchant_id", "description": "Unique merchant identifier"},
        {"name": "merchant_name", "description": "Business name"},
        {"name": "merchant_category_code", "description": "MCC code for business classification"},
        {"name": "merchant_country_id", "description": "Country identifier"},
        {"name": "merchant_city", "description": "City name"},
        {"name": "merchant_postal_code", "description": "Postal code"},
        {"name": "merchant_type", "description": "Type classification"},
        {"name": "country_name", "description": "Country name from geography dimension"}
    ]
    primary_key = ["merchant_id"]
    nl_description = "Merchants are located in geographic regions. Merchant category code (MCC) determines merchant type. Postal code determines city within a country. Country ID determines country name through the geography dimension."
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "entity_description": entity_description,
        "attributes": attributes,
        "primary_key": primary_key,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = FunctionalDependencyAnalysisOutput(
        functional_dependencies=[
            FunctionalDependency(
                lhs=["merchant_category_code"],
                rhs=["merchant_type"],
                reasoning="Merchant category code (MCC) functionally determines merchant type classification"
            ),
            FunctionalDependency(
                lhs=["merchant_postal_code", "merchant_country_id"],
                rhs=["merchant_city"],
                reasoning="Postal code combined with country ID determines city (postal codes are unique within countries)"
            ),
            FunctionalDependency(
                lhs=["merchant_country_id"],
                rhs=["country_name"],
                reasoning="Country ID determines country name through geography dimension relationship"
            )
        ],
        should_add=[
            FunctionalDependency(
                lhs=["merchant_category_code"],
                rhs=["merchant_type"],
                reasoning="MCC determines merchant type"
            ),
            FunctionalDependency(
                lhs=["merchant_postal_code", "merchant_country_id"],
                rhs=["merchant_city"],
                reasoning="Postal code + country determines city"
            )
        ],
        should_remove=[],
        no_more_changes=True,
        reasoning="Identified three functional dependencies: MCC→merchant_type, (postal_code, country_id)→city, and country_id→country_name"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("4.1"):
            result = await step_8_1_functional_dependency_analysis_single(
                entity_name=entity_name,
                entity_description=entity_description,
                attributes=attributes,
                primary_key=primary_key,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["functional_dependencies", "should_add", "should_remove", "no_more_changes"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "functional_dependencies": list,
            "should_add": list,
            "should_remove": list,
            "no_more_changes": bool
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.1")
    return all_valid


async def main():
    """Run all tests for Step 4.1."""
    results = []
    results.append(await test_step_4_1_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
