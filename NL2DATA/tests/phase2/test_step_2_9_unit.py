"""Unit tests for Step 2.9: Derived Attribute Formulas."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase2.step_2_9_derived_attribute_formulas import (
    step_2_9_derived_attribute_formulas,
    DerivedFormulaOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_9_basic():
    """Test Step 2.9 with basic input."""
    TestResultDisplay.print_test_header("Derived Attribute Formulas", "2.9")
    TestResultDisplay.print_test_case(1, "Basic derived formula generation")
    
    entity_name = "Transaction"
    attribute_name = "fraud_risk_score"
    entity_attributes = [
        {"name": "amount_usd", "description": "Transaction amount in USD"},
        {"name": "is_cross_border", "description": "Cross-border transaction flag"},
        {"name": "high_risk_mcc_flag", "description": "High-risk merchant category code flag"},
        {"name": "velocity_score", "description": "Velocity score from transaction frequency"},
        {"name": "base_risk_score", "description": "Base risk score"},
        {"name": "fraud_risk_score", "description": "Combined fraud risk score"}
    ]
    entity_description = "A financial transaction record with fraud detection features"
    nl_description = "Fraud risk score combines base_risk_score with 0.5 * is_cross_border + 0.7 * high_risk_mcc_flag + 0.3 * (amount_usd > high_amount_threshold) + velocity_score. The score integrates multiple fraud signals including cross-border flags, high-risk MCCs, amount thresholds, and velocity patterns."
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "entity_attributes": entity_attributes,
        "entity_description": entity_description,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = DerivedFormulaOutput(
        formula="base_risk_score + 0.5 * is_cross_border + 0.7 * high_risk_mcc_flag + 0.3 * (amount_usd > high_amount_threshold) + velocity_score",
        dependencies=["base_risk_score", "is_cross_border", "high_risk_mcc_flag", "amount_usd", "velocity_score"],
        formula_type="composite",
        reasoning="Fraud risk score is a composite derived attribute that combines base risk score with weighted fraud indicators: cross-border transactions (0.5 weight), high-risk MCCs (0.7 weight), high amount threshold violations (0.3 weight), and velocity patterns"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("2.9"):
            result = await step_2_9_derived_attribute_formulas(
                entity_name=entity_name,
                attribute_name=attribute_name,
                entity_attributes=entity_attributes,
                entity_description=entity_description,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["formula", "dependencies", "formula_type", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "formula": str,
            "dependencies": list,
            "formula_type": str,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.9")
    return all_valid


async def main():
    """Run all tests for Step 2.9."""
    results = []
    results.append(await test_step_2_9_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.9 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.9 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
