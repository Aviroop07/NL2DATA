"""Unit tests for Step 1.6: Auxiliary Entity Suggestion."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_6_auxiliary_entity_suggestion import (
    step_1_6_auxiliary_entity_suggestion,
    AuxiliaryEntityOutput,
    AuxiliaryEntitySuggestion
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_6_basic():
    """Test Step 1.6 with basic input."""
    TestResultDisplay.print_test_header("Auxiliary Entity Suggestion", "1.6")
    TestResultDisplay.print_test_case(1, "Basic auxiliary entity suggestion")
    
    nl_description = "Generate a financial transactions dataset with a large transaction fact table and dimension tables for customers, merchants, cards, and geography. Transactions include fraud detection patterns with velocity features, cross-border flags, and risk scores. Card-level spending shows weekly seasonality and pay-day spikes."
    key_entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Customer", "description": "A customer who owns cards"},
        {"name": "Card", "description": "A payment card used for transactions"},
        {"name": "Merchant", "description": "A merchant where transactions occur"}
    ]
    domain = "financial"
    input_data = {
        "nl_description": nl_description,
        "key_entities": key_entities,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = AuxiliaryEntityOutput(
        suggested_entities=[
            AuxiliaryEntitySuggestion(
                name="RiskProfile",
                description="Risk profile classification for fraud detection and transaction scoring",
                reasoning="Fraud detection patterns and risk scores require a risk profile dimension",
                motivation="completeness",
                priority="must_have",
                trigger="fraud detection patterns with velocity features, cross-border flags, and risk scores"
            ),
            AuxiliaryEntitySuggestion(
                name="MerchantCategory",
                description="Merchant category code (MCC) classification for transaction categorization",
                reasoning="Merchant categorization is needed for fraud pattern analysis and high-risk MCC detection",
                motivation="completeness",
                priority="should_have",
                trigger="fraud detection patterns and merchant categorization"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.6"):
            result = await step_1_6_auxiliary_entity_suggestion(
                nl_description=nl_description,
                key_entities=key_entities,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["suggested_entities"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"suggested_entities": list}
    )
    validations.append(type_validation)
    
    entity_validation = {"valid": True, "errors": []}
    suggested = result_dict.get("suggested_entities", [])
    if suggested:
        for entity in suggested:
            # Handle Pydantic models in the list
            if hasattr(entity, 'model_dump'):
                entity = entity.model_dump()
            if not isinstance(entity, dict):
                entity_validation["valid"] = False
                entity_validation["errors"].append("Each suggested entity must be a dictionary")
                continue
            required_fields = ["name", "description", "reasoning", "motivation", "priority", "trigger"]
            for field in required_fields:
                if field not in entity:
                    entity_validation["valid"] = False
                    entity_validation["errors"].append(f"Missing required field: {field}")
    validations.append(entity_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.6")
    return all_valid


async def main():
    """Run all tests for Step 1.6."""
    results = []
    results.append(await test_step_1_6_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.6 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.6 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
