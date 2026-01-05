"""Unit tests for Step 1.10: Schema Connectivity Validation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_10_schema_connectivity import (
    step_1_10_schema_connectivity,
    ConnectivityValidationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_10_basic():
    """Test Step 1.10 with basic input."""
    TestResultDisplay.print_test_header("Schema Connectivity Validation", "1.10")
    TestResultDisplay.print_test_case(1, "Basic connectivity validation")
    
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Customer", "description": "A customer who owns cards"},
        {"name": "Card", "description": "A payment card used for transactions"},
        {"name": "Merchant", "description": "A merchant where transactions occur"},
        {"name": "Geography", "description": "Geographic location information"},
        {"name": "RiskProfile", "description": "Risk profile classification for fraud detection"}
    ]
    relations = [
        {"entities": ["Customer", "Card"], "type": "one-to-many"},
        {"entities": ["Card", "Transaction"], "type": "one-to-many"},
        {"entities": ["Merchant", "Transaction"], "type": "one-to-many"}
    ]
    nl_description = "Transactions are linked to cards which belong to customers. Merchants are located in geographic regions, and transactions occur at merchant locations. Risk profiles are used to classify transaction fraud patterns."
    input_data = {
        "entities": entities,
        "relations": relations,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    from NL2DATA.phases.phase1.step_1_10_schema_connectivity import (
        ConnectivityStatusEntry
    )
    mock_response = ConnectivityValidationOutput(
        orphan_entities=["Geography", "RiskProfile"],
        connectivity_status=[
            ConnectivityStatusEntry(entity_name="Transaction", is_connected=True),
            ConnectivityStatusEntry(entity_name="Customer", is_connected=True),
            ConnectivityStatusEntry(entity_name="Card", is_connected=True),
            ConnectivityStatusEntry(entity_name="Merchant", is_connected=True),
            ConnectivityStatusEntry(entity_name="Geography", is_connected=False),
            ConnectivityStatusEntry(entity_name="RiskProfile", is_connected=False)
        ],
        suggested_relations=[
            "Geography should relate to Merchant through location",
            "RiskProfile should relate to Transaction through risk classification"
        ],
        reasoning="Geography and RiskProfile entities are not connected to the main entity graph"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.10"):
            result = await step_1_10_schema_connectivity(
                entities=entities,
                relations=relations,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["orphan_entities", "connectivity_status", "suggested_relations", "reasoning"]
    )
    validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.10")
    return all_valid


async def main():
    """Run all tests for Step 1.10."""
    results = []
    results.append(await test_step_1_10_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.10 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.10 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
