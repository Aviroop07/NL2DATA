"""Unit tests for Step 1.9: Key Relations Extraction."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_9_key_relations_extraction import (
    step_1_9_key_relations_extraction,
    RelationExtractionOutput
)
from NL2DATA.ir.models.state import RelationInfo
from NL2DATA.ir.models.relation_type import RelationType
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_9_basic():
    """Test Step 1.9 with basic input."""
    TestResultDisplay.print_test_header("Key Relations Extraction", "1.9")
    TestResultDisplay.print_test_case(1, "Basic relation extraction")
    
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Customer", "description": "A customer who owns cards"},
        {"name": "Card", "description": "A payment card used for transactions"},
        {"name": "Merchant", "description": "A merchant where transactions occur"},
        {"name": "Geography", "description": "Geographic location information"}
    ]
    nl_description = "Generate a financial transactions dataset with a large transaction fact table and dimension tables for customers, merchants, cards, and geography. Transactions are linked to cards, which belong to customers. Merchants are located in geographic regions, and transactions occur at merchant locations."
    domain = "financial"
    input_data = {
        "entities": entities,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = RelationExtractionOutput(
        relations=[
            RelationInfo(
                entities=["Customer", "Card"],
                type=RelationType.ONE_TO_MANY,
                description="Customer owns Card",
                arity=2,
                reasoning="Cards belong to customers",
                source="explicit_in_text",
                evidence="cards, which belong to customers"
            ),
            RelationInfo(
                entities=["Card", "Transaction"],
                type=RelationType.ONE_TO_MANY,
                description="Card used in Transaction",
                arity=2,
                reasoning="Transactions are linked to cards",
                source="explicit_in_text",
                evidence="Transactions are linked to cards"
            ),
            RelationInfo(
                entities=["Merchant", "Transaction"],
                type=RelationType.ONE_TO_MANY,
                description="Merchant receives Transaction",
                arity=2,
                reasoning="Transactions occur at merchants",
                source="explicit_in_text",
                evidence="transactions occur at merchant locations"
            ),
            RelationInfo(
                entities=["Geography", "Merchant"],
                type=RelationType.ONE_TO_MANY,
                description="Geography contains Merchant",
                arity=2,
                reasoning="Merchants are located in geographic regions",
                source="explicit_in_text",
                evidence="Merchants are located in geographic regions"
            )
        ]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.9"):
            result = await step_1_9_key_relations_extraction(
                entities=entities,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["relations"]
    )
    validations.append(struct_validation)
    
    list_validation = ValidationHelper.validate_non_empty_lists(
        result,
        ["relations"]
    )
    validations.append(list_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.9")
    return all_valid


async def main():
    """Run all tests for Step 1.9."""
    results = []
    results.append(await test_step_1_9_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.9 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.9 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
