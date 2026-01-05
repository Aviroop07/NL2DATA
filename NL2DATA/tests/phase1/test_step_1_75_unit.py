"""Unit tests for Step 1.75: Entity vs Relation Reclassification."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_75_entity_relation_reclassification import (
    step_1_75_entity_relation_reclassification,
    EntityRelationReclassificationOutput,
    ReclassifyAsRelation
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_75_basic():
    """Test Step 1.75 with basic input."""
    TestResultDisplay.print_test_header("Entity vs Relation Reclassification", "1.75")
    TestResultDisplay.print_test_case(1, "Basic reclassification")
    
    entities = [
        {"name": "FraudRing", "description": "A group of cards that transact with the same merchant in a short time window"},
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Card", "description": "A payment card"},
        {"name": "Merchant", "description": "A merchant where transactions occur"},
        {"name": "Customer", "description": "A customer who owns cards"}
    ]
    nl_description = "Fraud rings involve many cards transacting with the same merchant in a short time window. Transactions are linked to cards which belong to customers. Each transaction occurs at a merchant location."
    domain = "financial"
    input_data = {
        "entities": entities,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = EntityRelationReclassificationOutput(
        keep_entities=["Transaction", "Card", "Merchant", "Customer"],
        reclassify_as_relation=[
            ReclassifyAsRelation(
                name="FraudRing",
                reasoning="FraudRing represents a relationship pattern between multiple Cards and a Merchant, not a standalone entity",
                endpoints={"left": "Card", "right": "Merchant"},
                relationship_type="many_to_many",
                key_strategy="junction_table",
                relationship_attributes=["time_window", "transaction_count", "fraud_confidence_score"]
            )
        ],
        reasoning="FraudRing should be reclassified as a many-to-many relation between Card and Merchant, representing the pattern of coordinated fraud activity"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.75"):
            result = await step_1_75_entity_relation_reclassification(
                entities=entities,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["entities", "removed_entity_names", "reclassified"]
    )
    validations.append(struct_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.75")
    return all_valid


async def main():
    """Run all tests for Step 1.75."""
    results = []
    results.append(await test_step_1_75_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.75 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.75 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
