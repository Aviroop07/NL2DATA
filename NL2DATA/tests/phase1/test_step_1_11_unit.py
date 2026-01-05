"""Unit tests for Step 1.11: Relation Cardinality & Participation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_11_relation_cardinality import (
    step_1_11_relation_cardinality_single,
    RelationCardinalityOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_11_basic():
    """Test Step 1.11 with basic input."""
    TestResultDisplay.print_test_header("Relation Cardinality & Participation", "1.11")
    TestResultDisplay.print_test_case(1, "Basic cardinality detection")
    
    relation = {
        "entities": ["Card", "Transaction"],
        "type": "one-to-many",
        "description": "Card used in Transaction"
    }
    entities = [
        {"name": "Card", "description": "A payment card associated with a customer"},
        {"name": "Transaction", "description": "A financial transaction record"}
    ]
    nl_description = "Transactions are linked to cards, which belong to customers. Each card can have multiple transactions over time, with card-level spending showing strong weekly seasonality and pay-day spikes. Fraud patterns include coordinated fraud rings where many cards transact with the same merchant in a short time window."
    input_data = {
        "relation": relation,
        "entities": entities,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    from NL2DATA.phases.phase1.step_1_11_relation_cardinality import (
        EntityCardinalityEntry,
        EntityParticipationEntry
    )
    mock_response = RelationCardinalityOutput(
        entity_cardinalities=[
            EntityCardinalityEntry(entity_name="Card", cardinality="1"),
            EntityCardinalityEntry(entity_name="Transaction", cardinality="N")
        ],
        entity_participations=[
            EntityParticipationEntry(entity_name="Card", participation="partial"),
            EntityParticipationEntry(entity_name="Transaction", participation="total")
        ],
        reasoning="One card can be used for many transactions over time, but each transaction is associated with exactly one card. Not all cards necessarily have transactions (partial participation), but every transaction must have a card (total participation)"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.11"):
            result = await step_1_11_relation_cardinality_single(
                relation=relation,
                entities=entities,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["entity_cardinalities", "entity_participations", "reasoning"]
    )
    validations.append(struct_validation)
    
    cardinality_validation = {"valid": True, "errors": []}
    entity_cardinalities_list = result_dict.get("entity_cardinalities", [])
    entity_participations_list = result_dict.get("entity_participations", [])
    if not entity_cardinalities_list or not entity_participations_list:
        cardinality_validation["valid"] = False
        cardinality_validation["errors"].append(
            "entity_cardinalities and entity_participations must be non-empty lists"
        )
    else:
        # Convert lists to dicts for easier checking
        entity_cardinalities = {entry.get("entity_name"): entry.get("cardinality") for entry in entity_cardinalities_list if isinstance(entry, dict) or hasattr(entry, 'model_dump')}
        entity_participations = {entry.get("entity_name"): entry.get("participation") for entry in entity_participations_list if isinstance(entry, dict) or hasattr(entry, 'model_dump')}
        # Check for Card and Transaction (the actual entities in the test)
        for entity_name in ["Card", "Transaction"]:
            if entity_name not in entity_cardinalities:
                cardinality_validation["valid"] = False
                cardinality_validation["errors"].append(
                    f"Missing cardinality for entity: {entity_name}"
                )
            if entity_name not in entity_participations:
                cardinality_validation["valid"] = False
                cardinality_validation["errors"].append(
                    f"Missing participation for entity: {entity_name}"
                )
    validations.append(cardinality_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.11")
    return all_valid


async def main():
    """Run all tests for Step 1.11."""
    results = []
    results.append(await test_step_1_11_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.11 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.11 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
