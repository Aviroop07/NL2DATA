"""Unit tests for Step 1.12: Relation Validation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_12_relation_validation import (
    step_1_12_relation_validation,
    RelationValidationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_12_basic():
    """Test Step 1.12 with basic input."""
    TestResultDisplay.print_test_header("Relation Validation", "1.12")
    TestResultDisplay.print_test_case(1, "Basic relation validation")
    
    entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "Customer", "description": "A customer who owns cards"},
        {"name": "Card", "description": "A payment card used for transactions"},
        {"name": "Merchant", "description": "A merchant where transactions occur"},
        {"name": "Geography", "description": "Geographic location information"}
    ]
    relations = [
        {
            "entities": ["Customer", "Card"],
            "type": "one-to-many",
            "description": "Customer owns Card"
        },
        {
            "entities": ["Card", "Transaction"],
            "type": "one-to-many",
            "description": "Card used in Transaction"
        },
        {
            "entities": ["Merchant", "Transaction"],
            "type": "one-to-many",
            "description": "Merchant receives Transaction"
        },
        {
            "entities": ["Geography", "Merchant"],
            "type": "one-to-many",
            "description": "Geography contains Merchant"
        }
    ]
    relation_cardinalities = [
        {
            "relation_id": "Customer+Card",
            "entity_cardinalities": {"Customer": "1", "Card": "N"},
            "entity_participations": {"Customer": "partial", "Card": "total"}
        },
        {
            "relation_id": "Card+Transaction",
            "entity_cardinalities": {"Card": "1", "Transaction": "N"},
            "entity_participations": {"Card": "partial", "Transaction": "total"}
        },
        {
            "relation_id": "Merchant+Transaction",
            "entity_cardinalities": {"Merchant": "1", "Transaction": "N"},
            "entity_participations": {"Merchant": "partial", "Transaction": "total"}
        },
        {
            "relation_id": "Geography+Merchant",
            "entity_cardinalities": {"Geography": "1", "Merchant": "N"},
            "entity_participations": {"Geography": "partial", "Merchant": "total"}
        }
    ]
    nl_description = "Transactions are linked to cards which belong to customers. Merchants are located in geographic regions, and transactions occur at merchant locations. Fraud patterns include coordinated fraud rings where many cards transact with the same merchant in a short time window."
    input_data = {
        "entities": entities,
        "relations": relations,
        "relation_cardinalities": relation_cardinalities,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = RelationValidationOutput(
        impossible_cardinalities=[],
        conflicts=[],
        validation_passed=True,
        reasoning="All relations are valid: Customer-Card (1:N), Card-Transaction (1:N), Merchant-Transaction (1:N), and Geography-Merchant (1:N) form a consistent schema with proper cardinalities and participations"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.12"):
            result = await step_1_12_relation_validation(
                entities=entities,
                relations=relations,
                relation_cardinalities=relation_cardinalities,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["impossible_cardinalities", "conflicts", "validation_passed", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "impossible_cardinalities": list,
            "conflicts": list,
            "validation_passed": bool,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.12")
    return all_valid


async def main():
    """Run all tests for Step 1.12."""
    results = []
    results.append(await test_step_1_12_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.12 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.12 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
