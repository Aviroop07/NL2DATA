"""Unit tests for Step 1.5: Relation Mention Detection."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_5_relation_mention_detection import (
    step_1_5_relation_mention_detection,
    RelationMentionOutput,
    RelationWithEvidence
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_5_basic():
    """Test Step 1.5 with basic input."""
    TestResultDisplay.print_test_header("Relation Mention Detection", "1.5")
    TestResultDisplay.print_test_case(1, "Basic relation mention detection")
    
    nl_description = "Transactions are linked to cards, which belong to customers. Merchants are located in geographic regions, and transactions occur at merchant locations. Cards can have multiple transactions, and customers can own multiple cards. Fraud rings involve many cards transacting with the same merchant in a short time window."
    entities = [
        {"name": "Transaction"}, 
        {"name": "Card"}, 
        {"name": "Customer"}, 
        {"name": "Merchant"}, 
        {"name": "Geography"}
    ]
    input_data = {
        "nl_description": nl_description,
        "entities": entities
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = RelationMentionOutput(
        has_explicit_relations=True,
        relations=[
            RelationWithEvidence(
                subject="Card",
                predicate="belong to",
                object="Customer",
                evidence="cards, which belong to customers"
            ),
            RelationWithEvidence(
                subject="Transaction",
                predicate="linked to",
                object="Card",
                evidence="Transactions are linked to cards"
            ),
            RelationWithEvidence(
                subject="Merchant",
                predicate="located in",
                object="Geography",
                evidence="Merchants are located in geographic regions"
            ),
            RelationWithEvidence(
                subject="Transaction",
                predicate="occur at",
                object="Merchant",
                evidence="transactions occur at merchant locations"
            ),
            RelationWithEvidence(
                subject="Card",
                predicate="transact with",
                object="Merchant",
                evidence="many cards transacting with the same merchant"
            )
        ],
        reasoning="Multiple relations are explicitly mentioned in the description"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.5"):
            result = await step_1_5_relation_mention_detection(
                nl_description=nl_description,
                entities=entities
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_relations", "relations", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "has_explicit_relations": bool,
            "relations": list,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    evidence_validation = {"valid": True, "errors": []}
    if result_dict.get("has_explicit_relations"):
        relations = result_dict.get("relations", [])
        if not isinstance(relations, list) or len(relations) == 0:
            evidence_validation["valid"] = False
            evidence_validation["errors"].append(
                "When has_explicit_relations is True, relations should be non-empty"
            )
        else:
            for rel in relations:
                # Handle Pydantic models in the list
                if hasattr(rel, 'model_dump'):
                    rel = rel.model_dump()
                if isinstance(rel, dict):
                    evidence = rel.get("evidence", "")
                    if evidence and evidence not in nl_description:
                        evidence_validation["valid"] = False
                        evidence_validation["errors"].append(
                            f"Evidence '{evidence}' must be a substring of input"
                        )
    validations.append(evidence_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.5")
    return all_valid


async def main():
    """Run all tests for Step 1.5."""
    results = []
    results.append(await test_step_1_5_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.5 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.5 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
