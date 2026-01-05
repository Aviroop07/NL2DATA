"""Unit tests for Step 1.7: Entity Consolidation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_7_entity_consolidation import (
    step_1_7_entity_consolidation,
    EntityConsolidationOutput,
    MergeDecision,
    MergeDecisionEvidence,
    RenameSuggestion
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_7_basic():
    """Test Step 1.7 with basic input."""
    TestResultDisplay.print_test_header("Entity Consolidation", "1.7")
    TestResultDisplay.print_test_case(1, "Basic entity consolidation")
    
    key_entities = [
        {"name": "Transaction", "description": "A financial transaction record"},
        {"name": "CardTransaction", "description": "A card-based transaction"},
        {"name": "Payment", "description": "A payment transaction"}
    ]
    auxiliary_entities = [
        {"name": "RiskProfile", "description": "Risk profile for fraud detection"},
        {"name": "FraudIndicator", "description": "Fraud indicator entity"}
    ]
    domain = "financial"
    nl_description = "Generate a financial transactions dataset with transaction fact table. Transactions include card transactions and payment records. Fraud detection uses risk profiles and fraud indicators to identify suspicious patterns including coordinated fraud rings and location anomalies."
    input_data = {
        "key_entities": key_entities,
        "auxiliary_entities": auxiliary_entities,
        "domain": domain,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = EntityConsolidationOutput(
        merge_decisions=[
            MergeDecision(
                entity1="Transaction",
                entity2="CardTransaction",
                similarity=0.95,
                should_merge=True,
                merged_entity_name="Transaction",
                evidence=MergeDecisionEvidence(
                    definition_overlap="Transaction and CardTransaction both represent financial transaction records, with CardTransaction being a specific type of Transaction"
                ),
                reasoning="CardTransaction is a subtype of Transaction and should be merged into the main Transaction entity"
            ),
            MergeDecision(
                entity1="Transaction",
                entity2="Payment",
                similarity=0.90,
                should_merge=True,
                merged_entity_name="Transaction",
                evidence=MergeDecisionEvidence(
                    definition_overlap="Transaction and Payment both represent financial transaction records, with Payment being a synonym for Transaction in this context"
                ),
                reasoning="Payment is a synonym for Transaction in the financial domain"
            ),
            MergeDecision(
                entity1="RiskProfile",
                entity2="FraudIndicator",
                similarity=0.75,
                should_merge=False,
                merged_entity_name=None,
                evidence=MergeDecisionEvidence(
                    definition_overlap="RiskProfile and FraudIndicator are related but distinct: RiskProfile is a dimension entity for classification, while FraudIndicator represents specific fraud signals",
                    counterexample="RiskProfile is a dimension table for risk classification, while FraudIndicator represents specific fraud detection signals - they serve different purposes"
                ),
                reasoning="RiskProfile and FraudIndicator are related but serve different purposes and should remain separate"
            )
        ],
        rename_suggestions=[],
        final_entities=["Transaction", "RiskProfile", "FraudIndicator"]
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.7"):
            result = await step_1_7_entity_consolidation(
                key_entities=key_entities,
                auxiliary_entities=auxiliary_entities,
                domain=domain,
                nl_description=nl_description
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["merge_decisions", "rename_suggestions", "final_entities"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "merge_decisions": list,
            "rename_suggestions": list,
            "final_entities": list
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.7")
    return all_valid


async def main():
    """Run all tests for Step 1.7."""
    results = []
    results.append(await test_step_1_7_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.7 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.7 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
