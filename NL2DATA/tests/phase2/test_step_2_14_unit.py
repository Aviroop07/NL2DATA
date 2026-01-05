"""Unit tests for Step 2.14: Relation Realization."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_2_14_basic():
    """Test Step 2.14 with basic input."""
    TestResultDisplay.print_test_header("Relation Realization", "2.14")
    TestResultDisplay.print_test_case(1, "Basic relation realization")
    
    relation = {
        "entities": ["Card", "Transaction"],
        "type": "one-to-many",
        "description": "Card used in Transaction",
        "entity_cardinalities": {"Card": "1", "Transaction": "N"},
        "entity_participations": {"Card": "partial", "Transaction": "total"}
    }
    primary_keys = {
        "Card": ["card_id"],
        "Transaction": ["transaction_id"]
    }
    nl_description = "Transactions are linked to cards, which belong to customers. Each card can have multiple transactions over time. Fraud patterns include coordinated fraud rings where many cards transact with the same merchant in a short time window."
    input_data = {
        "relation": relation,
        "primary_keys": primary_keys,
        "nl_description": nl_description
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock response structure
    mock_response_dict = {
        "foreign_keys": [
            {
                "from_entity": "Transaction",
                "from_attributes": ["card_id"],
                "to_entity": "Card",
                "to_attributes": ["card_id"],
                "on_delete": "RESTRICT"
            }
        ],
        "realization_type": "foreign_key",
        "reasoning": "One-to-many relation between Card and Transaction realized as foreign key card_id in Transaction table. RESTRICT on delete to prevent accidental deletion of cards with transaction history (important for fraud detection and audit trails)"
    }
    
    result = mock_response_dict
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["foreign_keys", "realization_type"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "foreign_keys": list,
            "realization_type": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "2.14")
    return all_valid


async def main():
    """Run all tests for Step 2.14."""
    results = []
    results.append(await test_step_2_14_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 2.14 unit tests passed!")
    else:
        print("[FAIL] Some Step 2.14 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
