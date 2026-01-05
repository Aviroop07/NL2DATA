"""Unit tests for Step 3.2: Junction Table Naming."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase3.step_3_2_junction_table_naming import (
    step_3_2_junction_table_naming,
    JunctionTableNameOutput,
    JunctionTableNamingOutput
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_3_2_basic():
    """Test Step 3.2 with basic input."""
    TestResultDisplay.print_test_header("Junction Table Naming", "3.2")
    TestResultDisplay.print_test_case(1, "Basic junction table naming")
    
    relations = [
        {
            "entities": ["Card", "Merchant"],
            "type": "many-to-many",
            "description": "Cards transact with merchants, with fraud rings showing many cards transacting with the same merchant in short time windows"
        }
    ]
    entities = [
        {"name": "Card", "description": "A payment card used for transactions"},
        {"name": "Merchant", "description": "A merchant where transactions occur"}
    ]
    nl_description = "Fraud rings involve many cards transacting with the same merchant in a short time window. Cards can transact with multiple merchants, and merchants receive transactions from multiple cards. This many-to-many relationship is captured through the Transaction fact table."
    domain = "financial"
    input_data = {
        "relations": relations,
        "entities": entities,
        "nl_description": nl_description,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response for single junction table
    mock_response = JunctionTableNameOutput(
        table_name="card_merchant_transaction"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("3.2"):
            result = await step_3_2_junction_table_naming(
                relations=relations,
                entities=entities,
                nl_description=nl_description,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["junction_table_names"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {"junction_table_names": list}
    )
    validations.append(type_validation)
    
    # Validate junction table names structure
    junction_validation = {"valid": True, "errors": []}
    junction_table_names = result_dict.get("junction_table_names", [])
    if junction_table_names:
        for entry in junction_table_names:
            if hasattr(entry, 'model_dump'):
                entry = entry.model_dump()
            if not isinstance(entry, dict):
                junction_validation["valid"] = False
                junction_validation["errors"].append("Each junction table entry must be a dictionary")
                continue
            if "relation_key" not in entry or "table_name" not in entry:
                junction_validation["valid"] = False
                junction_validation["errors"].append("Each junction table entry must have 'relation_key' and 'table_name'")
    validations.append(junction_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "3.2")
    return all_valid


async def main():
    """Run all tests for Step 3.2."""
    results = []
    results.append(await test_step_3_2_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 3.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 3.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
