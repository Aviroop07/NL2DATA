"""Unit tests for Step 5.2: Independent Attribute Data Types."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5.step_5_2_independent_attribute_data_types import (
    step_5_2_independent_attribute_data_types
)
from NL2DATA.utils.data_types.type_assignment import (
    DataTypeAssignmentOutput,
    AttributeTypeInfo
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_5_2_basic():
    """Test Step 5.2 with basic input."""
    TestResultDisplay.print_test_header("Independent Attribute Data Types", "5.2")
    TestResultDisplay.print_test_case(1, "Basic independent attribute type assignment")
    
    entity_name = "Transaction"
    attribute_name = "amount_usd"
    attributes = {
        "Transaction": [
            {"name": "transaction_id", "description": "Transaction identifier", "type_hint": "integer"},
            {"name": "amount_usd", "description": "Transaction amount in USD, following log-normal distribution with heavy tail", "type_hint": "decimal"},
            {"name": "exchange_rate", "description": "Currency exchange rate (0.1-10.0 range)", "type_hint": "decimal"},
            {"name": "transaction_datetime", "description": "Transaction timestamp", "type_hint": "timestamp"}
        ]
    }
    primary_keys = {"Transaction": ["transaction_id"]}
    domain = "financial"
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "attributes": attributes,
        "primary_keys": primary_keys,
        "domain": domain
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = DataTypeAssignmentOutput(
        attribute_types={
            "amount_usd": AttributeTypeInfo(
                type="DECIMAL",
                precision=12,
                scale=2,
                reasoning="Transaction amounts in USD follow log-normal distribution with heavy tail, requiring DECIMAL(12,2) to accommodate large values (up to billions) with 2 decimal places for currency precision"
            )
        }
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("5.2"):
            result = await step_5_2_independent_attribute_data_types(
                entity_name=entity_name,
                attribute_name=attribute_name,
                attributes=attributes,
                primary_keys=primary_keys,
                domain=domain
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["data_types"]
    )
    validations.append(struct_validation)
    
    # Convert to dict for validation
    result_dict = ValidationHelper._to_dict(result)
    data_types = result_dict.get("data_types", [])
    
    type_validation = {"valid": True, "errors": []}
    if not isinstance(data_types, list):
        type_validation["valid"] = False
        type_validation["errors"].append(
            f"data_types must be a list, got {type(data_types).__name__}"
        )
    validations.append(type_validation)
    
    attr_validation = {"valid": True, "errors": []}
    if not isinstance(data_types, list) or len(data_types) == 0:
        attr_validation["valid"] = False
        attr_validation["errors"].append(
            "data_types must be a non-empty list"
        )
    else:
        expected_key = f"{entity_name}.{attribute_name}"
        found = False
        for assignment in data_types:
            # Handle Pydantic models in the list
            if hasattr(assignment, 'model_dump'):
                assignment = assignment.model_dump()
            if isinstance(assignment, dict) and assignment.get("attribute_key") == expected_key:
                found = True
                # Validate type_info structure
                type_info = assignment.get("type_info", {})
                if hasattr(type_info, 'model_dump'):
                    type_info = type_info.model_dump()
                if not isinstance(type_info, dict):
                    attr_validation["valid"] = False
                    attr_validation["errors"].append(
                        f"type_info for {expected_key} must be a dict or AttributeTypeInfo"
                    )
                elif "type" not in type_info:
                    attr_validation["valid"] = False
                    attr_validation["errors"].append(
                        f"type_info for {expected_key} must have a 'type' field"
                    )
                break
        if not found:
            attr_validation["valid"] = False
            attr_validation["errors"].append(
                f"data_types must contain an assignment with attribute_key '{expected_key}'"
            )
    validations.append(attr_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "5.2")
    return all_valid


async def main():
    """Run all tests for Step 5.2."""
    results = []
    results.append(await test_step_5_2_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 5.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 5.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
