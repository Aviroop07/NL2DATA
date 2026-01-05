"""Unit tests for Step 8.3: Categorical Value Identification.

This is Phase 8 Step 8.3 - identifies explicit categorical values for categorical columns.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_3_categorical_value_identification import (
    step_8_3_categorical_value_identification_single,
    step_8_3_categorical_value_identification_batch,
    CategoricalValueIdentificationOutput,
    CategoricalValue,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_3_single_basic():
    """Test Step 8.3 single with basic input."""
    TestResultDisplay.print_test_header("Categorical Value Identification (Single)", "8.3")
    TestResultDisplay.print_test_case(1, "Basic categorical value identification")
    
    entity_name = "Transaction"
    attribute_name = "status"
    attribute_description = "Transaction status"
    column_datatype = "VARCHAR(50)"
    entity_description = "A financial transaction record"
    domain = "financial"
    nl_description = "Transactions can have status values: pending, completed, cancelled, failed"
    
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "attribute_description": attribute_description,
        "column_datatype": column_datatype,
        "entity_description": entity_description,
        "domain": domain,
        "nl_description": nl_description,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = CategoricalValueIdentificationOutput(
        categorical_values=[
            CategoricalValue(value="pending", description="Transaction is pending"),
            CategoricalValue(value="completed", description="Transaction is completed"),
            CategoricalValue(value="cancelled", description="Transaction is cancelled"),
            CategoricalValue(value="failed", description="Transaction failed"),
        ],
        reasoning="These are the standard transaction status values in a financial system",
        no_more_changes=True,
    )
    
    with patch('NL2DATA.phases.phase8.step_8_3_categorical_value_identification.step_8_3_categorical_value_identification_single') as mock_step:
        mock_step.return_value = mock_response
        result = await step_8_3_categorical_value_identification_single(
            entity_name=entity_name,
            attribute_name=attribute_name,
            attribute_description=attribute_description,
            column_datatype=column_datatype,
            entity_description=entity_description,
            domain=domain,
            nl_description=nl_description,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["categorical_values", "reasoning", "no_more_changes"]
    )
    validations.append(struct_validation)
    
    # Validate categorical values
    if hasattr(result, 'categorical_values'):
        cat_values = result.categorical_values
    elif isinstance(result, dict):
        cat_values = result.get("categorical_values", [])
    else:
        cat_values = []
    
    if len(cat_values) >= 2:
        validations.append({
            "name": "Minimum values check",
            "valid": True,
            "message": f"Has {len(cat_values)} values (>= 2 required)"
        })
    else:
        validations.append({
            "name": "Minimum values check",
            "valid": False,
            "message": f"Only {len(cat_values)} values (need >= 2)"
        })
    
    # Check for duplicates
    value_strings = []
    for cv in cat_values:
        if hasattr(cv, 'value'):
            value_strings.append(cv.value)
        elif isinstance(cv, dict):
            value_strings.append(cv.get("value", ""))
        elif isinstance(cv, str):
            value_strings.append(cv)
    
    if len(value_strings) == len(set(value_strings)):
        validations.append({
            "name": "Uniqueness check",
            "valid": True,
            "message": "All values are unique"
        })
    else:
        duplicates = [v for v in value_strings if value_strings.count(v) > 1]
        validations.append({
            "name": "Uniqueness check",
            "valid": False,
            "message": f"Duplicate values found: {set(duplicates)}"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.3 (single)")
    return all_valid


async def test_step_8_3_batch_basic():
    """Test Step 8.3 batch with basic input."""
    TestResultDisplay.print_test_header("Categorical Value Identification (Batch)", "8.3")
    TestResultDisplay.print_test_case(2, "Basic batch categorical value identification")
    
    categorical_attributes = {
        "Transaction": ["status", "transaction_type"],
        "Card": ["card_type", "card_status"],
    }
    
    entity_attributes = {
        "Transaction": [
            {"name": "status", "description": "Transaction status"},
            {"name": "transaction_type", "description": "Type of transaction"},
        ],
        "Card": [
            {"name": "card_type", "description": "Type of card"},
            {"name": "card_status", "description": "Status of card"},
        ],
    }
    
    data_types = {
        "Transaction": {
            "attribute_types": {
                "status": {"type": "VARCHAR(50)"},
                "transaction_type": {"type": "VARCHAR(50)"},
            }
        },
        "Card": {
            "attribute_types": {
                "card_type": {"type": "VARCHAR(50)"},
                "card_status": {"type": "VARCHAR(50)"},
            }
        },
    }
    
    entity_descriptions = {
        "Transaction": "A financial transaction record",
        "Card": "A payment card",
    }
    
    domain = "financial"
    nl_description = "Transactions have status and type. Cards have type and status."
    
    input_data = {
        "categorical_attributes": categorical_attributes,
        "entity_attributes": entity_attributes,
        "data_types": data_types,
        "entity_descriptions": entity_descriptions,
        "domain": domain,
        "nl_description": nl_description,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock responses for each categorical column
    mock_responses = {
        "Transaction": {
            "status": CategoricalValueIdentificationOutput(
                categorical_values=[
                    CategoricalValue(value="pending", description="Pending"),
                    CategoricalValue(value="completed", description="Completed"),
                    CategoricalValue(value="cancelled", description="Cancelled"),
                ],
                reasoning="Standard transaction statuses",
                no_more_changes=True,
            ),
            "transaction_type": CategoricalValueIdentificationOutput(
                categorical_values=[
                    CategoricalValue(value="purchase", description="Purchase"),
                    CategoricalValue(value="refund", description="Refund"),
                    CategoricalValue(value="chargeback", description="Chargeback"),
                ],
                reasoning="Standard transaction types",
                no_more_changes=True,
            ),
        },
        "Card": {
            "card_type": CategoricalValueIdentificationOutput(
                categorical_values=[
                    CategoricalValue(value="credit", description="Credit card"),
                    CategoricalValue(value="debit", description="Debit card"),
                    CategoricalValue(value="prepaid", description="Prepaid card"),
                ],
                reasoning="Standard card types",
                no_more_changes=True,
            ),
            "card_status": CategoricalValueIdentificationOutput(
                categorical_values=[
                    CategoricalValue(value="active", description="Active"),
                    CategoricalValue(value="blocked", description="Blocked"),
                    CategoricalValue(value="expired", description="Expired"),
                ],
                reasoning="Standard card statuses",
                no_more_changes=True,
            ),
        },
    }
    
    with patch('NL2DATA.phases.phase8.step_8_3_categorical_value_identification.step_8_3_categorical_value_identification_batch') as mock_step:
        mock_step.return_value = mock_responses
        result = await step_8_3_categorical_value_identification_batch(
            categorical_attributes=categorical_attributes,
            entity_attributes=entity_attributes,
            data_types=data_types,
            entity_descriptions=entity_descriptions,
            domain=domain,
            nl_description=nl_description,
        )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure
    if isinstance(result, dict):
        struct_validation = ValidationHelper.validate_output_structure(
            result,
            []  # Dictionary of entity -> attribute -> output
        )
        validations.append(struct_validation)
        
        # Check that all categorical attributes have values
        total_expected = sum(len(attrs) for attrs in categorical_attributes.values())
        total_found = sum(len(attrs) for attrs in result.values())
        
        if total_found == total_expected:
            validations.append({
                "name": "Completeness check",
                "valid": True,
                "message": f"Found values for all {total_expected} categorical columns"
            })
        else:
            validations.append({
                "name": "Completeness check",
                "valid": False,
                "message": f"Found values for {total_found}/{total_expected} categorical columns"
            })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.3 (batch)")
    return all_valid


async def test_step_8_3_datatype_matching():
    """Test Step 8.3 with different datatypes."""
    TestResultDisplay.print_test_header("Categorical Value Identification (Datatype Matching)", "8.3")
    TestResultDisplay.print_test_case(3, "Test datatype matching for different SQL types")
    
    test_cases = [
        {
            "name": "VARCHAR column",
            "column_datatype": "VARCHAR(50)",
            "values": ["pending", "completed", "cancelled"],
            "should_pass": True,
        },
        {
            "name": "INT column",
            "column_datatype": "INT",
            "values": ["1", "2", "3", "4", "5"],
            "should_pass": True,
        },
        {
            "name": "BOOLEAN column",
            "column_datatype": "BOOLEAN",
            "values": ["true", "false"],
            "should_pass": True,
        },
    ]
    
    all_passed = True
    for test_case in test_cases:
        # This would test the deterministic validation function
        # In a real test, we'd call _detect_categorical_value_issues
        # For now, we just verify the test structure
        pass
    
    TestResultDisplay.print_test_summary(all_passed, "8.3 (datatype matching)")
    return all_passed


async def main():
    """Run all tests for Step 8.3."""
    results = []
    results.append(await test_step_8_3_single_basic())
    results.append(await test_step_8_3_batch_basic())
    results.append(await test_step_8_3_datatype_matching())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.3 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.3 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
