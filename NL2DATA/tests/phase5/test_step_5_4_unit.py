"""Unit tests for Step 5.4: Dependent Attribute Data Types."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase5.step_5_4_dependent_attribute_data_types import (
    step_5_4_dependent_attribute_data_types
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


async def test_step_5_4_basic():
    """Test Step 5.4 with basic input."""
    TestResultDisplay.print_test_header("Dependent Attribute Data Types", "5.4")
    TestResultDisplay.print_test_case(1, "Basic dependent attribute type assignment")
    
    entity_name = "Transaction"
    attribute_name = "fraud_risk_score"
    attributes = {
        "Transaction": [
            {"name": "base_risk_score", "description": "Base risk score"},
            {"name": "is_cross_border", "description": "Cross-border flag"},
            {"name": "high_risk_mcc_flag", "description": "High-risk MCC flag"},
            {"name": "amount_usd", "description": "Transaction amount"},
            {"name": "velocity_score", "description": "Velocity score"},
            {"name": "fraud_risk_score", "description": "Combined fraud risk score"}
        ]
    }
    dependency_graph = {
        "Transaction.fraud_risk_score": [
            "Transaction.base_risk_score", "Transaction.is_cross_border",
            "Transaction.high_risk_mcc_flag", "Transaction.amount_usd", "Transaction.velocity_score"
        ]
    }
    fk_dependencies = {}
    derived_dependencies = {
        "Transaction.fraud_risk_score": [
            "Transaction.base_risk_score", "Transaction.is_cross_border",
            "Transaction.high_risk_mcc_flag", "Transaction.amount_usd", "Transaction.velocity_score"
        ]
    }
    independent_types = {
        "Transaction.base_risk_score": {"type": "DECIMAL", "precision": 5, "scale": 2},
        "Transaction.is_cross_border": {"type": "BOOLEAN"},
        "Transaction.high_risk_mcc_flag": {"type": "BOOLEAN"},
        "Transaction.amount_usd": {"type": "DECIMAL", "precision": 12, "scale": 2},
        "Transaction.velocity_score": {"type": "DECIMAL", "precision": 5, "scale": 2}
    }
    fk_types = {}
    input_data = {
        "entity_name": entity_name,
        "attribute_name": attribute_name,
        "attributes": attributes,
        "dependency_graph": dependency_graph,
        "fk_dependencies": fk_dependencies,
        "derived_dependencies": derived_dependencies,
        "independent_types": independent_types,
        "fk_types": fk_types
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = DataTypeAssignmentOutput(
        attribute_types={
            "fraud_risk_score": AttributeTypeInfo(
                type="DECIMAL",
                precision=5,
                scale=2,
                reasoning="Fraud risk score is derived from multiple DECIMAL and BOOLEAN inputs, should be DECIMAL(5,2) to accommodate range 0.0-100.0"
            )
        }
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("5.4"):
            result = await step_5_4_dependent_attribute_data_types(
                entity_name=entity_name,
                attribute_name=attribute_name,
                attributes=attributes,
                dependency_graph=dependency_graph,
                fk_dependencies=fk_dependencies,
                derived_dependencies=derived_dependencies,
                independent_types=independent_types,
                fk_types=fk_types
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
        # Check that the list contains AttributeTypeAssignment objects
        for assignment in data_types:
            # Handle Pydantic models in the list
            if hasattr(assignment, 'model_dump'):
                assignment = assignment.model_dump()
            if not isinstance(assignment, dict):
                attr_validation["valid"] = False
                attr_validation["errors"].append(
                    "Each assignment in data_types must be a dict or AttributeTypeAssignment"
                )
                continue
            if "attribute_key" not in assignment:
                attr_validation["valid"] = False
                attr_validation["errors"].append(
                    "Each assignment must have an 'attribute_key' field"
                )
            if "type_info" not in assignment:
                attr_validation["valid"] = False
                attr_validation["errors"].append(
                    "Each assignment must have a 'type_info' field"
                )
    validations.append(attr_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "5.4")
    return all_valid


async def main():
    """Run all tests for Step 5.4."""
    results = []
    results.append(await test_step_5_4_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 5.4 unit tests passed!")
    else:
        print("[FAIL] Some Step 5.4 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
