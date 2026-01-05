"""Unit tests for Step 3.1: ER Design Compilation."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase3.step_3_1_er_design_compilation import (
    step_3_1_er_design_compilation,
    ERDesignCompilationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


def test_step_3_1_basic():
    """Test Step 3.1 with basic input."""
    TestResultDisplay.print_test_header("ER Design Compilation", "3.1")
    TestResultDisplay.print_test_case(1, "Basic ER design compilation")
    
    entities = [
        {"name": "Customer", "description": "A customer entity"},
        {"name": "Order", "description": "An order entity"}
    ]
    relations = [
        {"entities": ["Customer", "Order"], "type": "one-to-many", "description": "Customer places orders"}
    ]
    attributes = {
        "Customer": [{"name": "customer_id"}, {"name": "name"}],
        "Order": [{"name": "order_id"}, {"name": "customer_id"}, {"name": "order_date"}]
    }
    primary_keys = {
        "Customer": ["customer_id"],
        "Order": ["order_id"]
    }
    foreign_keys = []
    constraints = []
    
    input_data = {
        "entities": entities,
        "relations": relations,
        "attributes": attributes,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
        "constraints": constraints
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_3_1_er_design_compilation(
        entities=entities,
        relations=relations,
        attributes=attributes,
        primary_keys=primary_keys,
        foreign_keys=foreign_keys,
        constraints=constraints
    )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["entities", "relations", "entity_attributes"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "entities": list,
            "relations": list,
            "entity_attributes": list
        }
    )
    validations.append(type_validation)
    
    # Validate entities structure
    entities_validation = {"valid": True, "errors": []}
    compiled_entities = result_dict.get("entities", [])
    if compiled_entities:
        for entity in compiled_entities:
            if hasattr(entity, 'model_dump'):
                entity = entity.model_dump()
            if not isinstance(entity, dict):
                entities_validation["valid"] = False
                entities_validation["errors"].append("Each entity must be a dictionary")
                continue
            if "name" not in entity or "attributes" not in entity or "primary_key" not in entity:
                entities_validation["valid"] = False
                entities_validation["errors"].append("Each entity must have 'name', 'attributes', and 'primary_key'")
    validations.append(entities_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "3.1")
    return all_valid


def main():
    """Run all tests for Step 3.1."""
    results = []
    results.append(test_step_3_1_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 3.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 3.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
