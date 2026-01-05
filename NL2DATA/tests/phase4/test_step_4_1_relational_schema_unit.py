"""Unit tests for Step 4.1: Relational Schema Compilation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase4.step_4_1_relational_schema_compilation import (
    step_4_1_relational_schema_compilation,
    RelationalSchemaCompilationOutput
)
from NL2DATA.tests.utils.test_helpers import (
    ValidationHelper,
    TestResultDisplay
)


def test_step_4_1_basic():
    """Test Step 4.1 with basic input."""
    TestResultDisplay.print_test_header("Relational Schema Compilation", "4.1")
    TestResultDisplay.print_test_case(1, "Basic relational schema compilation")
    
    er_design = {
        "entities": [
            {
                "name": "Customer",
                "description": "A customer entity",
                "attributes": [{"name": "customer_id"}, {"name": "name"}],
                "primary_key": ["customer_id"]
            },
            {
                "name": "Order",
                "description": "An order entity",
                "attributes": [{"name": "order_id"}, {"name": "customer_id"}, {"name": "order_date"}],
                "primary_key": ["order_id"]
            }
        ],
        "relations": [
            {
                "entities": ["Customer", "Order"],
                "type": "one-to-many",
                "description": "Customer places orders",
                "arity": 2
            }
        ],
        "attributes": {
            "Customer": [{"name": "customer_id"}, {"name": "name"}],
            "Order": [{"name": "order_id"}, {"name": "customer_id"}, {"name": "order_date"}]
        }
    }
    primary_keys = {
        "Customer": ["customer_id"],
        "Order": ["order_id"]
    }
    foreign_keys = []
    constraints = []
    junction_table_names = {}
    
    input_data = {
        "er_design": er_design,
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
        "constraints": constraints,
        "junction_table_names": junction_table_names
    }
    TestResultDisplay.print_input_summary(input_data)
    
    result = step_4_1_relational_schema_compilation(
        er_design=er_design,
        foreign_keys=foreign_keys,
        primary_keys=primary_keys,
        constraints=constraints,
        junction_table_names=junction_table_names
    )
    
    TestResultDisplay.print_output_summary(result)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["relational_schema"]
    )
    validations.append(struct_validation)
    
    # Validate relational schema structure
    schema_validation = {"valid": True, "errors": []}
    relational_schema = result_dict.get("relational_schema", {})
    if relational_schema:
        if hasattr(relational_schema, 'model_dump'):
            relational_schema = relational_schema.model_dump()
        if "tables" not in relational_schema:
            schema_validation["valid"] = False
            schema_validation["errors"].append("Relational schema must have 'tables' field")
        else:
            tables = relational_schema.get("tables", [])
            if not isinstance(tables, list):
                schema_validation["valid"] = False
                schema_validation["errors"].append("Tables must be a list")
            elif len(tables) == 0:
                schema_validation["valid"] = False
                schema_validation["errors"].append("Tables list should not be empty")
    validations.append(schema_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "4.1")
    return all_valid


def main():
    """Run all tests for Step 4.1."""
    results = []
    results.append(test_step_4_1_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 4.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 4.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
