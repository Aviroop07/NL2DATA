from NL2DATA.orchestration.phase_gates.validators import (
    validate_derived_formula_dependencies_match_formula,
)


def test_phase2_derived_formula_dependencies_extracted_and_compared():
    attributes = {
        "Order": [{"name": "quantity"}, {"name": "unit_price"}, {"name": "total_price"}],
        "Customer": [{"name": "name"}],
    }
    derived_formulas = {
        "Order.total_price": {
            "formula": "quantity * unit_price",
            "dependencies": ["quantity", "unit_price"],
        }
    }
    issues = validate_derived_formula_dependencies_match_formula(attributes, derived_formulas)
    assert issues == []


def test_phase2_derived_formula_rejects_out_of_entity_identifier():
    attributes = {
        "Order": [{"name": "quantity"}, {"name": "total_price"}],
        "Product": [{"name": "unit_price"}],
    }
    derived_formulas = {
        # unit_price is not an Order attribute -> should fail (naked identifier must be entity-local)
        "Order.total_price": {
            "formula": "quantity * unit_price",
            "dependencies": ["quantity", "unit_price"],
        }
    }
    issues = validate_derived_formula_dependencies_match_formula(attributes, derived_formulas)
    assert any("not in entity 'Order'" in s for s in issues), issues


def test_phase2_derived_formula_dependency_mismatch_detected():
    attributes = {"Order": [{"name": "a"}, {"name": "b"}, {"name": "c"}]}
    derived_formulas = {
        "Order.c": {
            "formula": "a + b",
            "dependencies": ["a"],  # missing b
        }
    }
    issues = validate_derived_formula_dependencies_match_formula(attributes, derived_formulas)
    assert any("missing identifiers used in formula" in s for s in issues), issues
