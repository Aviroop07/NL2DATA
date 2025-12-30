"""Unit tests for state validation utilities."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.validation import (
    validate_state_consistency,
    validate_parallel_update_results,
    validate_no_duplicate_entities,
    validate_no_duplicate_attributes,
    check_state_consistency,
    StateValidationError,
)


class TestValidateStateConsistency:
    """Test state consistency validation."""
    
    def test_valid_state(self):
        """Test validation passes for valid state."""
        state = {
            "entities": [
                {"name": "Customer"},
                {"name": "Order"}
            ],
            "relations": [
                {"entities": ["Customer", "Order"]}
            ],
            "attributes": {
                "Customer": [{"name": "customer_id"}, {"name": "name"}],
                "Order": [{"name": "order_id"}, {"name": "customer_id"}]  # FK attribute in Order
            },
            "primary_keys": {
                "Customer": ["customer_id"],
                "Order": ["order_id"]
            },
            "foreign_keys": [
                {
                    "from_entity": "Order",
                    "to_entity": "Customer",
                    "attributes": ["customer_id"]
                }
            ]
        }
        
        issues = validate_state_consistency(state)
        assert len(issues) == 0
    
    def test_missing_entity_in_relation(self):
        """Test validation detects missing entity in relation."""
        state = {
            "entities": [{"name": "Customer"}],
            "relations": [
                {"entities": ["Customer", "NonExistent"]}
            ]
        }
        
        issues = validate_state_consistency(state)
        assert len(issues) > 0
        assert any("NonExistent" in issue for issue in issues)
    
    def test_missing_primary_key_attribute(self):
        """Test validation detects missing PK attribute."""
        state = {
            "entities": [{"name": "Customer"}],
            "attributes": {
                "Customer": [{"name": "name"}]
            },
            "primary_keys": {
                "Customer": ["customer_id"]  # Doesn't exist
            }
        }
        
        issues = validate_state_consistency(state)
        assert len(issues) > 0
        assert any("customer_id" in issue for issue in issues)
    
    def test_invalid_foreign_key_reference(self):
        """Test validation detects invalid FK reference."""
        state = {
            "entities": [
                {"name": "Customer"},
                {"name": "Order"}
            ],
            "attributes": {
                "Customer": [{"name": "customer_id"}],
                "Order": [{"name": "order_id"}]
            },
            "primary_keys": {
                "Customer": ["customer_id"]
            },
            "foreign_keys": [
                {
                    "from_entity": "Order",
                    "to_entity": "NonExistent",  # Doesn't exist
                    "attributes": ["customer_id"]
                }
            ]
        }
        
        issues = validate_state_consistency(state)
        assert len(issues) > 0
        assert any("NonExistent" in issue for issue in issues)
    
    def test_entity_without_attributes(self):
        """Test validation detects entity without attributes."""
        state = {
            "entities": [{"name": "Customer"}],
            "attributes": {"Customer": []}  # Empty attributes list (after Phase 2)
        }
        
        issues = validate_state_consistency(state)
        # Should warn about missing attributes
        assert any("no attributes" in issue.lower() for issue in issues)


class TestValidateParallelUpdateResults:
    """Test parallel update results validation."""
    
    def test_all_entities_have_results(self):
        """Test validation passes when all entities have results."""
        results = [
            {"entity": "Customer", "attributes": []},
            {"entity": "Order", "attributes": []}
        ]
        expected_keys = ["entity", "attributes"]
        entity_names = ["Customer", "Order"]
        
        issues = validate_parallel_update_results(results, expected_keys, entity_names)
        assert len(issues) == 0
    
    def test_missing_entity_result(self):
        """Test validation detects missing entity result."""
        results = [
            {"entity": "Customer", "attributes": []}
        ]
        expected_keys = ["entity", "attributes"]
        entity_names = ["Customer", "Order"]  # Order missing
        
        issues = validate_parallel_update_results(results, expected_keys, entity_names)
        assert len(issues) > 0
        assert any("Order" in issue for issue in issues)
    
    def test_missing_expected_keys(self):
        """Test validation detects missing expected keys."""
        results = [
            {"entity": "Customer"}  # Missing "attributes" key
        ]
        expected_keys = ["entity", "attributes"]
        entity_names = ["Customer"]
        
        issues = validate_parallel_update_results(results, expected_keys, entity_names)
        assert len(issues) > 0
        assert any("attributes" in issue for issue in issues)


class TestValidateNoDuplicateEntities:
    """Test duplicate entity validation."""
    
    def test_no_duplicates(self):
        """Test validation passes with no duplicates."""
        entities = [
            {"name": "Customer"},
            {"name": "Order"}
        ]
        
        issues = validate_no_duplicate_entities(entities)
        assert len(issues) == 0
    
    def test_duplicate_entities(self):
        """Test validation detects duplicate entities."""
        entities = [
            {"name": "Customer"},
            {"name": "Customer"}  # Duplicate
        ]
        
        issues = validate_no_duplicate_entities(entities)
        assert len(issues) > 0
        assert any("Customer" in issue for issue in issues)


class TestValidateNoDuplicateAttributes:
    """Test duplicate attribute validation."""
    
    def test_no_duplicates(self):
        """Test validation passes with no duplicates."""
        attributes = {
            "Customer": [
                {"name": "customer_id"},
                {"name": "name"}
            ]
        }
        
        issues = validate_no_duplicate_attributes(attributes)
        assert len(issues) == 0
    
    def test_duplicate_attributes(self):
        """Test validation detects duplicate attributes."""
        attributes = {
            "Customer": [
                {"name": "name"},
                {"name": "name"}  # Duplicate
            ]
        }
        
        issues = validate_no_duplicate_attributes(attributes)
        assert len(issues) > 0
        assert any("name" in issue and "Customer" in issue for issue in issues)


class TestCheckStateConsistency:
    """Test check_state_consistency convenience function."""
    
    def test_valid_state_no_raise(self):
        """Test valid state doesn't raise when raise_on_error=False."""
        state = {
            "entities": [{"name": "Customer"}],
            "relations": []
        }
        
        result = check_state_consistency(state, raise_on_error=False)
        assert result is True
    
    def test_invalid_state_no_raise(self):
        """Test invalid state returns False when raise_on_error=False."""
        state = {
            "entities": [],
            "relations": [
                {"entities": ["NonExistent"]}
            ]
        }
        
        result = check_state_consistency(state, raise_on_error=False)
        assert result is False
    
    def test_invalid_state_raises(self):
        """Test invalid state raises when raise_on_error=True."""
        state = {
            "entities": [],
            "relations": [
                {"entities": ["NonExistent"]}
            ]
        }
        
        try:
            check_state_consistency(state, raise_on_error=True)
            assert False, "Should have raised StateValidationError"
        except StateValidationError:
            pass  # Expected


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 80)
    print("Testing State Validation Utilities")
    print("=" * 80)
    
    test_classes = [
        TestValidateStateConsistency,
        TestValidateParallelUpdateResults,
        TestValidateNoDuplicateEntities,
        TestValidateNoDuplicateAttributes,
        TestCheckStateConsistency,
    ]
    
    total_tests = 0
    passed_tests = 0
    
    for test_class in test_classes:
        class_name = test_class.__name__
        print(f"\n{class_name}:")
        print("-" * 80)
        
        test_methods = [m for m in dir(test_class) if m.startswith("test_")]
        
        for method_name in test_methods:
            total_tests += 1
            test_method = getattr(test_class(), method_name)
            try:
                test_method()
                print(f"  [PASS] {method_name}")
                passed_tests += 1
            except AssertionError as e:
                print(f"  [FAIL] {method_name}: {e}")
            except Exception as e:
                print(f"  [ERROR] {method_name}: {e}")
    
    print("\n" + "=" * 80)
    print(f"Test Results: {passed_tests}/{total_tests} passed")
    print("=" * 80)
    
    return passed_tests == total_tests


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)

