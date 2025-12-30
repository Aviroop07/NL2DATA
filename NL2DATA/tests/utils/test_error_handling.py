"""Unit tests for error handling utilities."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.error_handling import (
    ErrorContext,
    StepError,
    handle_step_error,
    log_error_with_context,
    create_error_response,
)


class TestErrorContext:
    """Test ErrorContext dataclass."""
    
    def test_create_error_context(self):
        """Test creating error context."""
        context = ErrorContext(
            step_id="2.2",
            phase=2,
            entity_name="Customer"
        )
        
        assert context.step_id == "2.2"
        assert context.phase == 2
        assert context.entity_name == "Customer"
        assert context.relation_id is None
        assert context.additional_context == {}


class TestStepError:
    """Test StepError exception."""
    
    def test_create_step_error(self):
        """Test creating StepError."""
        context = ErrorContext(step_id="2.2", phase=2)
        error = StepError(
            message="Test error",
            context=context
        )
        
        assert str(error) == "[2.2] Test error"
        assert error.context == context
    
    def test_step_error_with_original_exception(self):
        """Test StepError with original exception."""
        context = ErrorContext(step_id="2.2", phase=2)
        original = ValueError("Original error")
        error = StepError(
            message="Wrapped error",
            context=context,
            original_exception=original
        )
        
        assert error.original_exception == original


class TestHandleStepError:
    """Test handle_step_error function."""
    
    def test_handle_step_error_no_raise(self):
        """Test handle_step_error without raising."""
        context = ErrorContext(step_id="2.2", phase=2, entity_name="Customer")
        error = ValueError("Test error")
        
        response = handle_step_error(
            error,
            context,
            return_partial=False,
            reraise=False
        )
        
        assert response is not None
        assert response["success"] is False
        assert response["error"]["step_id"] == "2.2"
        assert response["error"]["phase"] == 2
        assert response["error"]["entity_name"] == "Customer"
    
    def test_handle_step_error_raises(self):
        """Test handle_step_error with reraise=True."""
        context = ErrorContext(step_id="2.2", phase=2)
        error = ValueError("Test error")
        
        try:
            handle_step_error(
                error,
                context,
                reraise=True
            )
            assert False, "Should have raised StepError"
        except StepError:
            pass  # Expected


class TestCreateErrorResponse:
    """Test create_error_response function."""
    
    def test_create_error_response(self):
        """Test creating error response."""
        context = ErrorContext(
            step_id="2.2",
            phase=2,
            entity_name="Customer"
        )
        error = ValueError("Test error")
        
        response = create_error_response(error, context)
        
        assert response["success"] is False
        assert response["error"]["type"] == "ValueError"
        assert response["error"]["message"] == "Test error"
        assert response["error"]["step_id"] == "2.2"
        assert response["error"]["phase"] == 2
        assert response["error"]["entity_name"] == "Customer"
        assert "timestamp" in response["error"]
    
    def test_create_error_response_with_additional_context(self):
        """Test error response with additional context."""
        context = ErrorContext(
            step_id="2.2",
            phase=2,
            additional_context={"custom_field": "custom_value"}
        )
        error = ValueError("Test error")
        
        response = create_error_response(error, context)
        
        assert response["error"]["additional_context"]["custom_field"] == "custom_value"


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 80)
    print("Testing Error Handling Utilities")
    print("=" * 80)
    
    test_classes = [
        TestErrorContext,
        TestStepError,
        TestHandleStepError,
        TestCreateErrorResponse,
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

