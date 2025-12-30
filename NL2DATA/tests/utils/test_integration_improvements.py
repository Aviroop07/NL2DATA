"""Integration test for improvements: rate limiting, state validation, error handling."""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.validation import check_state_consistency
from NL2DATA.utils.rate_limiting import get_rate_limiter
from NL2DATA.utils.error_handling import handle_step_error, ErrorContext
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig


async def test_rate_limiting_integration():
    """Test rate limiting is properly initialized and accessible."""
    print("\nTesting Rate Limiting Integration...")
    
    limiter = get_rate_limiter()
    
    if limiter is None:
        print("  [SKIP] Rate limiting disabled in config")
        return True
    
    # Test that we can acquire permits
    async with limiter.acquire(step_type="per-entity", estimated_tokens=1000):
        await asyncio.sleep(0.01)  # Simulate API call
    
    print("  [PASS] Rate limiter accessible and working")
    return True


def test_state_validation_integration():
    """Test state validation works with real state structure."""
    print("\nTesting State Validation Integration...")
    
    # Create a realistic state
    state = {
        "entities": [
            {"name": "Customer", "description": "A customer"},
            {"name": "Order", "description": "An order"}
        ],
        "relations": [
            {"entities": ["Customer", "Order"], "type": "one-to-many"}
        ],
        "attributes": {
            "Customer": [{"name": "customer_id"}, {"name": "name"}],
            "Order": [{"name": "order_id"}, {"name": "customer_id"}]
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
    
    # Should pass validation
    result = check_state_consistency(state, raise_on_error=False)
    assert result is True, "Valid state should pass validation"
    
    # Test invalid state
    invalid_state = {
        "entities": [{"name": "Customer"}],
        "relations": [
            {"entities": ["Customer", "NonExistent"]}
        ]
    }
    
    result = check_state_consistency(invalid_state, raise_on_error=False)
    assert result is False, "Invalid state should fail validation"
    
    print("  [PASS] State validation works correctly")
    return True


def test_error_handling_integration():
    """Test error handling works with real errors."""
    print("\nTesting Error Handling Integration...")
    
    context = ErrorContext(
        step_id="2.2",
        phase=2,
        entity_name="Customer"
    )
    
    # Test with a real error
    try:
        raise ValueError("Test error for integration test")
    except Exception as e:
        response = handle_step_error(
            e,
            context,
            return_partial=False,
            reraise=False
        )
        
        assert response["success"] is False
        assert response["error"]["step_id"] == "2.2"
        assert response["error"]["phase"] == 2
        assert response["error"]["entity_name"] == "Customer"
    
    print("  [PASS] Error handling works correctly")
    return True


async def test_loop_executor_integration():
    """Test loop executor with realistic scenario."""
    print("\nTesting Loop Executor Integration...")
    
    executor = SafeLoopExecutor()
    
    iteration_count = 0
    
    async def step_func(previous_result=None):
        nonlocal iteration_count
        iteration_count += 1
        # Simulate iterative refinement
        return {
            "value": iteration_count,
            "satisfied": iteration_count >= 3,
            "missing_components": [] if iteration_count >= 3 else ["component1"]
        }
    
    def termination_check(result):
        return result.get("satisfied", False)
    
    config = LoopConfig(
        max_iterations=5,
        max_wall_time_sec=10,
        enable_cycle_detection=False
    )
    
    result = await executor.run_loop(
        step_func=step_func,
        termination_check=termination_check,
        config=config
    )
    
    assert result["terminated_by"] == "condition_met"
    assert result["condition_met"] is True
    assert result["iterations"] == 3
    assert iteration_count == 3
    
    print("  [PASS] Loop executor works correctly")
    return True


async def run_integration_tests():
    """Run all integration tests."""
    print("=" * 80)
    print("Integration Tests for Improvements")
    print("=" * 80)
    
    results = []
    
    try:
        success = await test_rate_limiting_integration()
        results.append(("Rate Limiting", success))
    except Exception as e:
        print(f"  [FAIL] Rate limiting integration: {e}")
        results.append(("Rate Limiting", False))
    
    try:
        success = test_state_validation_integration()
        results.append(("State Validation", success))
    except Exception as e:
        print(f"  [FAIL] State validation integration: {e}")
        results.append(("State Validation", False))
    
    try:
        success = test_error_handling_integration()
        results.append(("Error Handling", success))
    except Exception as e:
        print(f"  [FAIL] Error handling integration: {e}")
        results.append(("Error Handling", False))
    
    try:
        success = await test_loop_executor_integration()
        results.append(("Loop Executor", success))
    except Exception as e:
        print(f"  [FAIL] Loop executor integration: {e}")
        results.append(("Loop Executor", False))
    
    print("\n" + "=" * 80)
    print("Integration Test Summary")
    print("=" * 80)
    
    for name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        print(f"{status} - {name}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "=" * 80)
    if all_passed:
        print("[SUCCESS] All integration tests passed!")
    else:
        print("[FAILURE] Some integration tests failed.")
    print("=" * 80)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(run_integration_tests())
    sys.exit(0 if success else 1)

