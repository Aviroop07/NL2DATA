"""Run all utility tests."""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.tests.utils import test_state_validation
from NL2DATA.tests.utils import test_error_handling
from NL2DATA.tests.utils import test_rate_limiting
from NL2DATA.tests.utils import test_loop_executor


async def run_all_tests():
    """Run all utility tests."""
    print("=" * 80)
    print("Running All Utility Tests")
    print("=" * 80)
    
    results = []
    
    # Run synchronous tests
    print("\n" + "=" * 80)
    print("1. State Validation Tests")
    print("=" * 80)
    try:
        success = test_state_validation.run_all_tests()
        results.append(("State Validation", success))
    except Exception as e:
        print(f"ERROR: {e}")
        results.append(("State Validation", False))
    
    print("\n" + "=" * 80)
    print("2. Error Handling Tests")
    print("=" * 80)
    try:
        success = test_error_handling.run_all_tests()
        results.append(("Error Handling", success))
    except Exception as e:
        print(f"ERROR: {e}")
        results.append(("Error Handling", False))
    
    # Run async tests
    print("\n" + "=" * 80)
    print("3. Rate Limiting Tests")
    print("=" * 80)
    try:
        success = await test_rate_limiting.run_all_tests()
        results.append(("Rate Limiting", success))
    except Exception as e:
        print(f"ERROR: {e}")
        results.append(("Rate Limiting", False))
    
    print("\n" + "=" * 80)
    print("4. Loop Executor Tests")
    print("=" * 80)
    try:
        success = await test_loop_executor.run_all_tests()
        results.append(("Loop Executor", success))
    except Exception as e:
        print(f"ERROR: {e}")
        results.append(("Loop Executor", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    
    for name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        print(f"{status} - {name}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "=" * 80)
    if all_passed:
        print("[SUCCESS] All tests passed!")
    else:
        print("[FAILURE] Some tests failed.")
    print("=" * 80)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

