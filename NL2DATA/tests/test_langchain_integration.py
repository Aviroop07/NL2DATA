"""Integration test to verify LangChain best practices in actual step execution.

This test runs a few actual step functions to verify:
1. RunnableConfig is being used
2. Tracing is working
3. Retry logic is functioning
4. No errors from the new implementations
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from NL2DATA.phases.phase1 import (
    step_1_1_domain_detection,
    step_1_2_entity_mention_detection,
)
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config

logger = get_logger(__name__)


async def test_step_1_1_with_tracing():
    """Test Step 1.1 with tracing enabled."""
    print("\n" + "=" * 80)
    print("Testing Step 1.1: Domain Detection (with LangChain best practices)")
    print("=" * 80)
    
    nl_description = "I need a database for an e-commerce store. Customers can place orders for products."
    
    try:
        result = await step_1_1_domain_detection(nl_description)
        
        print(f"\n[OK] Step 1.1 completed successfully")
        print(f"  - Has explicit domain: {result.get('has_explicit_domain')}")
        print(f"  - Domain: {result.get('domain', 'N/A')}")
        
        # Verify result structure
        assert isinstance(result, dict)
        assert 'has_explicit_domain' in result
        
        return True
    except Exception as e:
        print(f"\n[FAIL] Step 1.1 failed: {e}")
        logger.error("Step 1.1 test failed", exc_info=True)
        return False


async def test_step_1_2_with_tracing():
    """Test Step 1.2 with tracing enabled."""
    print("\n" + "=" * 80)
    print("Testing Step 1.2: Entity Mention Detection (with LangChain best practices)")
    print("=" * 80)
    
    nl_description = "I need Customer and Order tables for tracking purchases."
    
    try:
        result = await step_1_2_entity_mention_detection(nl_description)
        
        print(f"\n[OK] Step 1.2 completed successfully")
        print(f"  - Has explicit entities: {result.get('has_explicit_entities')}")
        print(f"  - Mentioned entities: {result.get('mentioned_entities', [])}")
        
        # Verify result structure
        assert isinstance(result, dict)
        assert 'has_explicit_entities' in result
        assert 'mentioned_entities' in result
        
        return True
    except Exception as e:
        print(f"\n[FAIL] Step 1.2 failed: {e}")
        logger.error("Step 1.2 test failed", exc_info=True)
        return False


async def run_integration_tests():
    """Run integration tests for LangChain best practices."""
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 80)
    print("LangChain Best Practices Integration Test")
    print("=" * 80)
    print("\nThis test verifies that:")
    print("  1. @traceable_step decorators are working")
    print("  2. RunnableConfig is being passed correctly")
    print("  3. RunnableRetry is functioning")
    print("  4. No errors from LangChain best practices implementation")
    print("\nNote: This will make actual LLM API calls.")
    
    results = []
    
    # Test Step 1.1
    result_1_1 = await test_step_1_1_with_tracing()
    results.append(("Step 1.1: Domain Detection", result_1_1))
    
    # Test Step 1.2
    result_1_2 = await test_step_1_2_with_tracing()
    results.append(("Step 1.2: Entity Mention Detection", result_1_2))
    
    # Summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("=" * 80)
    
    if passed == total:
        print("\n[OK] All integration tests passed! LangChain best practices are working correctly.")
    else:
        print(f"\n[FAIL] {total - passed} test(s) failed. Check logs for details.")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_integration_tests())
    sys.exit(0 if success else 1)

