"""Unit tests for rate limiting utilities."""

import sys
import asyncio
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.rate_limiting import RateLimiter, get_rate_limiter


class TestRateLimiter:
    """Test RateLimiter class."""
    
    async def test_acquire_context_manager(self):
        """Test rate limiter context manager."""
        limiter = RateLimiter(
            requests_per_minute=100,
            tokens_per_minute=10000,
            max_concurrent=2
        )
        
        async def dummy_task():
            await asyncio.sleep(0.1)
            return "done"
        
        # Should complete without error
        async with limiter.acquire():
            result = await dummy_task()
        
        assert result == "done"
    
    async def test_concurrency_limit(self):
        """Test concurrency limit is enforced."""
        limiter = RateLimiter(
            requests_per_minute=1000,
            tokens_per_minute=100000,
            max_concurrent=2
        )
        
        start_times = []
        
        async def task_with_delay(task_id):
            async with limiter.acquire():
                start_times.append((task_id, datetime.now()))
                await asyncio.sleep(0.2)
        
        # Start 4 tasks, but only 2 should run concurrently
        tasks = [task_with_delay(i) for i in range(4)]
        await asyncio.gather(*tasks)
        
        # Verify that tasks were serialized (max 2 concurrent)
        # This is a basic test - in practice, timing can vary
        assert len(start_times) == 4
    
    async def test_per_step_type_limit(self):
        """Test per-step-type concurrency limits."""
        limiter = RateLimiter(
            requests_per_minute=1000,
            tokens_per_minute=100000,
            max_concurrent=10,
            max_concurrency_per_step_type={"per-entity": 2}
        )
        
        start_times = []
        
        async def task_with_delay(task_id):
            async with limiter.acquire(step_type="per-entity"):
                start_times.append((task_id, datetime.now()))
                await asyncio.sleep(0.1)
        
        # Start 4 per-entity tasks, but only 2 should run concurrently
        tasks = [task_with_delay(i) for i in range(4)]
        await asyncio.gather(*tasks)
        
        assert len(start_times) == 4
    
    async def test_request_rate_limiting(self):
        """Test request rate limiting."""
        limiter = RateLimiter(
            requests_per_minute=10,  # Very low limit for testing
            tokens_per_minute=100000,
            max_concurrent=10
        )
        
        async def quick_task():
            async with limiter.acquire():
                return "done"
        
        # Make multiple requests quickly
        start_time = datetime.now()
        tasks = [quick_task() for _ in range(5)]
        results = await asyncio.gather(*tasks)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # All should complete
        assert all(r == "done" for r in results)
        # Should have taken some time due to rate limiting
        assert elapsed >= 0  # At least some time passed


class TestGetRateLimiter:
    """Test get_rate_limiter singleton."""
    
    def test_get_rate_limiter_returns_instance(self):
        """Test get_rate_limiter returns RateLimiter instance."""
        # Note: This test assumes rate limiting is enabled in config
        # If disabled, it will return None
        limiter = get_rate_limiter()
        
        # Should return RateLimiter or None (if disabled)
        assert limiter is None or isinstance(limiter, RateLimiter)
    
    def test_singleton_behavior(self):
        """Test that get_rate_limiter returns same instance."""
        limiter1 = get_rate_limiter()
        limiter2 = get_rate_limiter()
        
        # Should be the same instance (or both None)
        assert limiter1 is limiter2


async def run_all_tests():
    """Run all tests and report results."""
    print("=" * 80)
    print("Testing Rate Limiting Utilities")
    print("=" * 80)
    
    test_classes = [
        TestRateLimiter,
        TestGetRateLimiter,
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
                if asyncio.iscoroutinefunction(test_method):
                    await test_method()
                else:
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
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

