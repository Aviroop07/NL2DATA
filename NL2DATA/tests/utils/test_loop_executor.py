"""Unit tests for loop executor."""

import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig


class TestSafeLoopExecutor:
    """Test SafeLoopExecutor class."""
    
    async def test_termination_by_condition(self):
        """Test loop terminates when condition is met."""
        executor = SafeLoopExecutor()
        
        iteration_count = 0
        
        async def step_func(previous_result=None):
            nonlocal iteration_count
            iteration_count += 1
            return {"value": iteration_count, "done": iteration_count >= 3}
        
        def termination_check(result):
            return result.get("done", False)
        
        config = LoopConfig(
            max_iterations=10,
            max_wall_time_sec=30,
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
    
    async def test_termination_by_max_iterations(self):
        """Test loop terminates at max iterations."""
        executor = SafeLoopExecutor()
        
        async def step_func(previous_result=None):
            return {"done": False}  # Never satisfies condition
        
        def termination_check(result):
            return result.get("done", False)
        
        config = LoopConfig(
            max_iterations=3,
            max_wall_time_sec=30,
            enable_cycle_detection=False
        )
        
        result = await executor.run_loop(
            step_func=step_func,
            termination_check=termination_check,
            config=config
        )
        
        assert result["terminated_by"] == "max_iterations"
        assert result["condition_met"] is False
        assert result["iterations"] == 3
    
    async def test_termination_by_timeout(self):
        """Test loop terminates on timeout."""
        executor = SafeLoopExecutor()
        
        async def step_func(previous_result=None):
            await asyncio.sleep(0.1)  # Slow step
            return {"done": False}
        
        def termination_check(result):
            return result.get("done", False)
        
        config = LoopConfig(
            max_iterations=100,
            max_wall_time_sec=1,  # Very short timeout
            enable_cycle_detection=False
        )
        
        result = await executor.run_loop(
            step_func=step_func,
            termination_check=termination_check,
            config=config
        )
        
        assert result["terminated_by"] == "timeout"
        assert result["condition_met"] is False
        assert result["iterations"] > 0
    
    async def test_oscillation_detection(self):
        """Test oscillation detection."""
        executor = SafeLoopExecutor()
        
        states = [{"value": 1}, {"value": 2}, {"value": 1}, {"value": 2}]
        state_index = 0
        
        async def step_func():
            nonlocal state_index
            result = states[state_index % len(states)]
            state_index += 1
            return result
        
        def termination_check(result):
            return False  # Never satisfies
        
        config = LoopConfig(
            max_iterations=10,
            max_wall_time_sec=30,
            enable_cycle_detection=True,
            oscillation_window=3
        )
        
        result = await executor.run_loop(
            step_func=step_func,
            termination_check=termination_check,
            config=config
        )
        
        # Should detect oscillation
        assert result["terminated_by"] == "oscillation"
        assert result["condition_met"] is False
    
    async def test_previous_result_passed(self):
        """Test that previous result is passed to step function."""
        executor = SafeLoopExecutor()
        
        previous_results = []
        
        async def step_func(previous_result=None):
            if previous_result:
                previous_results.append(previous_result)
            return {"value": len(previous_results), "done": len(previous_results) >= 2}
        
        def termination_check(result):
            return result.get("done", False)
        
        config = LoopConfig(
            max_iterations=10,
            max_wall_time_sec=30,
            enable_cycle_detection=False
        )
        
        result = await executor.run_loop(
            step_func=step_func,
            termination_check=termination_check,
            config=config
        )
        
        assert len(previous_results) >= 1  # Should have received previous results


async def run_all_tests():
    """Run all tests and report results."""
    print("=" * 80)
    print("Testing Loop Executor")
    print("=" * 80)
    
    test_class = TestSafeLoopExecutor
    print(f"\n{test_class.__name__}:")
    print("-" * 80)
    
    test_methods = [m for m in dir(test_class) if m.startswith("test_")]
    
    total_tests = 0
    passed_tests = 0
    
    for method_name in test_methods:
        total_tests += 1
        test_method = getattr(test_class(), method_name)
        try:
            await test_method()
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

