"""Safe loop executor with guardrails for iterative refinement."""

from dataclasses import dataclass
from typing import Callable, Any, Optional
import hashlib
import json
import asyncio
from datetime import datetime

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class LoopConfig:
    """Configuration for iterative refinement loops."""
    max_iterations: int
    max_wall_time_sec: int
    oscillation_window: int = 3  # Detect if same state repeats
    enable_cycle_detection: bool = True


class SafeLoopExecutor:
    """Execute iterative loops with safety guarantees."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    async def run_loop(
        self,
        step_func: Callable,
        termination_check: Callable[[Any], bool],
        config: LoopConfig,
        *args,
        **kwargs
    ) -> dict:
        """
        Run loop with max iterations, timeout, and cycle detection.
        
        Args:
            step_func: The step function to call in each iteration
            termination_check: Function that returns True when loop should terminate
            config: Loop configuration with safety limits
            *args, **kwargs: Arguments to pass to step_func
            
        Returns:
            dict with result, iterations, and terminated_by reason
        """
        history = []
        state_hashes = []
        start_time = datetime.now()
        
        try:
            async with asyncio.timeout(config.max_wall_time_sec):
                for iteration in range(config.max_iterations):
                    iteration_start = datetime.now()
                    
                    # Call step function with previous result if available
                    # Check if step_func accepts previous_result parameter
                    import inspect
                    sig = inspect.signature(step_func)
                    step_kwargs = kwargs.copy()
                    if history and "previous_result" in sig.parameters:
                        step_kwargs["previous_result"] = history[-1]
                    
                    result = await step_func(*args, **step_kwargs)
                    history.append(result)
                    
                    elapsed = (datetime.now() - iteration_start).total_seconds()
                    self.logger.debug(f"Loop iteration {iteration + 1}/{config.max_iterations} completed in {elapsed:.2f}s")
                    
                    # Check termination condition
                    if termination_check(result):
                        total_time = (datetime.now() - start_time).total_seconds()
                        self.logger.info(
                            f"Loop terminated by condition after {iteration + 1} iterations "
                            f"({total_time:.2f}s total)"
                        )
                        return {
                            "result": result,
                            "iterations": iteration + 1,
                            "terminated_by": "condition_met",
                            "history": history,
                            "condition_met": True  # Indicate condition was met
                        }
                    
                    # Cycle detection
                    if config.enable_cycle_detection:
                        state_hash = self._hash_state(result)
                        if state_hash in state_hashes[-config.oscillation_window:]:
                            total_time = (datetime.now() - start_time).total_seconds()
                            self.logger.warning(
                                f"Loop terminated by oscillation detection after {iteration + 1} iterations "
                                f"({total_time:.2f}s total)"
                            )
                        return {
                            "result": result,
                            "iterations": iteration + 1,
                            "terminated_by": "oscillation",
                            "history": history,
                            "condition_met": False  # Indicate condition was not met
                        }
                        state_hashes.append(state_hash)
                
                # Max iterations reached
                total_time = (datetime.now() - start_time).total_seconds()
                last_result = history[-1] if history else None
                self.logger.warning(
                    f"Loop terminated by max iterations ({config.max_iterations}) "
                    f"({total_time:.2f}s total). "
                    f"Termination condition may not have been met."
                )
                return {
                    "result": last_result,
                    "iterations": config.max_iterations,
                    "terminated_by": "max_iterations",
                    "history": history,
                    "condition_met": False  # Indicate condition was not met
                }
        except asyncio.TimeoutError:
            total_time = (datetime.now() - start_time).total_seconds()
            last_result = history[-1] if history else None
            self.logger.warning(
                f"Loop terminated by timeout ({config.max_wall_time_sec}s) "
                f"after {len(history)} iterations ({total_time:.2f}s elapsed). "
                f"Termination condition may not have been met."
            )
            return {
                "result": last_result,
                "iterations": len(history),
                "terminated_by": "timeout",
                "history": history,
                "condition_met": False  # Indicate condition was not met
            }
    
    def _hash_state(self, state: Any) -> str:
        """Create canonical hash of state for cycle detection."""
        normalized = self._canonicalize_state(state)
        serialized = json.dumps(normalized, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()
    
    def _canonicalize_state(self, state: Any) -> Any:
        """
        Normalize state for comparison (case-fold, sort, stable IDs, etc.).
        
        Canonicalization rules:
        - Textual lists: normalize whitespace, lowercase, sort by stable ID
        - Schema objects: diff on stable IDs (entity_id, table_id, column_id) and structured fields
        - Remove None values
        - Sort all collections
        """
        if isinstance(state, dict):
            # Use stable IDs if available, otherwise use keys
            if any(k.endswith('_id') for k in state.keys()):
                # Schema object: preserve stable IDs, canonicalize other fields
                canonical = {}
                for k, v in sorted(state.items()):
                    if v is not None:
                        if k.endswith('_id'):
                            canonical[k] = v  # Preserve stable IDs as-is
                        else:
                            canonical[k] = self._canonicalize_state(v)
                return canonical
            else:
                return {k: self._canonicalize_state(v) for k, v in sorted(state.items()) if v is not None}
        elif isinstance(state, list):
            # Sort by stable ID if available, otherwise by canonicalized value
            if state and isinstance(state[0], dict):
                if any('id' in state[0] or 'entity_id' in state[0] or 'table_id' in state[0] or 'column_id' in state[0] for _ in [state[0]]):
                    # Sort by stable ID
                    return sorted(
                        [self._canonicalize_state(item) for item in state],
                        key=lambda x: x.get('id') or x.get('entity_id') or x.get('table_id') or x.get('column_id', '')
                    )
            # For lists of dictionaries without IDs, sort by a stable representation
            canonicalized = [self._canonicalize_state(item) for item in state]
            # If items are dictionaries, use JSON string as sort key
            if canonicalized and isinstance(canonicalized[0], dict):
                return sorted(canonicalized, key=lambda x: json.dumps(x, sort_keys=True, default=str))
            # For other types (strings, numbers, etc.), direct sort works
            return sorted(canonicalized)
        elif isinstance(state, str):
            # Normalize whitespace, lowercase
            return ' '.join(state.lower().strip().split())
        return state

