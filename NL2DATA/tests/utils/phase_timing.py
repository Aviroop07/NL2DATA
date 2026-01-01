"""Utilities for timing phases in the integration harness (deterministic)."""

from __future__ import annotations

from time import perf_counter
from typing import Optional


def timer_start() -> float:
    """Return a high-resolution start timestamp."""
    return perf_counter()


def timer_elapsed_seconds(start_ts: float) -> float:
    """Return elapsed seconds since `start_ts` (from `timer_start()`)."""
    return float(perf_counter() - float(start_ts))


def log_phase_duration(logger, *, phase: int, seconds: float, label: Optional[str] = None) -> None:
    """
    Log phase duration in a consistent, grep-friendly format.

    Note: This intentionally does not truncate or hide values.
    """
    name = f"Phase {phase}" + (f" ({label})" if label else "")
    logger.info(f"PHASE_TIMING {name}: {seconds:.3f} seconds")


