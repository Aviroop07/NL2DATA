"""
Phase artifact dumping utilities.

Goal:
- After each phase, formally print + log the complete set of artifacts gathered so far.
- Keep output untruncated (full JSON) when debug dumping is enabled.

This is intentionally opt-in via NL2DATA_DEBUG_DUMP because artifacts can be large.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from NL2DATA.tests.utils.debug_dump import debug_dump_enabled, log_json


def _default_json(o: Any) -> Any:
    """
    Best-effort conversion for objects that aren't JSON-serializable.
    """
    if hasattr(o, "model_dump"):
        try:
            return o.model_dump()
        except Exception:
            return str(o)
    if hasattr(o, "dict"):
        try:
            return o.dict()
        except Exception:
            return str(o)
    return str(o)


def dump_phase_artifacts(*, logger: Any, phase: int, state: Dict[str, Any]) -> None:
    """
    Dump all artifacts gathered so far after a phase completes.

    - Always prints a short summary (keys present).
    - When NL2DATA_DEBUG_DUMP is enabled, also prints and logs the full JSON payload.
    """
    # Artifacts can be massive and noisy; keep *all* artifact dumping opt-in.
    if not debug_dump_enabled():
        return

    # Log full JSON (to file) using the existing debug dump mechanism.
    log_json(logger, f"Phase {phase} artifacts snapshot (state)", state)

    # Print full JSON (to console). Use ensure_ascii=True to avoid Windows console issues
    # with special Unicode characters; this keeps output plain ASCII.
    try:
        print("\n--- BEGIN PHASE ARTIFACTS JSON (state) ---")
        print(json.dumps(state, indent=2, ensure_ascii=True, default=_default_json))
        print("--- END PHASE ARTIFACTS JSON (state) ---")
    except Exception as e:
        # Fallback to string to avoid losing the dump entirely.
        print("\n--- BEGIN PHASE ARTIFACTS (state) FALLBACK STR ---")
        print(f"Error dumping JSON: {e}")
        print(str(state))
        print("--- END PHASE ARTIFACTS (state) FALLBACK STR ---")


