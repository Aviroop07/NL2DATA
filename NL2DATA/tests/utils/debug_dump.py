"""
Utilities for writing full, untruncated debug dumps to logs during test runs.

This is intentionally opt-in (via environment variable) because dumps can be large.
"""

from __future__ import annotations

import json
import os
from typing import Any


def debug_dump_enabled() -> bool:
    """
    Enable full debug dumps when NL2DATA_DEBUG_DUMP is truthy.

    Accepted truthy values: 1, true, yes, on (case-insensitive).
    """
    raw = os.getenv("NL2DATA_DEBUG_DUMP", "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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


def log_json(logger: Any, title: str, payload: Any) -> None:
    """
    Log a full, untruncated JSON dump of payload. Intended for test logs.
    """
    if not debug_dump_enabled():
        return

    logger.info("DEBUG_DUMP BEGIN: %s", title)
    logger.info("%s", json.dumps(payload, indent=2, ensure_ascii=False, default=_default_json))
    logger.info("DEBUG_DUMP END: %s", title)


