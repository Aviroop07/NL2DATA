"""Centralized, env-backed configuration for pipeline behavior.

Keep this lightweight and dependency-free so it can be imported from any phase/step.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        value = default
    else:
        try:
            value = int(str(raw).strip())
        except Exception:
            value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _get_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        value = default
    else:
        try:
            value = float(str(raw).strip())
        except Exception:
            value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = str(raw).strip()
    return s if s else default


@dataclass(frozen=True)
class Phase2Config:
    """Phase 2 tuning knobs.

    These are intentionally coarse-grained so it's easy to tweak behavior via env vars.
    """

    # Step 2.2 intrinsic attribute "revise until clean" loop
    step_2_2_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_2_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 2.3 synonym validation/retry loop
    step_2_3_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_3_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 2.8 multivalued/derived detection revision loop
    step_2_8_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_8_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 2.7 primary key identification revision loop
    step_2_7_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_7_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Context controls: do NOT always pass full NL to every step (reduces global leakage)
    step_2_7_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_7_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_2_8_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_8_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)

    # Phase 2: other steps where NL is frequently "global poison" (constraints steps should not need full NL)
    step_2_4_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_4_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_2_10_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_10_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_2_11_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_11_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_2_12_include_nl_context: int = _get_int("NL2DATA_PHASE2_STEP_2_12_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)

    # Step 2.3 candidate generation (deterministic semantic similarity)
    step_2_3_similarity_enabled: int = _get_int("NL2DATA_PHASE2_STEP_2_3_SIMILARITY_ENABLED", 1, min_value=0, max_value=1)
    step_2_3_similarity_model_name: str = _get_str("NL2DATA_PHASE2_STEP_2_3_SIMILARITY_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
    step_2_3_similarity_threshold: float = _get_float("NL2DATA_PHASE2_STEP_2_3_SIMILARITY_THRESHOLD", 0.82, min_value=0.0, max_value=1.0)
    step_2_3_similarity_max_pairs: int = _get_int("NL2DATA_PHASE2_STEP_2_3_SIMILARITY_MAX_PAIRS", 20, min_value=0, max_value=200)
    # Hybrid gating knobs (reduce false positives)
    step_2_3_similarity_lexical_min_jaccard: float = _get_float(
        "NL2DATA_PHASE2_STEP_2_3_SIMILARITY_LEXICAL_MIN_JACCARD", 0.20, min_value=0.0, max_value=1.0
    )
    step_2_3_similarity_filter_description_pairs: int = _get_int(
        "NL2DATA_PHASE2_STEP_2_3_SIMILARITY_FILTER_DESCRIPTION_PAIRS", 1, min_value=0, max_value=1
    )
    step_2_3_similarity_filter_id_vs_non_id: int = _get_int(
        "NL2DATA_PHASE2_STEP_2_3_SIMILARITY_FILTER_ID_VS_NON_ID", 1, min_value=0, max_value=1
    )
    step_2_3_similarity_filter_id_vs_name: int = _get_int(
        "NL2DATA_PHASE2_STEP_2_3_SIMILARITY_FILTER_ID_VS_NAME", 1, min_value=0, max_value=1
    )

    # Step 2.14 entity cleanup loop (how many iterations we allow)
    step_2_14_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_14_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)
    step_2_14_max_iterations: int = _get_int("NL2DATA_PHASE2_STEP_2_14_MAX_ITERATIONS", 4, min_value=1, max_value=20)
    step_2_14_max_time_sec: int = _get_int("NL2DATA_PHASE2_STEP_2_14_MAX_TIME_SEC", 180, min_value=10, max_value=3600)

    # Step 2.16 cross-entity reconciliation loop
    step_2_16_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_16_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 2.4 composite attribute decomposition loop (DSL + decomposition rules)
    step_2_4_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_4_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 2.9 derived formula loop (keep asking until DSL is valid)
    step_2_9_max_revision_rounds: int = _get_int("NL2DATA_PHASE2_STEP_2_9_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)


def get_phase2_config() -> Phase2Config:
    return Phase2Config()


@dataclass(frozen=True)
class Phase3Config:
    """Phase 3 tuning knobs (env-backed)."""

    # Step 3.1 info needs: validation + revision loop rounds
    step_3_1_max_revision_rounds: int = _get_int("NL2DATA_PHASE3_STEP_3_1_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Step 3.2 completeness: loop bounds (kept small for speed)
    step_3_2_max_iterations: int = _get_int("NL2DATA_PHASE3_STEP_3_2_MAX_ITERATIONS", 5, min_value=1, max_value=10)
    step_3_2_max_time_sec: int = _get_int("NL2DATA_PHASE3_STEP_3_2_MAX_TIME_SEC", 120, min_value=30, max_value=600)

    # Phase 3: disable tool-based executors for speed + determinism (use deterministic checks instead)
    phase3_tools_enabled: int = _get_int("NL2DATA_PHASE3_TOOLS_ENABLED", 0, min_value=0, max_value=1)


def get_phase3_config() -> Phase3Config:
    return Phase3Config()


@dataclass(frozen=True)
class Phase4Config:
    """Phase 4 tuning knobs (env-backed)."""

    # Step 4.1 FD analysis: revision rounds for invalid/cross-entity outputs
    step_4_1_max_revision_rounds: int = _get_int("NL2DATA_PHASE4_STEP_4_1_MAX_REVISION_ROUNDS", 5, min_value=0, max_value=10)

    # Context control: include full NL only when needed (default off)
    step_4_1_include_nl_context: int = _get_int("NL2DATA_PHASE4_STEP_4_1_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)

    # Phase 4: categorical / constraints steps should not need full NL by default
    step_4_4_include_nl_context: int = _get_int("NL2DATA_PHASE4_STEP_4_4_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_4_5_include_nl_context: int = _get_int("NL2DATA_PHASE4_STEP_4_5_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_4_6_include_nl_context: int = _get_int("NL2DATA_PHASE4_STEP_4_6_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)
    step_4_7_include_nl_context: int = _get_int("NL2DATA_PHASE4_STEP_4_7_INCLUDE_NL_CONTEXT", 0, min_value=0, max_value=1)


def get_phase4_config() -> Phase4Config:
    return Phase4Config()

