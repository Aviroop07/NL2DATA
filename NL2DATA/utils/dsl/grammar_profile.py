"""Controllable DSL grammar extension mechanism.

Goal:
- Keep default grammar stable.
- Allow opt-in extensions via an explicit profile (version + feature flags).

Important:
- We avoid changing the base grammar in a way that introduces ambiguity.
- Extensions should be small, isolated, and guarded by flags.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet

from .grammar import DSL_GRAMMAR


@dataclass(frozen=True)
class DSLGrammarProfile:
    """A profile controlling which optional DSL grammar extensions are enabled."""

    version: str = "v1"
    features: FrozenSet[str] = field(default_factory=frozenset)


# Feature flags (string constants)
FEATURE_BETWEEN = "between"  # add: expr BETWEEN expr AND expr
FEATURE_IS_NULL = "is_null"  # add: expr IS NULL / expr IS NOT NULL


_EXT_FRAGMENTS = {
    FEATURE_BETWEEN: r"""
// --- optional: BETWEEN operator ---
BETWEEN.2: /(?i:between)/
// Also requires AND keyword (already exists) in base grammar.
// cmp_tail extension:
| BETWEEN sum_expr AND sum_expr
""",
    FEATURE_IS_NULL: r"""
// --- optional: IS NULL / IS NOT NULL ---
IS.2: /(?i:is)/
// cmp_tail extension:
| IS NULL
| IS NOT NULL
""",
}


def build_dsl_grammar(profile: DSLGrammarProfile | None = None) -> str:
    """Return a full grammar string for the given profile.

    Default (None) returns the base grammar unchanged.
    """
    if profile is None:
        return DSL_GRAMMAR

    g = DSL_GRAMMAR

    # Controlled injection points.
    if "//__EXT_CMP_TAIL__" not in g or "//__EXT_KEYWORDS__" not in g:
        # Defensive: if grammar changes, fail closed (no extensions).
        return g

    keyword_inserts: list[str] = []
    cmp_tail_inserts: list[str] = []

    for feat in sorted(profile.features):
        frag = _EXT_FRAGMENTS.get(feat)
        if not frag:
            continue
        # Split fragments into token/keyword lines vs cmp_tail lines (starting with '|').
        for line in frag.splitlines():
            s = line.rstrip()
            if not s:
                continue
            if s.lstrip().startswith("|"):
                cmp_tail_inserts.append(s)
            else:
                keyword_inserts.append(s)

    # Inject keywords/tokens (no indentation requirement).
    g = g.replace("//__EXT_KEYWORDS__", "\n".join(keyword_inserts) if keyword_inserts else "")
    # Inject cmp_tail alternatives (must align indentation with existing rule lines).
    # The base rule uses 8 spaces before '|', keep consistent by prefixing "        ".
    cmp_block = "\n".join(["        " + s.lstrip() for s in cmp_tail_inserts]) if cmp_tail_inserts else ""
    g = g.replace("//__EXT_CMP_TAIL__", cmp_block)
    return g

