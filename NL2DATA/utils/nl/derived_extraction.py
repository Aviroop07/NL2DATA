"""Deterministic extraction of derived columns from NL descriptions.

We keep this intentionally conservative and high-precision:
- Only extracts backticked derivations like: `line_subtotal = unit_price * quantity`
- Returns both the derived attribute name and the RHS expression as a hint

Rationale:
For desc_012-like specs, derived columns are central. If derived attributes are not
present in the entity attribute list, Phase 2 Step 2.8 will drop them (it is designed
to classify only existing attributes), and Step 2.9 will run on an empty set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from NL2DATA.utils.dsl.function_registry import supported_function_names


@dataclass(frozen=True)
class DerivedColumnCandidate:
    name: str
    rhs: str
    evidence: str


_BACKTICK_DERIV_RE = re.compile(
    r"`\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<rhs>[^`]+?)\s*`",
    flags=re.DOTALL,
)


def extract_backticked_derived_candidates(nl_description: str) -> List[DerivedColumnCandidate]:
    """Extract `name = rhs` candidates that are explicitly backticked in NL."""
    text = nl_description or ""
    out: List[DerivedColumnCandidate] = []
    for m in _BACKTICK_DERIV_RE.finditer(text):
        name = (m.group("name") or "").strip()
        rhs = (m.group("rhs") or "").strip()
        ev = (m.group(0) or "").strip()
        if not name or not rhs:
            continue
        out.append(DerivedColumnCandidate(name=name, rhs=rhs, evidence=ev))
    return out


def _extract_identifiers(expr: str) -> List[str]:
    # Very simple identifier finder; DSL-level parsing happens later in Step 2.9.
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr or "")


def partition_local_vs_cross_entity(
    *,
    candidates: Sequence[DerivedColumnCandidate],
    known_attributes: Sequence[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Partition candidates into local-derived vs cross-entity derived.

    Returns:
      - local: {attr_name: rhs_hint}
      - cross: {attr_name: rhs_hint}
    """
    attrs: Set[str] = {a for a in (known_attributes or []) if isinstance(a, str) and a}
    funcs: Set[str] = {f.upper() for f in supported_function_names()}
    keywords = {
        "IF",
        "THEN",
        "ELSE",
        "CASE",
        "WHEN",
        "END",
        "AND",
        "OR",
        "NOT",
        "IN",
        "LIKE",
        "NULL",
        "TRUE",
        "FALSE",
    }

    local: Dict[str, str] = {}
    cross: Dict[str, str] = {}

    for c in candidates:
        name = c.name
        rhs = c.rhs
        if not name or not rhs:
            continue
        idents = _extract_identifiers(rhs)
        unknown: List[str] = []
        for tok in idents:
            up = tok.upper()
            if up in keywords or up in funcs:
                continue
            if tok in attrs:
                continue
            # numeric literals won't match identifier regex, so ignore
            unknown.append(tok)
        if unknown:
            cross[name] = rhs
        else:
            local[name] = rhs
    return local, cross

