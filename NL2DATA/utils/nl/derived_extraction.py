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


def _resolve_identifier_alias(token: str, attributes: Sequence[str]) -> Optional[str]:
    """Map NL token -> existing attribute name when there's a clear unique match.

    Examples:
      unit_price -> order_unit_price
      discount_percent -> order_discount_percent
    """
    tok = (token or "").strip()
    if not tok:
        return None
    tok_l = tok.lower()
    attrs = [a for a in (attributes or []) if isinstance(a, str) and a]

    exact = [a for a in attrs if a.lower() == tok_l]
    if len(exact) == 1:
        return exact[0]

    suff = [a for a in attrs if a.lower().endswith("_" + tok_l)]
    if len(suff) == 1:
        return suff[0]

    contains = [a for a in attrs if tok_l in a.lower()]
    if len(contains) == 1:
        return contains[0]

    # Part-based match (order-insensitive), with light synonym normalization.
    synonyms = {
        "percent": "percentage",
        "pct": "percentage",
        "qty": "quantity",
        "dt": "datetime",
        "num": "number",
        "amt": "amount",
    }

    def norm_parts(s: str) -> List[str]:
        parts = [p for p in (s or "").lower().split("_") if p]
        return [synonyms.get(p, p) for p in parts]

    tok_parts = set(norm_parts(tok))
    if len(tok_parts) >= 2:
        part_matches = []
        for a in attrs:
            a_parts = set(norm_parts(a))
            if tok_parts.issubset(a_parts):
                part_matches.append(a)
        if len(part_matches) == 1:
            return part_matches[0]

    return None


def _rewrite_rhs_with_aliases(rhs: str, attributes: Sequence[str]) -> str:
    """Rewrite RHS by replacing clearly-resolvable tokens with actual attribute names."""
    out = rhs or ""
    idents = sorted(set(_extract_identifiers(out)), key=len, reverse=True)
    for tok in idents:
        repl = _resolve_identifier_alias(tok, attributes)
        if not repl or repl == tok:
            continue
        # Replace whole-word occurrences only.
        out = re.sub(rf"\b{re.escape(tok)}\b", repl, out)
    return out


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
    # Important trick: include candidate names themselves so references like
    # discount_amount = line_subtotal * discount_percent can become "local"
    # once line_subtotal is also a candidate.
    base_attrs: List[str] = [a for a in (known_attributes or []) if isinstance(a, str) and a]
    candidate_names: List[str] = [c.name for c in candidates if c and c.name]
    augmented_attrs_list: List[str] = base_attrs + [n for n in candidate_names if n not in base_attrs]
    attrs: Set[str] = set(augmented_attrs_list)
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
        rhs = _rewrite_rhs_with_aliases(c.rhs, augmented_attrs_list)
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

