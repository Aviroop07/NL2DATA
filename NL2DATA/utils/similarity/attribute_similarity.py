"""Attribute similarity and candidate synonym-pair generation.

Goal: deterministically propose a small set of "suspicious" attribute pairs
that might be synonyms, using semantic embeddings (MiniLM) when available.

This module must remain safe to import even if sentence-transformers is not installed.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
import difflib
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_MODEL_SINGLETON: Any = None
_MODEL_NAME: Optional[str] = None
_EMBED_CACHE: Dict[Tuple[str, str], List[float]] = {}  # (model_name, text) -> embedding


def _normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _normalize_name_for_tokens(name: str) -> str:
    s = str(name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _name_tokens(name: str) -> List[str]:
    n = _normalize_name_for_tokens(name)
    if not n:
        return []
    return [t for t in n.split("_") if t]


def _token_set_for_synonym_heuristics(name: str) -> set[str]:
    toks = set(_name_tokens(name))
    # Keep 'id' as a token so we can apply rules around it.
    stop = {"the", "a", "an", "of", "for", "to", "in", "on", "by", "and", "or"}
    return {t for t in toks if t not in stop}


def _pair_key(a: str, b: str) -> Tuple[str, str]:
    a = str(a or "").strip()
    b = str(b or "").strip()
    if a <= b:
        return (a, b)
    return (b, a)


def _looks_like_time_variant_pair(a: str, b: str) -> bool:
    ta = _token_set_for_synonym_heuristics(a)
    tb = _token_set_for_synonym_heuristics(b)
    # Commonly-confused but distinct pairs: created vs updated, start vs end, min vs max.
    oppositions = [
        ({"created", "createdat", "created_at"}, {"updated", "updatedat", "updated_at"}),
        ({"start", "startat", "start_at", "from"}, {"end", "endat", "end_at", "to"}),
        ({"min", "minimum"}, {"max", "maximum"}),
        ({"first"}, {"last"}),
    ]
    for left, right in oppositions:
        if (ta & left and tb & right) or (ta & right and tb & left):
            return True
    return False


def _is_id_like(name: str) -> bool:
    n = _normalize_name_for_tokens(name)
    if not n:
        return False
    if n == "id" or n.endswith("_id"):
        return True
    toks = _token_set_for_synonym_heuristics(n)
    return "id" in toks


def _is_description_like(name: str) -> bool:
    toks = _token_set_for_synonym_heuristics(name)
    return "description" in toks or "desc" in toks


def _is_name_like(name: str) -> bool:
    toks = _token_set_for_synonym_heuristics(name)
    return "name" in toks


def _lexical_jaccard(name_a: str, name_b: str) -> float:
    ta = _token_set_for_synonym_heuristics(name_a)
    tb = _token_set_for_synonym_heuristics(name_b)
    if not ta and not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return float(inter) / float(union) if union else 0.0


def _char_similarity(name_a: str, name_b: str) -> float:
    a = _normalize_name_for_tokens(name_a)
    b = _normalize_name_for_tokens(name_b)
    if not a or not b:
        return 0.0
    return float(difflib.SequenceMatcher(a=a, b=b).ratio())


def _should_filter_pair(
    name_a: str,
    name_b: str,
    *,
    filter_description_pairs: bool,
    filter_id_vs_non_id: bool,
    filter_id_vs_name: bool,
) -> Optional[str]:
    """Return a reason to filter out a candidate pair, else None."""
    a = _normalize_name_for_tokens(name_a)
    b = _normalize_name_for_tokens(name_b)
    if not a or not b:
        return "empty_name"
    if a == b:
        return None  # exact duplicate should always be allowed through
    if _looks_like_time_variant_pair(a, b):
        return "time_variant_pair_distinct"
    if filter_description_pairs:
        da = _is_description_like(a)
        db = _is_description_like(b)
        # If one is description-like and the other isn't, it's almost never a synonym in schemas.
        if da != db and (da or db):
            return "description_vs_non_description"
    if filter_id_vs_non_id:
        ida = _is_id_like(a)
        idb = _is_id_like(b)
        if ida != idb:
            return "id_vs_non_id"
    if filter_id_vs_name:
        # Specifically block *_id vs *_name (often high embedding similarity but semantically distinct).
        ida = _is_id_like(a)
        idb = _is_id_like(b)
        na = _is_name_like(a)
        nb = _is_name_like(b)
        if (ida and nb) or (idb and na):
            return "id_vs_name"
    return None


def _attr_text(attr: Dict[str, Any]) -> str:
    name = _normalize_whitespace(str(attr.get("name", "")))
    desc = _normalize_whitespace(str(attr.get("description", "")))
    if desc:
        return f"name: {name}\ndescription: {desc}"
    return f"name: {name}"


def _lazy_get_sentence_transformer(model_name: str):
    # Lazy import so that the pipeline doesn't crash on import if dependency isn't installed.
    from sentence_transformers import SentenceTransformer  # type: ignore

    global _MODEL_SINGLETON, _MODEL_NAME
    if _MODEL_SINGLETON is None or _MODEL_NAME != model_name:
        _MODEL_SINGLETON = SentenceTransformer(model_name)
        _MODEL_NAME = model_name
    return _MODEL_SINGLETON


def _embed_texts(
    texts: Sequence[str],
    *,
    model_name: str,
) -> List[List[float]]:
    model = _lazy_get_sentence_transformer(model_name)
    # normalize_embeddings=True yields unit vectors, so cosine similarity is dot product.
    vectors = model.encode(list(texts), normalize_embeddings=True)
    # `vectors` can be numpy array; convert to python lists for portability.
    out: List[List[float]] = []
    for v in vectors:
        out.append([float(x) for x in v])
    return out


def _dot(u: Sequence[float], v: Sequence[float]) -> float:
    return sum((a * b for a, b in zip(u, v)))


def _safe_cosine_similarity(u: Sequence[float], v: Sequence[float]) -> float:
    # If embeddings are already normalized, this is just dot; otherwise normalize defensively.
    du = _dot(u, u)
    dv = _dot(v, v)
    if du <= 0.0 or dv <= 0.0:
        return 0.0
    return _dot(u, v) / math.sqrt(du * dv)


@dataclass(frozen=True)
class CandidatePair:
    attr1: str
    attr2: str
    score: float
    reason: str


def propose_attribute_synonym_candidates(
    attributes: List[Dict[str, Any]],
    *,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    threshold: float = 0.82,
    max_pairs: int = 20,
    lexical_min_jaccard: float = 0.20,
    filter_description_pairs: bool = True,
    filter_id_vs_non_id: bool = True,
    filter_id_vs_name: bool = True,
) -> List[Dict[str, Any]]:
    """Return a list of candidate synonym pairs for an entity's attribute list.

    Output is a JSON-serializable list of dicts:
      { "attr1": str, "attr2": str, "score": float, "reason": str }
    """
    if max_pairs <= 0:
        return []

    # Keep only well-formed attribute dicts with names.
    normalized_attrs: List[Dict[str, Any]] = []
    seen_names: set[str] = set()
    for a in attributes or []:
        if not isinstance(a, dict):
            continue
        name = str(a.get("name", "")).strip()
        if not name:
            continue
        # Preserve duplicates in raw list for prompting, but candidate generation uses unique names.
        if name in seen_names:
            continue
        seen_names.add(name)
        normalized_attrs.append(a)

    names = [str(a.get("name", "")).strip() for a in normalized_attrs]
    if len(names) < 2:
        return []

    # Exact duplicates (case-insensitive) are always candidates.
    lower_to_originals: Dict[str, List[str]] = {}
    for n in names:
        lower_to_originals.setdefault(n.lower(), []).append(n)

    candidates: List[CandidatePair] = []
    for _, group in lower_to_originals.items():
        if len(group) >= 2:
            # Propose all within group, but cap naturally later.
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    candidates.append(CandidatePair(attr1=group[i], attr2=group[j], score=1.0, reason="exact_duplicate_case_insensitive"))

    # Semantic similarity candidates via embeddings.
    # Use caching per (model_name, text) to avoid recompute within long runs.
    texts: List[str] = []
    for a in normalized_attrs:
        txt = _attr_text(a)
        texts.append(txt)

    embeddings: List[List[float]] = []
    missing_indices: List[int] = []
    for idx, txt in enumerate(texts):
        key = (model_name, txt)
        if key in _EMBED_CACHE:
            embeddings.append(_EMBED_CACHE[key])
        else:
            embeddings.append([])  # placeholder
            missing_indices.append(idx)

    if missing_indices:
        missing_texts = [texts[i] for i in missing_indices]
        new_vecs = _embed_texts(missing_texts, model_name=model_name)
        for local_i, global_i in enumerate(missing_indices):
            vec = new_vecs[local_i]
            embeddings[global_i] = vec
            _EMBED_CACHE[(model_name, texts[global_i])] = vec

    # Compute pairwise cosine similarity (dot if normalized).
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            n1 = names[i]
            n2 = names[j]
            filter_reason = _should_filter_pair(
                n1,
                n2,
                filter_description_pairs=filter_description_pairs,
                filter_id_vs_non_id=filter_id_vs_non_id,
                filter_id_vs_name=filter_id_vs_name,
            )
            if filter_reason:
                continue
            lex = _lexical_jaccard(n1, n2)
            if lex < lexical_min_jaccard:
                # Hybrid gate: semantic similarity alone is too permissive for schema attribute names.
                continue
            sim = _safe_cosine_similarity(embeddings[i], embeddings[j])
            if sim >= threshold:
                # Include lexical for debugging/review, but keep schema stable (LLM still decides).
                reason = f"semantic_embedding_cosine; lexical_jaccard={round(float(lex),4)}; char_sim={round(float(_char_similarity(n1, n2)),4)}"
                candidates.append(CandidatePair(attr1=n1, attr2=n2, score=float(sim), reason=reason))

    # Deduplicate candidate pairs keeping max score.
    best_by_pair: Dict[Tuple[str, str], CandidatePair] = {}
    for c in candidates:
        k = _pair_key(c.attr1, c.attr2)
        prev = best_by_pair.get(k)
        if prev is None or c.score > prev.score:
            best_by_pair[k] = c

    # Sort descending by score; then by attr names for stability.
    ordered = sorted(best_by_pair.values(), key=lambda x: (-x.score, x.attr1.lower(), x.attr2.lower()))
    ordered = ordered[:max_pairs]

    return [{"attr1": c.attr1, "attr2": c.attr2, "score": round(float(c.score), 4), "reason": c.reason} for c in ordered]

