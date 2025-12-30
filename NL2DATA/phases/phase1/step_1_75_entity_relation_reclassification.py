"""Phase 1, Step 1.75: Entity vs Associative-Link Reclassification.

Goal:
- Prevent junction/associative constructs (e.g., OrderItem, BookAuthor) from being treated
  as regular entities in Phase 2 (PK identification, intrinsic attrs, etc.).
- Optionally, produce "relation candidates" so Phase 1 relation extraction can account for them.

Design:
- Deterministic heuristic first (cheap guardrail).
- Optional LLM verification loop over heuristic candidates to reduce false positives.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.entity_reclassification import pick_associative_candidates
from NL2DATA.utils.tools.validation_tools import (
    _verify_entity_in_known_entities_impl,
    _check_entity_name_validity_impl,
    _validate_subset_impl,
)


logger = get_logger(__name__)


class ReclassifyAsRelation(BaseModel):
    name: str = Field(description="Entity name that should be reclassified as a relation (associative/link).")
    reasoning: str = Field(description="Why this looks like a relation/junction rather than a standalone entity.")
    endpoints: Dict[str, str] = Field(description="Entity endpoints with 'left' and 'right' keys (from EntityRegistry)")
    relationship_type: Literal["one_to_one", "one_to_many", "many_to_many"] = Field(description="Type of relationship")
    key_strategy: Literal["composite_fk", "surrogate_pk"] = Field(description="Key strategy for the relation")
    relationship_attributes: List[str] = Field(default_factory=list, description="Non-key attributes carried by the link")

    model_config = ConfigDict(extra="forbid")


class EntityRelationReclassificationOutput(BaseModel):
    keep_entities: List[str] = Field(default_factory=list, description="Entities to keep as real entities.")
    reclassify_as_relation: List[ReclassifyAsRelation] = Field(
        default_factory=list,
        description="Entities that should be treated as relations/junctions instead of entities.",
    )
    reasoning: str = Field(description="Overall reasoning and any borderline calls.")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.75", phase=1, tags=["entity_reclassification"])
async def step_1_75_entity_relation_reclassification(
    entities: List[Dict[str, Any]],
    nl_description: str,
    domain: Optional[str] = None,
    *,
    heuristic_threshold: float = 0.6,
    use_llm_verification: bool = True,
    max_iterations: int = 2,
) -> Dict[str, Any]:
    """Reclassify false-positive 'entities' that are really associative relations."""
    entity_names = [
        e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        for e in entities
        if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", ""))
    ]

    if not entities:
        return {
            "entities": [],
            "removed_entity_names": [],
            "reclassified": [],
            "relation_candidates": [],
        }

    # 1) Deterministic heuristic guardrail
    candidates = pick_associative_candidates(entities, threshold=heuristic_threshold)
    candidate_names = [c.name for c in candidates]
    if not candidate_names:
        logger.debug("Step 1.75: No heuristic associative candidates found.")
        return {
            "entities": entities,
            "removed_entity_names": [],
            "reclassified": [],
            "relation_candidates": [],
        }

    logger.info(f"Step 1.75: Heuristic candidates for reclassification: {candidate_names}")

    # If LLM verification is disabled, apply heuristic decision directly.
    removed_names: List[str] = []
    reclassified: List[Dict[str, Any]] = []
    relation_candidates: List[str] = []

    if not use_llm_verification:
        removed_names = list(candidate_names)
        reclassified = [
            {"name": n, "reasoning": "Heuristic: associative/junction-like entity name/description."}
            for n in removed_names
        ]
        relation_candidates = [f"{n} is likely an associative relation with attributes." for n in removed_names]
        kept_entities = [e for e in entities if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) not in set(removed_names)]
        return {
            "entities": kept_entities,
            "removed_entity_names": removed_names,
            "reclassified": reclassified,
            "relation_candidates": relation_candidates,
        }

    # 2) LLM verification loop over heuristic candidates
    # We only allow the LLM to decide among the heuristic candidates (safety).
    llm = get_model_for_step("1.75")

    system_prompt = """You are a database modeling expert.

Task
For each candidate in Candidates, decide whether it should remain an entity table or be reclassified as a relation/link table (association-with-attributes).

Inputs
- Candidates: list of {name, description}
- EntityRegistry: list of canonical entity names (valid endpoints)

Rules
- Evaluate ONLY Candidates. Do not mention entities not in Candidates.
- Be conservative: if unsure, KEEP as entity.
- Reclassify as relation ONLY if the concept is primarily defined by linking two entities in EntityRegistry and typically uses FKs as its identifying keys (often composite).
- Do NOT invent new entities, attributes, or endpoints outside EntityRegistry.

If reclassifying, you MUST provide:
- endpoints: {left, right} using names from EntityRegistry
- relationship_type: one_to_one | one_to_many | many_to_many
- key_strategy: composite_fk | surrogate_pk
- relationship_attributes: list of non-key attributes carried by the link

Output JSON only with EXACT keys:
{
  "keep_entities": [string],
  "reclassify_as_relation": [
    {
      "name": string,
      "endpoints": {"left": string, "right": string},
      "relationship_type": "one_to_one"|"one_to_many"|"many_to_many",
      "key_strategy": "composite_fk"|"surrogate_pk",
      "relationship_attributes": [string],
      "reasoning": string
    }
  ],
  "reasoning": string
}

Constraints
- keep_entities must be a subset of Candidates
- reclassify_as_relation[].name must be a subset of Candidates
- endpoints.left and endpoints.right must be from EntityRegistry
- No extra keys. No markdown. No extra text.

Tool usage (mandatory)
You have access to:
1) verify_entity_in_known_entities(entity: str, known_entities: List[str]) -> {exists: bool, error: str|null}
2) check_entity_name_validity(name: str) -> {valid: bool, error: str|null, suggestion: str|null}
3) validate_subset(subset: List[str], superset: List[str]) -> {is_subset: bool, invalid_items: List[str], error: str|null}

Use tools to validate: candidate subset membership, endpoint membership, and name validity before finalizing."""

    def _guess_kind(name: str) -> str:
        n = (name or "").lower()
        if "type" in n:
            return "dimension"
        if "event" in n or "incident" in n:
            return "event"
        if "reading" in n or "log" in n:
            return "fact"
        return "entity"

    candidates_payload = [
        {"name": e.get("name", ""), "description": e.get("description", "")}
        for e in entities
        if e.get("name", "") in set(candidate_names)
    ]
    entity_registry_payload = [
        {"name": e.get("name", ""), "kind": _guess_kind(e.get("name", ""))}
        for e in entities
        if e.get("name", "") and e.get("name", "") not in set(candidate_names)
    ]

    context_json_str = json.dumps(
        {
            "candidates": candidates_payload,
            "entity_registry": entity_registry_payload,
            "constraints": {
                "be_conservative": True,
                "evaluate_only_candidates": True,
                "no_new_entities": True,
            },
        },
        indent=2,
        ensure_ascii=False,
    )

    human_prompt_template = """Context (structured JSON):
{context_json}
"""

    # Loop: allow re-run if model returns empty/invalid subsets (rare) or contradictory output
    keep_set = set(candidate_names)
    reclass_set: set[str] = set()

    for _ in range(max_iterations):
        config = get_trace_config("1.75", phase=1, tags=["entity_reclassification"])
        out: EntityRelationReclassificationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityRelationReclassificationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "context_json": context_json_str,
            },
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Enforce safety: only accept decisions about candidates.
        llm_keep = {n for n in (out.keep_entities or []) if n in set(candidate_names)}
        llm_reclass = {r.name for r in (out.reclassify_as_relation or []) if r.name in set(candidate_names)}

        # Deterministic validation of subsets and endpoints
        subset_check_1 = _validate_subset_impl(list(llm_keep), candidate_names)
        subset_check_2 = _validate_subset_impl(list(llm_reclass), candidate_names)
        if not subset_check_1.get("is_subset", True) or not subset_check_2.get("is_subset", True):
            logger.warning("Step 1.75: LLM returned invalid subsets; falling back to KEEP all candidates.")
            keep_set = set(candidate_names)
            reclass_set = set()
            break

        # Validate endpoints (must be from entity registry payload names)
        registry_names = [x.get("name", "") for x in entity_registry_payload if x.get("name")]
        endpoints_ok = True
        for item in out.reclassify_as_relation or []:
            if item.name not in set(candidate_names):
                continue
            left = (item.endpoints or {}).get("left", "")
            right = (item.endpoints or {}).get("right", "")
            if not _verify_entity_in_known_entities_impl(left, registry_names).get("exists", False):
                endpoints_ok = False
                break
            if not _verify_entity_in_known_entities_impl(right, registry_names).get("exists", False):
                endpoints_ok = False
                break
            # Light name validity check (should be SQL-safe identifiers)
            if not _check_entity_name_validity_impl(left).get("valid", False):
                endpoints_ok = False
                break
            if not _check_entity_name_validity_impl(right).get("valid", False):
                endpoints_ok = False
                break
        if not endpoints_ok:
            logger.warning("Step 1.75: LLM returned invalid endpoints; falling back to KEEP all candidates.")
            keep_set = set(candidate_names)
            reclass_set = set()
            break

        # If model gave nothing useful, keep everything (conservative).
        if not llm_keep and not llm_reclass:
            keep_set = set(candidate_names)
            reclass_set = set()
            break

        # If overlaps, prefer KEEP (conservative)
        overlap = llm_keep & llm_reclass
        if overlap:
            llm_reclass -= overlap

        keep_set = llm_keep | (set(candidate_names) - llm_reclass - llm_keep)
        reclass_set = llm_reclass
        # Accept first stable pass
        break

    removed_names = sorted(reclass_set)
    removed_set = set(removed_names)

    # Build outputs
    for c in candidates:
        if c.name in removed_set:
            reclassified.append(
                {
                    "name": c.name,
                    "heuristic_score": c.score,
                    "heuristic_reasons": list(c.reasons),
                    "reasoning": "Reclassified as associative/link construct.",
                }
            )

    # Pull LLM-provided richer descriptions when available
    # (We do not store raw model output; we store structured, safe strings.)
    # Note: we only have `out` in scope if loop ran; this is okay because we return conservative keep otherwise.
    try:
        for item in out.reclassify_as_relation:
            if item.name in removed_set:
                left = (item.endpoints or {}).get("left", "")
                right = (item.endpoints or {}).get("right", "")
                relation_candidates.append(
                    f"{item.name} links {left} and {right} ({item.relationship_type}, {item.key_strategy})."
                )
    except Exception:
        relation_candidates = [f"{n} is an associative/link relation with attributes." for n in removed_names]

    kept_entities = [
        e for e in entities
        if (e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")) not in removed_set
    ]

    logger.info(
        f"Step 1.75: Removed {len(removed_names)} associative entities: {removed_names}. "
        f"Remaining entities: {len(kept_entities)}."
    )

    return {
        "entities": kept_entities,
        "removed_entity_names": removed_names,
        "reclassified": reclassified,
        "relation_candidates": relation_candidates,
    }


