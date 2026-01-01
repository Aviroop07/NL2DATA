"""Phase 2, Step 2.16: Cross-Entity Attribute Reconciliation.

Goal:
After we have per-entity attributes, perform a "double precaution" pass:
- For each entity, look at relations it participates in
- Compare its attributes to attributes of connected entities
- If attributes appear to duplicate/inline data from connected entities (e.g., Student.course_name while Course has name),
  detect this deterministically and feed it back to the LLM.

Important: Python does NOT delete attributes here. The LLM returns the revised list.
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional, Set, Tuple
import json
import re

from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.utils.pipeline_config import get_phase2_config


logger = get_logger(__name__)


class CrossEntityReconciledAttributesOutput(BaseModel):
    attributes: List[AttributeInfo] = Field(description="Revised attribute list for the entity")
    reasoning: Optional[str] = Field(default=None, description="Brief explanation of changes (optional)")


def _snake_tokens(name: str) -> Set[str]:
    base = re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()
    toks = set([t for t in base.split(" ") if t])
    # remove extremely generic tokens
    return {t for t in toks if t not in {"table", "record", "data", "info"}}


def _connected_entities(relations: List[Dict[str, Any]], entity_name: str) -> Set[str]:
    connected: Set[str] = set()
    for rel in relations or []:
        ents = rel.get("entities", []) or []
        if entity_name in ents:
            for e in ents:
                if e and e != entity_name:
                    connected.add(e)
    return connected


def _attr_names(attrs: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for a in attrs or []:
        if isinstance(a, dict):
            n = a.get("name", "")
        else:
            n = getattr(a, "name", "")
        if n:
            out.append(str(n))
    return out


def _detect_cross_entity_issues(
    *,
    entity_name: str,
    entity_attributes: List[Dict[str, Any]],
    connected_entity_name: str,
    connected_attributes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Heuristic detection only; do not mutate."""
    issues: List[Dict[str, Any]] = []

    ent_tokens = _snake_tokens(entity_name)
    conn_tokens = _snake_tokens(connected_entity_name)

    conn_attr_names = set([n.lower() for n in _attr_names(connected_attributes)])

    for a in entity_attributes or []:
        a_name = (a.get("name", "") if isinstance(a, dict) else getattr(a, "name", "")).strip()
        if not a_name:
            continue
        a_lc = a_name.lower()
        a_tokens = set(a_lc.split("_"))

        # Example patterns:
        # - course_name in Student when Course has name/course_name
        # - course_id in Student when Course is a connected entity (often FK-ish)
        if a_lc in conn_attr_names:
            issues.append(
                {
                    "attribute": a_name,
                    "issue_type": "same_name_as_connected_entity_attribute",
                    "connected_entity": connected_entity_name,
                    "detail": f"Attribute name matches an attribute in connected entity '{connected_entity_name}'.",
                }
            )

        # Entity-name token leakage: attr begins with connected entity token + '_' (course_*)
        if conn_tokens and any(tok in a_tokens for tok in conn_tokens):
            # Avoid flagging when it's clearly intrinsic (rare); keep as a weak signal.
            issues.append(
                {
                    "attribute": a_name,
                    "issue_type": "mentions_connected_entity_token",
                    "connected_entity": connected_entity_name,
                    "detail": f"Attribute name contains token(s) from connected entity '{connected_entity_name}' (possible inlining/denormalization).",
                }
            )

    return issues


@traceable_step("2.16", phase=2, tags=["phase_2_step_16"])
async def step_2_16_cross_entity_attribute_reconciliation_single(
    *,
    entity_name: str,
    entity_description: Optional[str],
    entity_attributes: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    all_entity_attributes: Dict[str, List[Dict[str, Any]]],
    nl_description: Optional[str],
    domain: Optional[str],
) -> Dict[str, Any]:
    connected = sorted(_connected_entities(relations, entity_name))
    if not connected:
        return {"updated_attributes": entity_attributes, "issues": [], "reasoning": "No connected entities; no cross-entity reconciliation needed."}

    # Collect issues across all connected entities
    all_issues: List[Dict[str, Any]] = []
    connected_context = []
    for conn in connected:
        conn_attrs = all_entity_attributes.get(conn, []) or []
        connected_context.append(
            {
                "entity": conn,
                "attributes": _attr_names(conn_attrs),
            }
        )
        all_issues.extend(
            _detect_cross_entity_issues(
                entity_name=entity_name,
                entity_attributes=entity_attributes,
                connected_entity_name=conn,
                connected_attributes=conn_attrs,
            )
        )

    # If no issues, do nothing (save LLM calls)
    if not all_issues:
        return {"updated_attributes": entity_attributes, "issues": [], "reasoning": "No cross-entity attribute leakage detected."}

    # Reuse the same model profile as Step 2.2 (high_fanout) for now.
    llm = get_model_for_step("2.2")
    config = get_trace_config("2.16", phase=2, tags=["phase_2_step_16"])
    cfg = get_phase2_config()

    system_prompt = """You are a database design assistant.

Task:
You are given an entity's current attribute list and a set of connected entities (via relationships),
plus a deterministic list of potential cross-entity attribute leakage issues (e.g., Student.course_name while Course has name).

Return a revised FULL attribute list for the entity that resolves these issues while preserving valid intrinsic attributes.

Rules:
- Do NOT invent new attributes unless necessary to fix a naming issue; prefer renaming to an intrinsic name or removing if it is clearly denormalized from a connected entity.
- Do NOT remove primary key attributes if present in the list (keep id fields that identify the entity itself).
- Avoid including relation-connecting attributes; those are handled later.
- Keep snake_case names.

Return JSON only in the required schema."""

    human_prompt_template = """Entity: {entity_name}
Entity description: {entity_description}
Domain: {domain}

Current attributes (JSON):
{current_attributes_json}

Connected entities + their attribute names (JSON):
{connected_entities_json}

Detected cross-entity issues (JSON):
{issues_json}

Return a revised attribute list for {entity_name}."""

    # Allow a small number of revision rounds (default 1)
    previous = entity_attributes
    revised: CrossEntityReconciledAttributesOutput | None = None
    for round_idx in range(max(1, cfg.step_2_16_max_revision_rounds)):
        revised = await standardized_llm_call(
            llm=llm,
            output_schema=CrossEntityReconciledAttributesOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "entity_description": entity_description or "",
                "domain": domain or "",
                "current_attributes_json": json.dumps(previous, ensure_ascii=True),
                "connected_entities_json": json.dumps(connected_context, ensure_ascii=True),
                "issues_json": json.dumps(all_issues, ensure_ascii=True),
            },
            config=config,
        )
        previous = [a.model_dump() for a in revised.attributes]
        # We intentionally do not re-detect issues here; this step is a soft second-pass.
        break

    updated = [a.model_dump() for a in (revised.attributes if revised else [])] if revised else entity_attributes
    return {"updated_attributes": updated, "issues": all_issues, "reasoning": revised.reasoning if revised else None}


async def step_2_16_cross_entity_attribute_reconciliation_batch(
    *,
    entities: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """Run cross-entity reconciliation in parallel for all entities."""
    import asyncio

    tasks = []
    for e in entities or []:
        name = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        desc = e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
        if not name:
            continue
        tasks.append(
            (
                name,
                step_2_16_cross_entity_attribute_reconciliation_single(
                    entity_name=name,
                    entity_description=desc,
                    entity_attributes=attributes.get(name, []) or [],
                    relations=relations or [],
                    all_entity_attributes=attributes,
                    nl_description=nl_description,
                    domain=domain,
                ),
            )
        )

    results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)

    entity_results: Dict[str, Any] = {}
    updated_attributes: Dict[str, List[Dict[str, Any]]] = dict(attributes or {})

    for (entity_name, _), res in zip(tasks, results):
        if isinstance(res, Exception):
            logger.error(f"Step 2.16 error for entity {entity_name}: {res}")
            entity_results[entity_name] = {"issues": [], "reasoning": f"Error: {res}", "updated_attributes": attributes.get(entity_name, [])}
        else:
            entity_results[entity_name] = res
            updated_attributes[entity_name] = res.get("updated_attributes", attributes.get(entity_name, []))

    return {"entity_results": entity_results, "updated_attributes": updated_attributes}

