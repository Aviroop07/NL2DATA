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

from typing import List, Optional, Set, Tuple
import json
import re

from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.utils.pipeline_config import get_phase2_config
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements


logger = get_logger(__name__)


class CrossEntityReconciledAttributesOutput(BaseModel):
    attributes: List[AttributeInfo] = Field(description="Revised attribute list for the entity")
    reasoning: Optional[str] = Field(default=None, description="Brief explanation of changes (optional)")


def _snake_tokens(name: str) -> Set[str]:
    base = re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()
    toks = set([t for t in base.split(" ") if t])
    # remove extremely generic tokens
    return {t for t in toks if t not in {"table", "record", "data", "info"}}


def _connected_entities(relations: List, entity_name: str) -> Set[str]:
    connected: Set[str] = set()
    for rel in relations or []:
        ents = rel.get("entities", []) or []
        if entity_name in ents:
            for e in ents:
                if e and e != entity_name:
                    connected.add(e)
    return connected


def _attr_names(attrs: List) -> List[str]:
    out: List[str] = []
    for a in attrs or []:
        if isinstance(a, dict):
            n = a.get("name", "")
        else:
            n = getattr(a, "name", "")
        if n:
            out.append(str(n))
    return out


class CrossEntityIssue(BaseModel):
    """Information about a cross-entity attribute issue."""
    attribute: str = Field(description="Name of the attribute with the issue")
    issue_type: str = Field(description="Type of issue")
    connected_entity: str = Field(description="Name of the connected entity")
    detail: str = Field(description="Description of the issue")


def _detect_cross_entity_issues(
    *,
    entity_name: str,
    entity_attributes: List,
    connected_entity_name: str,
    connected_attributes: List,
) -> List[CrossEntityIssue]:
    """Heuristic detection only; do not mutate."""
    issues: List[CrossEntityIssue] = []

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
                CrossEntityIssue(
                    attribute=a_name,
                    issue_type="same_name_as_connected_entity_attribute",
                    connected_entity=connected_entity_name,
                    detail=f"Attribute name matches an attribute in connected entity '{connected_entity_name}'.",
                )
            )

        # Entity-name token leakage: attr begins with connected entity token + '_' (course_*)
        if conn_tokens and any(tok in a_tokens for tok in conn_tokens):
            # Avoid flagging when it's clearly intrinsic (rare); keep as a weak signal.
            issues.append(
                CrossEntityIssue(
                    attribute=a_name,
                    issue_type="mentions_connected_entity_token",
                    connected_entity=connected_entity_name,
                    detail=f"Attribute name contains token(s) from connected entity '{connected_entity_name}' (possible inlining/denormalization).",
                )
            )

    return issues


@traceable_step("2.16", phase=2, tags=["phase_2_step_16"])
async def step_2_16_cross_entity_attribute_reconciliation_single(
    *,
    entity_name: str,
    entity_description: Optional[str],
    entity_attributes: List,
    relations: List,
    all_entity_attributes: dict,
    nl_description: Optional[str],
    domain: Optional[str],
) -> CrossEntityReconciledAttributesOutput:
    connected = sorted(_connected_entities(relations, entity_name))
    if not connected:
        # Convert entity_attributes to AttributeInfo if needed
        attrs = []
        for attr in entity_attributes:
            if isinstance(attr, dict):
                # Ensure required fields have defaults
                attr_dict = attr.copy()
                if "description" not in attr_dict:
                    attr_dict["description"] = ""
                attrs.append(AttributeInfo(**attr_dict))
            elif isinstance(attr, AttributeInfo):
                attrs.append(attr)
            else:
                # Try to convert if it has name attribute
                if hasattr(attr, 'name'):
                    attrs.append(AttributeInfo(
                        name=getattr(attr, 'name', ''),
                        description=getattr(attr, 'description', '')
                    ))
        return CrossEntityReconciledAttributesOutput(
            attributes=attrs,
            reasoning="No connected entities; no cross-entity reconciliation needed."
        )
    
    # Collect issues across all connected entities
    all_issues: List[CrossEntityIssue] = []
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
        # Convert entity_attributes to AttributeInfo if needed
        attrs = []
        for attr in entity_attributes:
            if isinstance(attr, dict):
                # Ensure required fields have defaults
                attr_dict = attr.copy()
                if "description" not in attr_dict:
                    attr_dict["description"] = ""
                attrs.append(AttributeInfo(**attr_dict))
            elif isinstance(attr, AttributeInfo):
                attrs.append(attr)
            else:
                # Try to convert if it has name attribute
                if hasattr(attr, 'name'):
                    attrs.append(AttributeInfo(
                        name=getattr(attr, 'name', ''),
                        description=getattr(attr, 'description', '')
                    ))
        return CrossEntityReconciledAttributesOutput(
            attributes=attrs,
            reasoning="No cross-entity attribute leakage detected."
        )

    # Reuse the same model profile as Step 2.2 (high_fanout) for now.
    llm = get_model_for_step("2.2")
    config = get_trace_config("2.16", phase=2, tags=["phase_2_step_16"])
    cfg = get_phase2_config()

    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=CrossEntityReconciledAttributesOutput,
        additional_requirements=[
            "Do NOT invent new attributes unless necessary to fix a naming issue",
            "Do NOT remove primary key attributes if present in the list",
            "Avoid including relation-connecting attributes; those are handled later",
            "Keep snake_case names",
        ]
    )
    
    system_prompt = f"""You are a database design assistant.

Task:
You are given an entity's current attribute list and a set of connected entities (via relationships),
plus a deterministic list of potential cross-entity attribute leakage issues (e.g., Student.course_name while Course has name).

Return a revised FULL attribute list for the entity that resolves these issues while preserving valid intrinsic attributes.

Rules:
- Do NOT invent new attributes unless necessary to fix a naming issue; prefer renaming to an intrinsic name or removing if it is clearly denormalized from a connected entity.
- Do NOT remove primary key attributes if present in the list (keep id fields that identify the entity itself).
- Avoid including relation-connecting attributes; those are handled later.
- Keep snake_case names.

{output_structure_section}"""

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
                "issues_json": json.dumps([issue.model_dump() for issue in all_issues], ensure_ascii=True),
            },
            config=config,
        )
        previous = [a.model_dump() for a in revised.attributes]
        # We intentionally do not re-detect issues here; this step is a soft second-pass.
        break

    if revised:
        return revised
    else:
        # Convert entity_attributes to AttributeInfo if needed
        if entity_attributes and isinstance(entity_attributes[0], dict):
            attrs = [AttributeInfo(**attr) if isinstance(attr, dict) else attr for attr in entity_attributes]
        else:
            attrs = entity_attributes
        return CrossEntityReconciledAttributesOutput(
            attributes=attrs,
            reasoning="No changes made"
        )


class EntityCrossEntityReconciliationResult(BaseModel):
    """Result for a single entity in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    attributes: List[AttributeInfo] = Field(description="Revised attribute list for the entity")
    reasoning: Optional[str] = Field(default=None, description="Brief explanation of changes")


class CrossEntityReconciliationBatchOutput(BaseModel):
    """Output structure for Step 2.16 batch processing."""
    entity_results: List[EntityCrossEntityReconciliationResult] = Field(
        description="List of cross-entity reconciliation results, one per entity"
    )
    total_entities: int = Field(description="Total number of entities processed")


async def step_2_16_cross_entity_attribute_reconciliation_batch(
    *,
    entities: List,
    attributes: dict,
    relations: List,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> CrossEntityReconciliationBatchOutput:
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

    entity_results_list = []
    for (entity_name, _), res in zip(tasks, results):
        if isinstance(res, Exception):
            logger.error(f"Step 2.16 error for entity {entity_name}: {res}")
            # Get original attributes for fallback
            original_attrs = attributes.get(entity_name, [])
            # Convert to AttributeInfo list, handling all formats
            attrs = []
            for attr in original_attrs:
                if isinstance(attr, dict):
                    # Ensure required fields have defaults
                    attr_dict = attr.copy()
                    if "description" not in attr_dict:
                        attr_dict["description"] = ""
                    if "name" not in attr_dict:
                        continue  # Skip invalid attributes
                    try:
                        attrs.append(AttributeInfo(**attr_dict))
                    except Exception as e:
                        logger.warning(f"Failed to convert attribute dict to AttributeInfo: {e}, skipping")
                elif isinstance(attr, AttributeInfo):
                    attrs.append(attr)
                else:
                    # Try to convert if it has name attribute
                    if hasattr(attr, 'name'):
                        try:
                            attrs.append(AttributeInfo(
                                name=getattr(attr, 'name', ''),
                                description=getattr(attr, 'description', '')
                            ))
                        except Exception as e:
                            logger.warning(f"Failed to convert attribute to AttributeInfo: {e}, skipping")
            entity_results_list.append(
                EntityCrossEntityReconciliationResult(
                    entity_name=entity_name,
                    attributes=attrs,
                    reasoning=f"Error: {res}"
                )
            )
        else:
            entity_results_list.append(
                EntityCrossEntityReconciliationResult(
                    entity_name=entity_name,
                    attributes=res.attributes,
                    reasoning=res.reasoning,
                )
            )

    return CrossEntityReconciliationBatchOutput(
        entity_results=entity_results_list,
        total_entities=len(entities),
    )

