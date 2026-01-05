"""Phase 1, Step 1.76: Entity vs Attribute Guardrail (Deterministic).

Goal:
- Prevent obvious *attributes / derived flags* from being modeled as standalone entities.
- Keep Phase 1 entity list focused on tables / record-types.

Design:
- Deterministic heuristics only (no LLM), conservative defaults.
- Never remove entities that were explicitly mentioned (mention_type == "explicit").
- Return removed candidates (with reasons) for debugging and potential Phase 2 use.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.observability import traceable_step
from NL2DATA.utils.logging import get_logger


logger = get_logger(__name__)


class AttributeCandidateInfo(BaseModel):
    name: str = Field(description="Name of the entity that was removed as attribute-like")
    reason: str = Field(description="Reason why this entity was removed")
    evidence: Optional[str] = Field(default=None, description="Evidence from the description that suggested this is an attribute")

    model_config = ConfigDict(extra="forbid")


class EntityAttributeGuardrailOutput(BaseModel):
    """Output structure for entity attribute guardrail."""
    entities: List = Field(
        default_factory=list,
        description="List of entities that passed the guardrail (kept as entities)"
    )
    removed_entity_names: List[str] = Field(
        default_factory=list,
        description="List of entity names that were removed as attribute-like"
    )
    attribute_candidates: List[AttributeCandidateInfo] = Field(
        default_factory=list,
        description="List of attribute candidate information for removed entities"
    )

    model_config = ConfigDict(extra="forbid")


def _looks_like_attribute_name(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    low = n.lower()

    # Very common attribute-only concepts that should not be tables in Phase 1.
    if low in {
        "gender",
        "dob",
        "dateofbirth",
        "birthdate",
        "timestamp",
        "datetime",
    }:
        return True

    # Derived/flag-like patterns.
    if low.startswith("is") and ("flag" in low or "breach" in low or "indicator" in low):
        return True
    if "flag" in low or "indicator" in low:
        return True

    # Time-bucket helper concepts often created as entities (should be derived attrs or dims later).
    if low in {"weekend", "peakhour", "timegap"}:
        return True

    return False


def _evidence_suggests_derived_field(evidence: str) -> bool:
    ev = (evidence or "").strip()
    if not ev:
        return False

    low = ev.lower()
    # Assignment / formula / derived-column cues
    if "=" in ev:
        return True
    if "derived" in low or "computed" in low:
        return True
    if "flag" in low or "indicator" in low:
        return True
    if "where(" in low or "if " in low or "else" in low:
        return True
    # FD-like cues: "patient_id -> gender, dob"
    if "->" in ev or " fd" in low or "functional dependency" in low:
        return True

    return False


def _should_remove_as_attribute(entity) -> Optional[str]:
    """
    Return a reason string if the entity should be removed as attribute-like; otherwise None.
    Conservative: only remove when we have strong signals and it isn't explicitly mentioned.
    """
    name = (entity.get("name") or "").strip()
    mention_type = (entity.get("mention_type") or "").strip().lower()
    evidence = (entity.get("evidence") or "").strip()

    if not name:
        return None

    # Never remove explicitly mentioned entities.
    if mention_type == "explicit":
        return None

    name_signal = _looks_like_attribute_name(name)
    evidence_signal = _evidence_suggests_derived_field(evidence)

    # Strong allowlist removal: core demographic attributes
    if name.lower() in {"gender", "dob", "dateofbirth", "birthdate"}:
        return "Attribute-like concept (demographic field); should be an attribute on an entity (e.g., Patient), not a standalone table in Phase 1."

    # General rule: require at least one strong signal from name or evidence.
    if name_signal and evidence_signal:
        return "Attribute-like derived/flag concept; evidence suggests it is computed/derived or an attribute, not a table."
    if name_signal and not evidence:
        # Still conservative: name alone can be strong for flags/indicators.
        return "Attribute-like concept by name; likely a derived field or attribute rather than a table."
    if evidence_signal and any(k in name.lower() for k in ["rate", "amount", "score", "multiplier", "threshold", "flag", "indicator"]):
        return "Evidence suggests a derived/computed field; likely an attribute rather than a table."

    return None


@traceable_step("1.76", phase=1, tags=["entity_attribute_guardrail"])
async def step_1_76_entity_attribute_guardrail(
    entities: List,
    nl_description: str,
    domain: Optional[str] = None,
) -> EntityAttributeGuardrailOutput:
    """
    Filter out attribute-like entities deterministically.
    """
    _ = nl_description
    _ = domain

    if not entities:
        return EntityAttributeGuardrailOutput(
            entities=[],
            removed_entity_names=[],
            attribute_candidates=[],
        )

    kept: List = []
    removed_names: List[str] = []
    candidates: List[AttributeCandidateInfo] = []

    for e in entities:
        reason = _should_remove_as_attribute(e)
        if reason:
            name = (e.get("name") if isinstance(e, dict) else getattr(e, "name", "") or "").strip()
            if name:
                removed_names.append(name)
            evidence = (e.get("evidence") if isinstance(e, dict) else getattr(e, "evidence", None) or None)
            candidates.append(
                AttributeCandidateInfo(
                    name=name,
                    reason=reason,
                    evidence=evidence,
                )
            )
            continue
        kept.append(e)

    if removed_names:
        logger.info(f"Step 1.76: Removed {len(removed_names)} attribute-like entities: {removed_names}")

    return EntityAttributeGuardrailOutput(
        entities=kept,
        removed_entity_names=removed_names,
        attribute_candidates=candidates,
    )


