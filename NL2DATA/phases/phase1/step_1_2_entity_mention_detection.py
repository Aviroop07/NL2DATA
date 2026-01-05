"""Phase 1, Step 1.2: Entity Mention Detection.

Checks if entities are explicitly named in the natural language description.
"""

from typing import List, Iterable, Tuple
import re
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _verify_evidence_substring_impl
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class EntityWithEvidence(BaseModel):
    name: str = Field(description="Canonical form derived only from evidence")
    evidence: str = Field(description="Verbatim substring copied from the input")

    model_config = ConfigDict(extra="forbid")


class EntityMentionOutput(BaseModel):
    """Output structure for entity mention detection."""
    has_explicit_entities: bool = Field(description="Whether entities are explicitly mentioned in the description")
    mentioned_entities: List[EntityWithEvidence] = Field(
        default_factory=list,
        description="List of explicitly mentioned entities with evidence"
    )
    reasoning: str = Field(description="Reasoning (<= 25 words)")

    model_config = ConfigDict(extra="forbid")


def _canonicalize_name_from_evidence(evidence: str) -> str:
    """
    Canonicalize an entity name derived ONLY from evidence.
    Allowed transforms: trim whitespace, remove surrounding quotes,
    convert spaces to underscores. Do not add words.
    """
    ev = (evidence or "").strip()
    ev = ev.strip('"').strip("'").strip()
    ev = re.sub(r"\s+", " ", ev)
    return ev.replace(" ", "_")


def _iter_list_items_with_spans(list_text: str, base_start: int) -> Iterable[Tuple[str, int, int]]:
    """
    Split a comma-separated list like:
      "purchase orders, shipments, inventory snapshots, and customer orders"
    into item spans relative to the original nl_description (via base_start).

    Returns tuples of (item_text, abs_start, abs_end) where item_text is the
    verbatim substring slice and [abs_start:abs_end] is the slice in the original
    description.
    """
    if not list_text:
        return

    i = 0
    n = len(list_text)
    while i < n:
        j = list_text.find(",", i)
        if j == -1:
            j = n

        seg_start = i
        seg_end = j
        i = j + 1  # skip comma

        # Trim whitespace bounds.
        while seg_start < seg_end and list_text[seg_start].isspace():
            seg_start += 1
        while seg_end > seg_start and list_text[seg_end - 1].isspace():
            seg_end -= 1
        if seg_end <= seg_start:
            continue

        # Remove leading conjunction "and " / "& " (common in final item).
        lower = list_text[seg_start:seg_end].lower()
        if lower.startswith("and "):
            seg_start += 4
            while seg_start < seg_end and list_text[seg_start].isspace():
                seg_start += 1
        elif lower.startswith("& "):
            seg_start += 2
            while seg_start < seg_end and list_text[seg_start].isspace():
                seg_start += 1

        if seg_end <= seg_start:
            continue

        abs_start = base_start + seg_start
        abs_end = base_start + seg_end
        yield list_text[seg_start:seg_end], abs_start, abs_end


def _extract_explicit_entities_deterministic(nl_description: str) -> List[EntityWithEvidence]:
    """
    Deterministically extract explicit entity mentions from common list patterns
    like "fact tables for X, Y, and Z" and "dimension tables for A, B, and C".

    This complements the LLM output and reduces false negatives caused by
    evidence substring grounding (LLM often returns "fact tables for shipments",
    which is not a verbatim substring).
    """
    text = nl_description or ""
    found: List[EntityWithEvidence] = []

    patterns = [
        # Fact tables list: stop before "and dimension tables" if present.
        r"\bfact tables?\s+for\s+(?P<list>.+?)(?:(?:,?\s+and\s+dimension tables?\b)|[.;\n])",
        # Dimension tables list.
        r"\bdimension tables?\s+for\s+(?P<list>.+?)(?:[.;\n])",
        # Common shorthand: "dimensions for A, B, and C"
        r"\bdimensions?\s+for\s+(?P<list>.+?)(?:[.;\n])",
        # Explicit table called X.
        r"\btable\s+called\s+(?P<list>[A-Za-z0-9_]+)\b",
        r"\bfact\s+table\s+called\s+(?P<list>[A-Za-z0-9_]+)\b",
        r"\bdimension\s+table\s+called\s+(?P<list>[A-Za-z0-9_]+)\b",
    ]

    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE | re.DOTALL):
            list_span = m.span("list")
            list_text = (m.group("list") or "")
            if not list_text.strip():
                continue

            # Single-token case.
            if "," not in list_text and " and " not in list_text.lower():
                ev = text[list_span[0]:list_span[1]].strip()
                if ev:
                    found.append(EntityWithEvidence(name=_canonicalize_name_from_evidence(ev), evidence=ev))
                continue

            # List case: split on commas (and handle leading "and").
            for _item_text, abs_start, abs_end in _iter_list_items_with_spans(list_text, base_start=list_span[0]):
                ev = text[abs_start:abs_end].strip()
                if not ev:
                    continue
                found.append(EntityWithEvidence(name=_canonicalize_name_from_evidence(ev), evidence=ev))

    return found


@traceable_step("1.2", phase=1, tags=["entity_mention_detection"])
async def step_1_2_entity_mention_detection(nl_description: str) -> EntityMentionOutput:
    """
    Step 1.2: Check if entities are explicitly named in the description.
    
    This step separates explicit mentions from implicit concepts, allowing
    prioritized extraction of explicitly mentioned entities.
    
    Args:
        nl_description: Natural language description of the database requirements
        
    Returns:
        dict: Entity mention detection result with has_explicit_entities and mentioned_entities
        
    Example:
        >>> result = await step_1_2_entity_mention_detection("I need Customer and Order tables")
        >>> result["has_explicit_entities"]
        True
        >>> result["mentioned_entities"]
        ["Customer", "Order"]
    """
    logger.info("Starting Step 1.2: Entity Mention Detection")
    logger.debug(f"Input description length: {len(nl_description)} characters")

    # Deterministic extraction pass (high recall for "fact tables for ..." / "dimension tables for ..." lists).
    deterministic_candidates: List[EntityWithEvidence] = []
    try:
        deterministic_candidates = _extract_explicit_entities_deterministic(nl_description)
    except Exception as e:
        logger.warning(f"Deterministic explicit entity extraction failed: {e}")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=EntityMentionOutput,
        additional_requirements=[
            "Grounding rule (critical): For every item, evidence MUST be copied verbatim from the input (exact substring; preserve casing/spaces)",
            "Reasoning must be <= 25 words",
            "name must be a canonical form derived ONLY from evidence (allowed transforms: trim whitespace, remove surrounding quotes, convert spaces to underscores). Do not add words."
        ]
    )
    
    system_prompt = """You are a database design assistant.

Task
Extract entity names that are EXPLICITLY mentioned in the input description (table names, entity names, or business concepts that should become tables).

Definition of "explicit"
An entity is explicit only if it appears as a verbatim noun/noun-phrase in the input text (e.g., "sensor_reading", "sensors", "plants", "maintenance events").
Do NOT infer entities that are not explicitly stated.
Do NOT treat attributes/metrics as entities (e.g., temperature, vibration, timestamps, percentages, row counts).

""" + output_structure_section + """

Selection rules
- Include each unique entity once (deduplicate by evidence case-insensitively).
- If the input explicitly says "table called X" or "fact/dimension table", include that table name.
- If no explicit entities exist, set has_explicit_entities=false and mentioned_entities=[].

Reasoning constraints
- <= 25 words.
- Must reference the evidence (by quoting at least one evidence string if any exist).

No extra text. No markdown. No code fences."""
    
    # Human prompt template
    human_prompt = "Natural language description:\n{nl_description}"
    
    # Initialize model
    llm = get_model_for_step("1.2")  # Step 1.2 maps to "simple" task type
    
    try:
        logger.debug("Invoking LLM for entity mention detection")
        config = get_trace_config("1.2", phase=1, tags=["entity_mention_detection"])
        result: EntityMentionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityMentionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic grounding enforcement + merge with deterministic candidates.
        merged: List[EntityWithEvidence] = []
        seen_evidence_lower: set[str] = set()
        seen_entity_key: set[str] = set()
        seen_name_lower: set[str] = set()

        def _entity_key_from_evidence(evidence: str) -> str:
            """
            Derive a best-effort "entity key" from evidence for deduping.
            This does NOT change the returned evidence/name; it only prevents
            duplicates like:
              - "products" vs "dimension tables for products"
              - "purchase orders" vs "fact tables for purchase orders"
            """
            ev = (evidence or "").strip().strip('"').strip("'").strip()
            ev = ev.strip(" ,.;")
            ev_lower = ev.lower()
            for prefix in [
                "fact tables for ",
                "dimension tables for ",
                "dimensions for ",
                "fact table for ",
                "dimension table for ",
                "table called ",
                "fact table called ",
                "dimension table called ",
            ]:
                if ev_lower.startswith(prefix):
                    ev = ev[len(prefix):].strip().strip(" ,.;")
                    break
            ev = re.sub(r"\s+", " ", ev).strip().lower()
            return ev

        def _try_add(ent: EntityWithEvidence) -> None:
            ev = (ent.evidence or "").strip()
            if not ev:
                return
            name_key = (ent.name or "").strip().lower()
            if name_key and name_key in seen_name_lower:
                return
            # Guardrail: LLM sometimes returns list-evidence like:
            #   "dimensions for customer, product, store, ..."
            # This is technically a substring, but it is NOT a single entity mention.
            # We already deterministically extract each list item as its own entity.
            ev_lower = ev.lower()
            if ev_lower.startswith(("dimensions for ", "dimension tables for ", "fact tables for ")) and (
                "," in ev or " and " in ev_lower
            ):
                return
            check = _verify_evidence_substring_impl(ev, nl_description)
            if not check.get("is_substring", False):
                return

            entity_key = _entity_key_from_evidence(ev)
            if entity_key and entity_key in seen_entity_key:
                return

            key = ev.lower()
            if key in seen_evidence_lower:
                return
            seen_evidence_lower.add(key)
            if entity_key:
                seen_entity_key.add(entity_key)
            if name_key:
                seen_name_lower.add(name_key)
            merged.append(ent)

        # Prefer deterministic candidates first (they are constructed as verbatim spans).
        for ent in (deterministic_candidates or []):
            _try_add(ent)

        # Then include LLM candidates.
        for ent in (result.mentioned_entities or []):
            _try_add(ent)

        has_any = len(merged) > 0
        result = result.model_copy(
            update={
                "has_explicit_entities": has_any,
                "mentioned_entities": merged,
            }
        )
        
        # Work with Pydantic model directly
        logger.info(f"Entity mention detection completed: has_explicit_entities={result.has_explicit_entities}")
        if result.mentioned_entities:
            logger.info(f"Found {len(result.mentioned_entities)} explicitly mentioned entities: {result.mentioned_entities}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in entity mention detection: {e}", exc_info=True)
        raise

