"""Phase 1, Step 1.5: Relation Mention Detection.

Checks if relationships between entities are explicitly mentioned in the description.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import (
    _verify_evidence_substring_impl,
    _verify_entity_in_known_entities_impl,
)
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class RelationWithEvidence(BaseModel):
    subject: str = Field(description="Canonical entity name (from KnownEntities)")
    predicate: str = Field(description="Relationship verb/phrase (e.g., deployed_in, experiences, resets)")
    object: str = Field(description="Canonical entity name (from KnownEntities)")
    evidence: str = Field(description="Verbatim evidence snippet (<= 30 words) copied from description")

    model_config = ConfigDict(extra="forbid")


class RelationMentionOutput(BaseModel):
    """Output structure for relation mention detection."""
    has_explicit_relations: bool = Field(description="Whether relationships are explicitly mentioned in the description")
    relations: List[RelationWithEvidence] = Field(
        default_factory=list,
        description="List of explicitly stated relations with evidence"
    )
    reasoning: str = Field(description="Reasoning (<= 25 words) referencing evidence")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.5", phase=1, tags=["relation_mention_detection"])
async def step_1_5_relation_mention_detection(
    nl_description: str,
    entities: Optional[List] = None,
) -> RelationMentionOutput:
    """
    Step 1.5: Check if relationships between entities are explicitly mentioned.
    
    This step helps identify which relations are user-specified vs. need inference.
    
    Args:
        nl_description: Natural language description of the database requirements
        entities: Optional list of entities from Step 1.4 for context
        
    Returns:
        dict: Relation mention detection result with has_explicit_relations and mentioned_relations
        
    Example:
        >>> result = await step_1_5_relation_mention_detection(
        ...     "Customers place orders. Orders contain products."
        ... )
        >>> result["has_explicit_relations"]
        True
        >>> result["mentioned_relations"]
        ["Customers place orders", "Orders contain products"]
    """
    logger.info("Starting Step 1.5: Relation Mention Detection")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    known_entities: List[str] = []
    if entities:
        for e in entities:
            if isinstance(e, dict):
                n = (e.get("name") or "").strip()
            else:
                n = (getattr(e, "name", "") or "").strip()
            if n:
                known_entities.append(n)
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=RelationMentionOutput,
        additional_requirements=[
            "Grounding rule (critical): For every relation, evidence MUST be copied verbatim from the input (exact substring; preserve casing/spaces)",
            "Subject and object MUST be from KnownEntities list (verify with tool)",
            "Reasoning must be <= 25 words"
        ]
    )
    
    system_prompt = """You are an information extraction engine.

Task: Extract ONLY relationships that are explicitly stated in the natural-language description between the provided entities.

Definition: A relationship is "explicit" ONLY if the description contains a direct relationship phrase/verb connecting two entities (e.g., "X deployed across Y", "Y experiences X", "events reset X").
Do NOT infer relationships based on typical database design, star schemas, or domain knowledge.
Do NOT invent relationships such as foreign keys unless the description explicitly states them.

Inputs you will receive:
- Natural language description
- KnownEntities: a list of canonical entity names you must use

Rules:
- Each relation must reference entities from KnownEntities (no new entity names).
- Each relation must include a short verbatim evidence snippet copied from the description.
- If you cannot find any explicit relations, return has_explicit_relations=false and an empty relations list.

Hard constraint reminder:
"Only output relations with direct evidence spans. Do not infer star-schema joins."

Tool usage (mandatory when has_explicit_relations = true)
You have access to two tools:
1) verify_evidence_substring(evidence: str, nl_description: str) -> {is_substring: bool, error: str|null}
2) verify_entity_in_known_entities(entity: str, known_entities: List[str]) -> {exists: bool, error: str|null}

Before finalizing your response:
1) For EACH relation in relations:
   a) Call verify_entity_in_known_entities for BOTH subject and object:
      {"entity": "<relation.subject>", "known_entities": [<KnownEntities_list>]}
      {"entity": "<relation.object>", "known_entities": [<KnownEntities_list>]}
   b) If exists = false for either subject or object, correct the entity name to match one from KnownEntities, then re-check.
2) For EACH relation, call verify_evidence_substring with:
   {"evidence": "<relation.evidence>", "nl_description": "<full_nl_description>"}
3) If is_substring = false for any relation, correct the evidence to be an exact substring from nl_description, then re-check.

""" + output_structure_section
    
    # Human prompt template
    human_prompt = """Natural language description:
{nl_description}

KnownEntities: {known_entities}
"""
    
    # Initialize model
    llm = get_model_for_step("1.5")  # Step 1.5 maps to "simple" task type
    
    try:
        logger.debug("Invoking LLM for relation mention detection")
        config = get_trace_config("1.5", phase=1, tags=["relation_mention_detection"])
        result: RelationMentionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=RelationMentionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description, "known_entities": known_entities},
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic enforcement:
        # - subject/object must be in known_entities
        # - evidence must be a verbatim substring
        # - dedupe by (subject, predicate, object, evidence) case-insensitively
        filtered: List[RelationWithEvidence] = []
        seen: set[str] = set()
        for rel in result.relations or []:
            subj = (rel.subject or "").strip()
            obj = (rel.object or "").strip()
            ev = (rel.evidence or "").strip()

            if not subj or not obj or not ev:
                continue

            if not _verify_entity_in_known_entities_impl(subj, known_entities).get("exists", False):
                continue
            if not _verify_entity_in_known_entities_impl(obj, known_entities).get("exists", False):
                continue
            if not _verify_evidence_substring_impl(ev, nl_description).get("is_substring", False):
                continue

            key = "|".join([subj.lower(), (rel.predicate or "").strip().lower(), obj.lower(), ev.lower()])
            if key in seen:
                continue
            seen.add(key)
            filtered.append(rel)

        result = result.model_copy(
            update={
                "has_explicit_relations": len(filtered) > 0,
                "relations": filtered,
            }
        )
        
        # Work with Pydantic model directly
        logger.info(f"Relation mention detection completed: has_explicit_relations={result.has_explicit_relations}")
        if result.relations:
            logger.info(f"Found {len(result.relations)} explicitly mentioned relations")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in relation mention detection: {e}", exc_info=True)
        raise

