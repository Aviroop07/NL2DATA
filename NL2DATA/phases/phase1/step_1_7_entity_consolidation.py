"""Phase 1, Step 1.7: Entity Consolidation.

Checks for duplicate entities, synonyms, or entities that should be merged.
Prevents schema bloat and confusion.
"""

from typing import List, Optional, Tuple, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    extract_entity_description,
)

logger = get_logger(__name__)

_COMMON_SYNONYM_PAIRS: List[Tuple[str, str]] = [
    ("user", "customer"),
    ("client", "customer"),
    ("person", "user"),
    ("item", "product"),
    ("order", "transaction"),
    ("purchase", "order"),
]


def _name_similarity(entity1: str, entity2: str) -> Dict[str, Any]:
    """
    Lightweight, deterministic approximation of the former tool `check_entity_name_similarity`.
    Returns: {"similarity": float, "are_synonyms": bool}
    """
    from difflib import SequenceMatcher

    n1 = (entity1 or "").strip().lower()
    n2 = (entity2 or "").strip().lower()

    if not n1 or not n2:
        return {"similarity": 0.0, "are_synonyms": False}

    if n1 == n2:
        return {"similarity": 1.0, "are_synonyms": True}

    # Explicit synonym pairs (order-insensitive)
    for a, b in _COMMON_SYNONYM_PAIRS:
        if (n1 == a and n2 == b) or (n1 == b and n2 == a):
            return {"similarity": 0.95, "are_synonyms": True}

    sim = SequenceMatcher(None, n1, n2).ratio()
    are_synonyms = sim >= 0.85
    return {"similarity": round(float(sim), 3), "are_synonyms": bool(are_synonyms)}


def _build_candidate_pairs(names: List[str]) -> List[Dict[str, Any]]:
    """
    Build a small, high-signal set of candidate duplicate pairs to keep the LLM focused.
    """
    candidates: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()

    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            e1 = names[i]
            e2 = names[j]
            key = (e1, e2)
            if key in seen:
                continue
            seen.add(key)

            sim = _name_similarity(e1, e2)
            if sim["are_synonyms"] or sim["similarity"] >= 0.80:
                candidates.append(
                    {
                        "entity1": e1,
                        "entity2": e2,
                        "similarity": sim["similarity"],
                        "are_synonyms": sim["are_synonyms"],
                    }
                )

    # Sort most similar first for readability
    candidates.sort(key=lambda x: (x.get("are_synonyms", False), x.get("similarity", 0.0)), reverse=True)
    return candidates


class MergeDecisionEvidence(BaseModel):
    definition_overlap: str = Field(description="How the definitions overlap (or differ)")
    grain_conflict: Optional[str] = Field(default=None, description="Grain conflict if present (e.g., Type vs Instance)")
    counterexample: Optional[str] = Field(default=None, description="Counterexample if NOT merging (why distinct)")

    model_config = ConfigDict(extra="forbid")


class MergeDecision(BaseModel):
    entity1: str = Field(description="Name of first entity")
    entity2: str = Field(description="Name of second entity")
    similarity: float = Field(ge=0.0, le=1.0, description="Similarity score (0.0 to 1.0)")
    should_merge: bool = Field(description="Whether these entities should be merged")
    merged_entity_name: Optional[str] = Field(default=None, description="Canonical name if merging, else null")
    evidence: MergeDecisionEvidence = Field(description="Audit evidence for the decision")
    reasoning: str = Field(description="Reasoning for merge decision")

    model_config = ConfigDict(extra="forbid")


class RenameSuggestion(BaseModel):
    from_name: str = Field(alias="from", description="Current entity name")
    to: str = Field(description="Suggested new name")
    reason: str = Field(description="Reason for rename")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class EntityConsolidationOutput(BaseModel):
    """Output structure for entity consolidation."""
    merge_decisions: List[MergeDecision] = Field(
        default_factory=list,
        description="Merge decisions for candidate duplicate pairs"
    )
    rename_suggestions: List[RenameSuggestion] = Field(
        default_factory=list,
        description="Rename suggestions for clarity"
    )
    final_entities: List[str] = Field(
        default_factory=list,
        description="Final consolidated list of unique entity names"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.7", phase=1, tags=["entity_consolidation"])
async def step_1_7_entity_consolidation(
    key_entities: List,
    auxiliary_entities: Optional[List] = None,
    domain: Optional[str] = None,
    nl_description: Optional[str] = None,
) -> EntityConsolidationOutput:
    """
    Step 1.7: Check for duplicate entities, synonyms, or entities that should be merged.
    
    This step prevents schema bloat and confusion by identifying:
    - Duplicate entities (same name, different descriptions)
    - Synonym entities (e.g., "User" vs "Customer")
    - Entities that should be merged for normalization
    
    Args:
        key_entities: List of key entities from Step 1.4
        auxiliary_entities: Optional list of auxiliary entities from Step 1.6
        domain: Optional domain context from Steps 1.1 or 1.3
        nl_description: Optional NL description for additional context
        
    Returns:
        dict: Consolidation result with duplicates, merged_entities, and final_entity_list
        
    Example:
        >>> result = await step_1_7_entity_consolidation(
        ...     key_entities=[{"name": "User", "description": "..."}],
        ...     auxiliary_entities=[{"name": "Customer", "description": "..."}]
        ... )
        >>> result["duplicates"][0]["should_merge"]
        True
    """
    logger.info("Starting Step 1.7: Entity Consolidation")
    
    # Combine all entities
    all_entities = []
    if key_entities:
        all_entities.extend(key_entities)
    if auxiliary_entities:
        all_entities.extend(auxiliary_entities)
    
    if not all_entities:
        logger.warning("No entities provided for consolidation")
        return EntityConsolidationOutput(
            merge_decisions=[],
            rename_suggestions=[],
            final_entities=[]
        )
    
    logger.debug(f"Consolidating {len(all_entities)} entities ({len(key_entities) if key_entities else 0} key, {len(auxiliary_entities) if auxiliary_entities else 0} auxiliary)")
    
    # Build compact entity registry with origin tags (high-signal)
    registry_items: List[Dict[str, str]] = []
    for entity in key_entities or []:
        n = extract_entity_name(entity)
        d = extract_entity_description(entity, default="No description")
        mention_type = (entity.get("mention_type") if isinstance(entity, dict) else None) or "inferred"
        origin = "explicit" if mention_type == "explicit" else "inferred"
        registry_items.append({"name": n, "description": d, "origin": origin})
    for entity in auxiliary_entities or []:
        n = extract_entity_name(entity)
        d = extract_entity_description(entity, default="No description")
        registry_items.append({"name": n, "description": d, "origin": "auxiliary"})

    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=EntityConsolidationOutput,
        additional_requirements=[
            "If merging, merged_entity_name MUST be one of entity1 or entity2 (do not invent new names)",
            "Every merge decision must include evidence: definition_overlap + grain check + counterexample if NOT merging"
        ]
    )

    # System prompt
    # NOTE: We intentionally avoid domain/nl_description to reduce bias-driven merges.
    system_prompt = """You are an entity deduplication assistant for database schema extraction.

Input includes:
1) EntityRegistry: a list of candidate entities with 1-line definitions and origin tags.
2) CandidatePairs: a precomputed, deterministic list of potentially duplicate pairs with similarity hints.

Your job: decide which entities should be merged, which should remain distinct, and whether any entities should be renamed for clarity.

Rules:
- Only merge if they represent the same real-world concept at the same grain (e.g., both are "event", or both are "type/lookup", or both are "physical object instance").
- Do not merge merely because names are similar. Similarity scores are hints, not proof.
- If two entities are related but different grains (e.g., "Type" vs "Instance", "Lookup" vs "Event", "Header" vs "Junction"), they must remain separate.
- If you merge, set merged_entity_name to ONE OF the ORIGINAL entity names (entity1 or entity2). Do not invent new names.
- Every decision must include evidence: definition_overlap + grain check + at least one counterexample if you choose NOT to merge.

""" + output_structure_section
    
    # Human prompt template: structured registry only
    import json
    human_prompt = """EntityRegistry (JSON):
{entity_registry_json}

CandidatePairs (JSON):
{candidate_pairs_json}
"""
    
    # Initialize model
    llm = get_model_for_step("1.7")  # Step 1.7 maps to "reasoning" task type
    
    try:
        logger.debug("Invoking LLM for entity consolidation")
        config = get_trace_config("1.7", phase=1, tags=["entity_consolidation"])

        candidate_pairs = _build_candidate_pairs([x["name"] for x in registry_items if x.get("name")])

        result: EntityConsolidationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityConsolidationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "entity_registry_json": json.dumps(registry_items, indent=2, ensure_ascii=False),
                "candidate_pairs_json": json.dumps(candidate_pairs, indent=2, ensure_ascii=False),
            },
            tools=None,
            use_agent_executor=False,
            config=config,
        )
        
        # Work with Pydantic model directly
        duplicate_count = len(result.merge_decisions)
        final_count = len(result.final_entities)
        
        logger.info(f"Entity consolidation completed: {duplicate_count} merge decisions, {final_count} final entities")
        
        if duplicate_count > 0:
            logger.info(f"Merge decisions: {[d.entity1 + ' <-> ' + d.entity2 for d in result.merge_decisions]}")
            for d in result.merge_decisions:
                logger.info(
                    "Merge decision: entity1=%s entity2=%s should_merge=%s merged_entity_name=%s reasoning=%s",
                    d.entity1,
                    d.entity2,
                    d.should_merge,
                    d.merged_entity_name,
                    d.reasoning,
                )

        logger.info("Final entities: %s", result.final_entities)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in entity consolidation: {e}", exc_info=True)
        raise

