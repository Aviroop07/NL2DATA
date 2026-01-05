"""Phase 2, Step 2.3: Attribute Synonym Detection.

Checks for duplicate or synonymous attributes (e.g., "email" and "email_address").
Prevents redundant columns and schema bloat.
"""

from typing import Dict, Any, List, Optional, Set, Tuple
import json
import re
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.pipeline_config import get_phase2_config
from NL2DATA.utils.similarity import propose_attribute_synonym_candidates

logger = get_logger(__name__)

def _attr_name_set(attributes: List[Dict[str, Any]]) -> Set[str]:
    names: Set[str] = set()
    for attr in attributes:
        if isinstance(attr, dict):
            n = str(attr.get("name", "")).strip()
        else:
            n = str(getattr(attr, "name", "")).strip()
        if n:
            names.add(n)
    return names


def _validate_synonym_output(
    *,
    entity_name: str,
    input_attr_names: Set[str],
    result: "AttributeSynonymOutput",
    allowed_pairs: Optional[Set[Tuple[str, str]]] = None,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []

    # Validate synonyms pairs
    for i, s in enumerate(result.synonyms):
        if s.attr1 not in input_attr_names:
            issues.append(
                {"issue_type": "unknown_attr_in_synonyms", "detail": f"synonyms[{i}].attr1='{s.attr1}' not in input attribute list", "value": s.attr1}
            )
        if s.attr2 not in input_attr_names:
            issues.append(
                {"issue_type": "unknown_attr_in_synonyms", "detail": f"synonyms[{i}].attr2='{s.attr2}' not in input attribute list", "value": s.attr2}
            )
        if s.should_merge and s.preferred_name and s.preferred_name not in input_attr_names:
            issues.append(
                {"issue_type": "unknown_preferred_name", "detail": f"synonyms[{i}].preferred_name='{s.preferred_name}' not in input attribute list", "value": s.preferred_name}
            )
        if s.should_merge and s.preferred_name and s.preferred_name not in {s.attr1, s.attr2}:
            issues.append(
                {"issue_type": "preferred_name_not_in_pair", "detail": f"synonyms[{i}].preferred_name must be either attr1 or attr2 when should_merge=true", "value": s.preferred_name}
            )
        if allowed_pairs is not None:
            pair_key = tuple(sorted([s.attr1, s.attr2]))
            if pair_key not in allowed_pairs:
                issues.append(
                    {"issue_type": "synonym_pair_not_allowed", "detail": f"synonyms[{i}] pair ({s.attr1}, {s.attr2}) is not in allowed candidate pairs", "value": [s.attr1, s.attr2]}
                )

    # Validate merged_attributes
    for a in result.merged_attributes:
        if a not in input_attr_names:
            issues.append(
                {"issue_type": "unknown_merged_attribute", "detail": f"merged_attributes contains '{a}' not in input attribute list", "value": a}
            )

    # Validate final_attribute_list
    for a in result.final_attribute_list:
        if a not in input_attr_names:
            issues.append(
                {"issue_type": "unknown_final_attribute", "detail": f"final_attribute_list contains '{a}' not in input attribute list", "value": a}
            )

    # Validate duplicates in final list (case-insensitive)
    seen_ci: Set[str] = set()
    for a in result.final_attribute_list:
        key = a.lower()
        if key in seen_ci:
            issues.append(
                {"issue_type": "duplicate_in_final_attribute_list", "detail": f"final_attribute_list contains duplicate '{a}' (case-insensitive)", "value": a}
            )
        seen_ci.add(key)

    if issues:
        logger.warning(f"Entity {entity_name}: Step 2.3 validation found {len(issues)} issue(s) in LLM output.")
    return issues


def _build_updated_attributes_from_final_list(
    attributes: List[Dict[str, Any]],
    final_attribute_list: List[str],
) -> List[Dict[str, Any]]:
    """Apply LLM decision deterministically (no heuristics).

    We keep the first occurrence of each name from the original attribute dict list.
    """
    by_name: Dict[str, Dict[str, Any]] = {}
    for attr in attributes:
        if not isinstance(attr, dict):
            continue
        n = str(attr.get("name", "")).strip()
        if n and n not in by_name:
            by_name[n] = attr

    updated: List[Dict[str, Any]] = []
    for n in final_attribute_list:
        if n in by_name:
            updated.append(by_name[n])
        else:
            updated.append({"name": n})
    return updated


class AttributeSynonymInfo(BaseModel):
    """Information about a potential synonym pair."""
    attr1: str = Field(description="Name of first attribute")
    attr2: str = Field(description="Name of second attribute")
    should_merge: bool = Field(description="Whether these attributes should be merged")
    preferred_name: str = Field(description="Preferred name if merging (canonical name)")
    reasoning: str = Field(description="Reasoning for merge decision")


class AttributeSynonymOutput(BaseModel):
    """Output structure for attribute synonym detection."""
    synonyms: List[AttributeSynonymInfo] = Field(
        default_factory=list,
        description="List of synonym pairs with merge decisions"
    )
    merged_attributes: List[str] = Field(
        default_factory=list,
        description="List of attribute names that should be removed after merging (kept attributes are in final_attribute_list)"
    )
    final_attribute_list: List[str] = Field(
        default_factory=list,
        description="Final consolidated list of unique attribute names after merging"
    )


@traceable_step("2.3", phase=2, tags=['phase_2_step_3'])
async def step_2_3_attribute_synonym_detection(
    entity_name: str,
    attributes: List,
    nl_description: Optional[str] = None,
) -> AttributeSynonymOutput:
    """
    Step 2.3 (per-entity): Check for duplicate or synonymous attributes.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of attributes from Step 2.2
        nl_description: Optional original NL description for context
        
    Returns:
        dict: Synonym detection result with synonyms, merged_attributes, and final_attribute_list
        
    Example:
        >>> result = await step_2_3_attribute_synonym_detection(
        ...     "Customer",
        ...     [{"name": "email"}, {"name": "email_address"}]
        ... )
        >>> result["synonyms"][0]["should_merge"]
        True
    """
    logger.debug(f"Detecting attribute synonyms for entity: {entity_name}")
    
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}")
        return AttributeSynonymOutput(
            synonyms=[],
            merged_attributes=[],
            final_attribute_list=[]
        )
    
    # Build attribute list for prompt
    attribute_list_str = ""
    for i, attr in enumerate(attributes, 1):
        attr_name = attr.get("name", "Unknown") if isinstance(attr, dict) else getattr(attr, "name", "Unknown")
        attr_desc = attr.get("description", "") if isinstance(attr, dict) else getattr(attr, "description", "")
        attribute_list_str += f"{i}. {attr_name}"
        if attr_desc:
            attribute_list_str += f": {attr_desc}"
        attribute_list_str += "\n"
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=AttributeSynonymOutput,
        additional_requirements=[
            "If should_merge=true, preferred_name MUST be either attr1 or attr2 (do not invent new names)",
            "The \"reasoning\" field is REQUIRED and cannot be omitted or empty"
        ]
    )
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to identify duplicate attributes, synonyms, or attributes that should be merged to prevent schema bloat and confusion.

Look for:
1. **Exact duplicates**: Same attribute name appearing multiple times
2. **Synonyms**: Different names referring to the same concept
   - Examples: "email" vs "email_address", "name" vs "full_name", "phone" vs "phone_number"
3. **Overlapping attributes**: Attributes that represent the same or very similar concepts
   - Examples: "address" and "mailing_address", "created" and "created_at"
4. **Attributes that should be merged**: Attributes that are better represented as a single attribute

""" + output_structure_section + """

CRITICAL CONSTRAINT: You MUST ONLY reference attribute names that appear in the provided "Attributes to check" list
Do NOT invent new attribute names (e.g., do not create "email_address" if it is not in the list)

Important:
- If two attributes are synonyms, merge them and keep the more standard/canonical name
- If attributes are related but distinct, keep them separate
- Consider domain context when making decisions
- Be conservative: only merge if truly necessary
- Prefer shorter, clearer names when merging"""
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}

Attributes to check:
{{attribute_list}}

Original description (if available):
{{nl_description}}"""
    
    cfg = get_phase2_config()
    input_attr_names = _attr_name_set(attributes)

    # Deterministically propose candidate pairs (semantic similarity) and only ask the LLM to decide on those.
    candidate_pairs: List[Dict[str, Any]] = []
    allowed_pairs: Optional[Set[Tuple[str, str]]] = None
    if cfg.step_2_3_similarity_enabled and len(input_attr_names) >= 2:
        try:
            candidate_pairs = propose_attribute_synonym_candidates(
                attributes=[a if isinstance(a, dict) else {"name": getattr(a, "name", "")} for a in attributes],
                model_name=cfg.step_2_3_similarity_model_name,
                threshold=cfg.step_2_3_similarity_threshold,
                max_pairs=cfg.step_2_3_similarity_max_pairs,
                lexical_min_jaccard=cfg.step_2_3_similarity_lexical_min_jaccard,
                filter_description_pairs=bool(cfg.step_2_3_similarity_filter_description_pairs),
                filter_id_vs_non_id=bool(cfg.step_2_3_similarity_filter_id_vs_non_id),
                filter_id_vs_name=bool(cfg.step_2_3_similarity_filter_id_vs_name),
            )
        except Exception as e:
            # Keep pipeline operational if optional dependency isn't installed.
            logger.warning(
                f"Entity {entity_name}: Step 2.3 similarity candidate generation failed; "
                f"falling back to full-attribute LLM scan. Error: {e}"
            )
            candidate_pairs = []

    if candidate_pairs:
        allowed_pairs = {tuple(sorted([p.get("attr1", ""), p.get("attr2", "")])) for p in candidate_pairs if p.get("attr1") and p.get("attr2")}
        system_prompt = """You are a database design assistant. Your task is to decide whether certain attribute pairs are duplicates/synonyms and should be merged.

You will be given:
- The full attribute list for the entity (for grounding)
- A SHORT list of candidate pairs with similarity scores (these are the ONLY pairs you should evaluate)

For each candidate pair, provide:
- attr1: Name of first attribute (must be from list)
- attr2: Name of second attribute (must be from list)
- should_merge: true only if they represent the same concept
- preferred_name: MUST be either attr1 or attr2 (do not invent names)
- reasoning: REQUIRED

After decisions:
- merged_attributes: list of attribute names that should be removed (the non-preferred names among pairs where should_merge=true)
- final_attribute_list: the final list of attribute names after applying the merges

CRITICAL CONSTRAINTS:
- You MUST ONLY reference attribute names from the provided list.
- You MUST ONLY include synonym pairs that appear in the provided candidate pairs list.
- Be conservative: if unsure, keep them separate."""

        human_prompt = f"""Entity: {entity_name}

Attributes (ground truth list):
{{attribute_list}}

Candidate pairs to evaluate (ONLY these):
{{candidate_pairs_json}}

Original description (if available):
{{nl_description}}"""
    else:
        # If similarity is enabled but we found no suspicious pairs, do a deterministic no-op.
        if cfg.step_2_3_similarity_enabled:
            return AttributeSynonymOutput(
                synonyms=[],
                merged_attributes=[],
                final_attribute_list=[a.get("name") if isinstance(a, dict) else getattr(a, "name", "") for a in attributes],
            )

    # Initialize model and create chain
    llm = get_model_for_step("2.3")  # Step 2.3 maps to "high_fanout" task type
    try:
        config = get_trace_config("2.3", phase=2, tags=["phase_2_step_3"])
        result: AttributeSynonymOutput = await standardized_llm_call(
            llm=llm,
            output_schema=AttributeSynonymOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "attribute_list": attribute_list_str,
                "nl_description": nl_description or "",
                "candidate_pairs_json": json.dumps(candidate_pairs, ensure_ascii=True),
            },
            config=config,
        )

        # Validate and (optionally) ask for a corrected response
        for round_idx in range(cfg.step_2_3_max_revision_rounds):
            issues = _validate_synonym_output(
                entity_name=entity_name,
                input_attr_names=input_attr_names,
                result=result,
                allowed_pairs=allowed_pairs,
            )
            if not issues:
                break

            revision_system_prompt = """You are a database design assistant.

You previously produced an attribute synonym/merge decision payload.
You will now receive:
- The attribute list you MUST use
- Your previous output (JSON)
- A deterministic list of validation issues

Task: Return a corrected JSON output that ONLY references attribute names from the provided list."""

            revision_human_prompt = """Entity: {entity_name}

Allowed attribute names (MUST use only these):
{allowed_names}

Allowed synonym pairs (ONLY these pairs are permitted):
{allowed_pairs}

Previous output (JSON):
{previous_output_json}

Validation issues (JSON):
{issues_json}

Return a corrected JSON output (same schema)."""

            result = await standardized_llm_call(
                llm=llm,
                output_schema=AttributeSynonymOutput,
                system_prompt=revision_system_prompt,
                human_prompt_template=revision_human_prompt,
                input_data={
                    "entity_name": entity_name,
                    "allowed_names": ", ".join(sorted(input_attr_names)),
                    "allowed_pairs": ", ".join([f"({a},{b})" for a, b in sorted(allowed_pairs or set())]) if allowed_pairs is not None else "(no restriction)",
                    "previous_output_json": json.dumps(result.model_dump(), ensure_ascii=True),
                    "issues_json": json.dumps(issues, ensure_ascii=True),
                },
                config=config,
            )

        # Final validation; if still invalid, fall back to no-op merges.
        final_issues = _validate_synonym_output(
            entity_name=entity_name,
            input_attr_names=input_attr_names,
            result=result,
            allowed_pairs=allowed_pairs,
        )
        if final_issues:
            logger.error(
                f"Entity {entity_name}: Step 2.3 output still invalid after retries; "
                f"falling back to no-op merge (keep original attribute list)."
            )
            # No-op fallback (LLM output discarded) - return Pydantic model, not dict
            return AttributeSynonymOutput(
                synonyms=[],
                merged_attributes=[],
                final_attribute_list=[a.get("name") if isinstance(a, dict) else getattr(a, "name", "") for a in attributes],
            )
        
        # Work with Pydantic model directly
        synonym_count = len(result.synonyms)
        merged_count = len(result.merged_attributes)
        final_count = len(result.final_attribute_list)
        
        logger.debug(
            f"Entity {entity_name}: {synonym_count} synonyms found, {merged_count} attributes merged, "
            f"{final_count} final attributes"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error detecting attribute synonyms for entity {entity_name}: {e}", exc_info=True)
        raise


class EntityAttributeSynonymResult(BaseModel):
    """Result for a single entity in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    synonyms: List[AttributeSynonymInfo] = Field(
        default_factory=list,
        description="List of synonym pairs with merge decisions"
    )
    merged_attributes: List[str] = Field(
        default_factory=list,
        description="List of attribute names that should be removed after merging"
    )
    final_attribute_list: List[str] = Field(
        default_factory=list,
        description="Final consolidated list of unique attribute names after merging"
    )


class AttributeSynonymBatchOutput(BaseModel):
    """Output structure for Step 2.3 batch processing."""
    entity_results: List[EntityAttributeSynonymResult] = Field(
        description="List of attribute synonym detection results, one per entity"
    )
    total_entities: int = Field(description="Total number of entities processed")


async def step_2_3_attribute_synonym_detection_batch(
    entities: List,
    entity_attributes: dict,  # entity_name -> attributes
    nl_description: Optional[str] = None,
) -> AttributeSynonymBatchOutput:
    """
    Step 2.3: Detect attribute synonyms for all entities (parallel execution).
    
    Args:
        entities: List of entities
        entity_attributes: Dictionary mapping entity names to their attributes from Step 2.2
        nl_description: Optional original NL description
        
    Returns:
        AttributeSynonymBatchOutput: Synonym detection results for all entities
        
    Example:
        >>> result = await step_2_3_attribute_synonym_detection_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": [{"name": "email"}, {"name": "email_address"}]}
        ... )
        >>> len(result.entity_results)
        1
    """
    logger.info(f"Starting Step 2.3: Attribute Synonym Detection for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for attribute synonym detection")
        return AttributeSynonymBatchOutput(entity_results=[], total_entities=0)
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    task_metadata = []  # Store entity_name for each task
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        attributes = entity_attributes.get(entity_name, [])
        
        task = step_2_3_attribute_synonym_detection(
            entity_name=entity_name,
            attributes=attributes,
            nl_description=nl_description,
        )
        tasks.append(task)
        task_metadata.append(entity_name)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )
    
    # Process results
    entity_results_list = []
    for entity_name, result in zip(task_metadata, results):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            # Get original attributes for fallback
            original_attrs = entity_attributes.get(entity_name, [])
            original_attr_names = [a.get("name", "") if isinstance(a, dict) else getattr(a, "name", "") for a in original_attrs]
            entity_results_list.append(
                EntityAttributeSynonymResult(
                    entity_name=entity_name,
                    synonyms=[],
                    merged_attributes=[],
                    final_attribute_list=original_attr_names,
                )
            )
        else:
            # Ensure result is a Pydantic model, not a dict
            if isinstance(result, dict):
                logger.warning(f"Entity {entity_name}: Received dict instead of Pydantic model, converting...")
                result = AttributeSynonymOutput.model_validate(result)
            
            entity_results_list.append(
                EntityAttributeSynonymResult(
                    entity_name=entity_name,
                    synonyms=result.synonyms,
                    merged_attributes=result.merged_attributes,
                    final_attribute_list=result.final_attribute_list,
                )
            )
    
    total_merged = sum(len(r.merged_attributes) for r in entity_results_list)
    logger.info(f"Attribute synonym detection completed: {total_merged} attributes merged across {len(entity_results_list)} entities")
    
    return AttributeSynonymBatchOutput(
        entity_results=entity_results_list,
        total_entities=len(entities),
    )

