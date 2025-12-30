"""Phase 1, Step 1.11: Relation Cardinality & Participation.

Determines relationship cardinality (1 or N) and participation (total or partial) for each entity in the relation.
"""

from typing import Dict, Any, List, Optional, Literal, Tuple
import re
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
)
# NOTE:
# Avoid LangChain @tool functions in internal Python flow. They are StructuredTools and not callable
# like normal Python functions. We do deterministic post-validation instead.

logger = get_logger(__name__)


class RelationCardinalityOutput(BaseModel):
    """Output structure for relation cardinality and participation."""
    entity_cardinalities: Dict[str, Literal["1", "N"]] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their cardinality in the relation ('1' for one, 'N' for many)"
    )
    entity_participations: Dict[str, Literal["total", "partial"]] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their participation type ('total' means every instance must participate, 'partial' means some instances may not participate)"
    )
    reasoning: str = Field(description="Reasoning for the cardinality and participation decisions")

    model_config = ConfigDict(extra="forbid")


def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().split())


def _entity_name_variants(entity_name: str) -> List[str]:
    """
    Generate lightweight variants so simple regex heuristics can match names in prose.
    Example: "SensorReading" -> ["sensorreading", "sensor reading", "sensor_reading", "sensor readings", ...]
    """
    name = (entity_name or "").strip()
    if not name:
        return []

    # CamelCase -> words
    spaced = re.sub(r"(?<!^)([A-Z])", r" \1", name).strip()
    snake = re.sub(r"(?<!^)([A-Z])", r"_\1", name).strip().lower()

    base_variants = {
        name.lower(),
        spaced.lower(),
        snake.lower(),
    }

    variants: set[str] = set()
    for v in base_variants:
        v = v.strip()
        if not v:
            continue
        variants.add(v)
        # naive plural
        if not v.endswith("s"):
            variants.add(v + "s")
    return sorted(variants)


def _infer_cardinalities_from_text(
    *,
    entities: List[str],
    rel_type: str,
    rel_text: str,
    entity_descriptions: Optional[Dict[str, str]] = None,
) -> Dict[str, Literal["1", "N"]]:
    """
    Deterministic fallback for missing cardinality fields.

    Strategy:
    - Use relation type when available.
    - For 1:N / N:1, try to infer direction from text heuristics like:
      - "each X ... one Y" => X=N, Y=1
      - "a Y can have many X" => Y=1, X=N
    - If direction can't be inferred, fall back to first=N, second=1 for 1:N-like types.
    """
    ents = [e for e in (entities or []) if str(e).strip()]
    if not ents:
        return {}

    rel_type_norm = (rel_type or "").strip().lower().replace("_", "-")
    text = _normalize_text(rel_text).lower()

    # Normalize common aliases
    if rel_type_norm in {"1:n", "one-many", "one-to-many", "one to many"}:
        rel_type_norm = "one-to-many"
    elif rel_type_norm in {"n:1", "many-one", "many-to-one", "many to one"}:
        rel_type_norm = "many-to-one"
    elif rel_type_norm in {"n:m", "many-many", "many-to-many", "many to many"}:
        rel_type_norm = "many-to-many"
    elif rel_type_norm in {"1:1", "one-one", "one-to-one", "one to one"}:
        rel_type_norm = "one-to-one"

    # Quick type-based defaults that don't need direction.
    if rel_type_norm == "one-to-one":
        return {e: "1" for e in ents}
    if rel_type_norm == "many-to-many":
        return {e: "N" for e in ents}

    # Helpers used for binary inference (even when rel_type is unknown).
    def matches_each_x_one_y(x_vars: List[str], y_vars: List[str]) -> bool:
        for xv in x_vars:
            for yv in y_vars:
                # keep it simple and not too expensive
                pat = rf"\beach\s+{re.escape(xv)}\b.*\bone\s+{re.escape(yv)}\b"
                if re.search(pat, text):
                    return True
        return False

    def matches_y_has_many_x(y_vars: List[str], x_vars: List[str]) -> bool:
        for yv in y_vars:
            for xv in x_vars:
                pat = rf"\b{re.escape(yv)}\b.*\b(can have|has)\b.*\b(many|multiple|several)\b.*\b{re.escape(xv)}\b"
                if re.search(pat, text):
                    return True
        return False

    def score_manyness(entity: str) -> int:
        """
        Heuristic score: higher => more likely to be the "many"/fact-like side.
        Uses name + description keywords.
        """
        desc = (entity_descriptions or {}).get(entity, "") or ""
        hay = f"{entity} {desc}".lower()
        many_kw = [
            "fact",
            "event",
            "reading",
            "log",
            "record",
            "measurement",
            "telemetry",
            "transaction",
        ]
        one_kw = [
            "dimension",
            "type",
            "category",
            "lookup",
            "reference",
            "master",
        ]
        score = 0
        score += sum(2 for k in many_kw if k in hay)
        score -= sum(2 for k in one_kw if k in hay)
        return score

    # Binary inference (directional when possible).
    if len(ents) == 2:
        a, b = ents[0], ents[1]
        a_vars = _entity_name_variants(a)
        b_vars = _entity_name_variants(b)

        # Text-based direction (independent of rel_type label)
        if matches_each_x_one_y(a_vars, b_vars) or matches_y_has_many_x(b_vars, a_vars):
            return {a: "N", b: "1"}
        if matches_each_x_one_y(b_vars, a_vars) or matches_y_has_many_x(a_vars, b_vars):
            return {a: "1", b: "N"}

        # Type-based default direction if rel_type suggests it
        if rel_type_norm in {"one-to-many", "many-to-one"}:
            return {a: "N", b: "1"}  # assume list is [many, one]

        # Name/description-based guess (useful when rel_type is a free-text predicate like "deployed across")
        a_score = score_manyness(a)
        b_score = score_manyness(b)
        if a_score != b_score:
            return {a: "N", b: "1"} if a_score > b_score else {a: "1", b: "N"}

    # Unknown / n-ary fallback: default to N to avoid under-constraining
    return {e: "N" for e in ents}


def _infer_participations_from_cardinalities(
    *,
    entities: List[str],
    entity_cardinalities: Dict[str, str],
    rel_text: str,
) -> Dict[str, Literal["total", "partial"]]:
    """
    Deterministic fallback for missing participation fields.

    Default heuristic:
    - "N" side tends to be total (facts usually must reference dimensions)
    - "1" side tends to be partial (not every dimension instance necessarily participates)

    If text strongly implies mandatory participation (contains "must"), we prefer "total"
    for the mentioned entity.
    """
    ents = [e for e in (entities or []) if str(e).strip()]
    text = _normalize_text(rel_text).lower()

    out: Dict[str, Literal["total", "partial"]] = {}
    for e in ents:
        card = (entity_cardinalities or {}).get(e)
        out[e] = "total" if card == "N" else "partial"

    # Very light mandatory override (best-effort)
    if "must" in text and ents:
        for e in ents:
            for ev in _entity_name_variants(e):
                if re.search(rf"\b{re.escape(ev)}\b.*\bmust\b", text) or re.search(rf"\bmust\b.*\b{re.escape(ev)}\b", text):
                    out[e] = "total"
                    break

    return out


def _validate_relation_cardinality_output_impl(
    *,
    entities: List[str],
    entity_cardinalities: Dict[str, str],
    entity_participations: Dict[str, str],
) -> Dict[str, Any]:
    missing: List[str] = []
    bad_vals: List[str] = []
    ents = [str(e) for e in (entities or []) if str(e).strip()]

    for e in ents:
        if e not in (entity_cardinalities or {}):
            missing.append(f"missing cardinality for '{e}'")
        else:
            if entity_cardinalities[e] not in ("1", "N"):
                bad_vals.append(f"invalid cardinality for '{e}': {entity_cardinalities[e]}")

        if e not in (entity_participations or {}):
            missing.append(f"missing participation for '{e}'")
        else:
            if entity_participations[e] not in ("total", "partial"):
                bad_vals.append(f"invalid participation for '{e}': {entity_participations[e]}")

    if missing or bad_vals:
        return {"valid": False, "error": "; ".join(missing + bad_vals)}
    return {"valid": True, "error": None}


@traceable_step("1.11", phase=1, tags=["relation_cardinality"])
async def step_1_11_relation_cardinality_single(
    relation: Dict[str, Any],
    entities: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.11 (per-relation): Determine cardinality and participation for each entity in a relation.
    
    This is designed to be called in parallel for multiple relations.
    
    Args:
        relation: Relation dictionary with entities, type, description, etc.
        entities: List of all entities for context
        nl_description: Optional original NL description
        
    Returns:
        dict: Cardinality and participation information with entity_cardinalities, entity_participations, and reasoning
        
    Example:
        >>> result = await step_1_11_relation_cardinality_single(
        ...     relation={"entities": ["Customer", "Order"], "type": "one-to-many"},
        ...     entities=[{"name": "Customer"}, {"name": "Order"}]
        ... )
        >>> result["entity_cardinalities"]["Customer"]
        "1"
        >>> result["entity_cardinalities"]["Order"]
        "N"
        >>> result["entity_participations"]["Order"]
        "total"
    """
    entities_in_rel = relation.get("entities", [])
    rel_type = relation.get("type", "unknown")
    rel_description = relation.get("description", "")
    
    # Create relation_id from entities for tracing
    relation_id = "_".join(sorted(entities_in_rel)) if entities_in_rel else "unknown"
    
    logger.debug(f"Analyzing cardinality and participation for relation: {', '.join(entities_in_rel)}")
    
    # Build entity context - filter entities that are in this relation
    relevant_entities = [
        entity for entity in entities
        if extract_entity_name(entity) in entities_in_rel
    ]
    entity_desc_map: Dict[str, str] = {}
    for ent in relevant_entities:
        try:
            name = extract_entity_name(ent)
        except Exception:
            name = ""
        if not name:
            continue
        if isinstance(ent, dict):
            entity_desc_map[name] = str(ent.get("description", "") or "")
        else:
            entity_desc_map[name] = str(getattr(ent, "description", "") or "")
    entity_context_str = build_entity_list_string(
        relevant_entities,
        include_descriptions=True,
        prefix="- ",
    )
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to determine the cardinality and participation of each entity participating in a relationship.

**Cardinality** indicates how many instances of one entity can relate to instances of another entity:
- **"1" (One)**: One instance of this entity participates in the relationship
- **"N" (Many)**: Many instances of this entity can participate in the relationship

**Participation** indicates whether every instance of an entity must participate in the relationship:
- **"total"**: Every instance of this entity MUST participate in the relationship (e.g., every order must belong to a customer)
- **"partial"**: Some instances of this entity may NOT participate in the relationship (e.g., not all customers place orders)

Examples:
- **One-to-Many (1:N)**: Customer (1, partial) - Order (N, total)
  - One customer can have many orders
  - Each order belongs to one customer
  - Not all customers place orders (Customer: partial)
  - Every order must belong to a customer (Order: total)
  - Result: {{"entity_cardinalities": {{"Customer": "1", "Order": "N"}}, "entity_participations": {{"Customer": "partial", "Order": "total"}}}}

- **Many-to-Many (N:M)**: Student (N, partial) - Course (N, partial)
  - Many students can enroll in many courses
  - Each course can have many students
  - Not all students enroll in all courses (Student: partial)
  - Not all courses have students enrolled (Course: partial)
  - Result: {{"entity_cardinalities": {{"Student": "N", "Course": "N"}}, "entity_participations": {{"Student": "partial", "Course": "partial"}}}}

- **Many-to-One (N:1) - CRITICAL**: Customer (N, partial) - Address (1, partial)
  - **MULTIPLE customers can share the SAME address** (e.g., family members, roommates)
  - Each address can be associated with many customers
  - Each customer has one address (for delivery purposes)
  - This is **NOT one-to-one** - it is **many-to-one** (customers are "N", address is "1")
  - Result: {{"entity_cardinalities": {{"Customer": "N", "Address": "1"}}, "entity_participations": {{"Customer": "partial", "Address": "partial"}}}}
  - **Common mistake**: Do NOT mark this as 1:1 just because "each customer has one address" - the key question is "can multiple customers share the same address?" If yes → many-to-one

- **One-to-One (1:1)**: User (1, partial) - Profile (1, total)
  - One user has one profile
  - Each profile belongs to one user
  - **CRITICAL**: In a true 1:1, instances are unique and cannot be shared
  - Not all users have profiles (User: partial)
  - Every profile must belong to a user (Profile: total)
  - Result: {{"entity_cardinalities": {{"User": "1", "Profile": "1"}}, "entity_participations": {{"User": "partial", "Profile": "total"}}}}

For each entity in the relationship, determine:
1. Cardinality: whether it participates as "1" or "N"
2. Participation: whether participation is "total" (every instance must participate) or "partial" (some instances may not participate)

**CRITICAL CARDINALITY RULES**:
- **Ask the key question**: "Can multiple instances of Entity A share/relate to the same instance of Entity B?"
  - If YES → Entity A is "N" (many), Entity B is "1" (one) → **Many-to-One**
  - If NO and reverse is also NO → Both are "1" → **One-to-One**
  - If YES in both directions → Both are "N" → **Many-to-Many**
- **Common mistake to avoid**: Do NOT confuse "each A has one B" with "one-to-one"
  - Example: "Each customer has one address" does NOT mean 1:1
  - The correct question: "Can multiple customers share the same address?" → YES → Many-to-One (Customer: N, Address: 1)
- **Real-world semantics matter**: Think about whether instances can be shared
  - Addresses, Categories, PaymentMethods, etc. are often shared → Many-to-One
  - Profiles, Passports, Social Security Numbers are unique → One-to-One

Important:
- Consider the relationship type and description
- Think about real-world semantics and whether instances can be shared
- For participation: ask "Must every instance of this entity participate?" If yes → total, if no → partial
- For ternary or n-ary relations, determine cardinality and participation for each entity independently
- Provide clear reasoning for your decisions, especially explaining why it's many-to-one vs one-to-one

CRITICAL: You MUST return ONLY valid JSON. Do NOT include any markdown formatting, explanations, or text outside the JSON object.

You have access to validation tools:
1) validate_entity_cardinality: validate that cardinality values are "1" or "N"
2) validate_relation_cardinality_output: validate completeness/allowed values for the full output

Return a JSON object with exactly these fields:
- entity_cardinalities: Dictionary mapping each entity name to "1" or "N"
- entity_participations: Dictionary mapping each entity name to "total" or "partial"
- reasoning: String with clear explanation of your cardinality and participation decisions

Example JSON output (NO markdown, NO text before/after):
{{"entity_cardinalities": {{"Customer": "1", "Order": "N"}}, "entity_participations": {{"Customer": "partial", "Order": "total"}}, "reasoning": "The relationship is one-to-many..."}}"""
    
    # Human prompt template
    # Note: Use format() placeholders for template variables, not f-string
    # to avoid conflicts with entity names that might contain special characters
    entities_str = ", ".join(entities_in_rel)
    human_prompt = """Relation:
- Entities: {entities_str}
- Type: {rel_type}
- Description: {rel_description}

Entity details:
{{entity_context}}

Original description (if available):
{{nl_description}}""".format(
        entities_str=entities_str,
        rel_type=rel_type,
        rel_description=rel_description or "No description",
    )
    
    # Initialize model
    llm = get_model_for_step("1.11")  # Step 1.11 maps to "high_fanout" task type

    rel_text = "\n".join(
        [
            f"Type: {rel_type}",
            f"Description: {rel_description or ''}",
            f"Reasoning: {relation.get('reasoning', '') or ''}",
            f"Evidence: {relation.get('evidence', '') or ''}",
            f"NL: {nl_description or ''}",
        ]
    )

    async def _call_llm(prompt_human: str) -> RelationCardinalityOutput:
        config = get_trace_config(
            "1.11",
            phase=1,
            tags=["relation_cardinality"],
            additional_metadata={"relation_id": relation_id},
        )
        return await standardized_llm_call(
            llm=llm,
            output_schema=RelationCardinalityOutput,
            system_prompt=system_prompt
            + "\n\nIMPORTANT: You MUST provide entries for ALL entities listed in the relation. Do not omit any entity.",
            human_prompt_template=prompt_human,
            input_data={
                "entity_context": entity_context_str,
                "nl_description": nl_description or "",
            },
            tools=None,
            use_agent_executor=False,
            config=config,
        )
    
    try:
        result: RelationCardinalityOutput = await _call_llm(human_prompt)
        
        # Work with Pydantic model directly
        cardinalities = result.entity_cardinalities
        participations = result.entity_participations

        # Deterministic post-validation (replaces tool-calling)
        check = _validate_relation_cardinality_output_impl(
            entities=entities_in_rel,
            entity_cardinalities=cardinalities,
            entity_participations=participations,
        )
        if not check.get("valid", True):
            # Attempt one repair pass with explicit error feedback.
            repair_prompt = (
                human_prompt
                + "\n\nVALIDATION_ERROR:\n"
                + (check.get("error") or "Unknown validation error")
                + "\n\nReturn corrected JSON with complete dictionaries for ALL entities."
            )
            repaired: RelationCardinalityOutput = await _call_llm(repair_prompt)
            cardinalities = repaired.entity_cardinalities
            participations = repaired.entity_participations
            check2 = _validate_relation_cardinality_output_impl(
                entities=entities_in_rel,
                entity_cardinalities=cardinalities,
                entity_participations=participations,
            )
            if check2.get("valid", True):
                return {
                    "entity_cardinalities": cardinalities,
                    "entity_participations": participations,
                    "reasoning": repaired.reasoning,
                }

            # Final deterministic fallback (no exception; keep pipeline stable).
            logger.warning(
                "Step 1.11: LLM output incomplete after repair; falling back deterministically. relation=%s error=%s",
                ", ".join(entities_in_rel),
                check2.get("error"),
            )
            fallback_cards = _infer_cardinalities_from_text(
                entities=entities_in_rel,
                rel_type=rel_type,
                rel_text=rel_text,
                entity_descriptions=entity_desc_map,
            )
            fallback_parts = _infer_participations_from_cardinalities(
                entities=entities_in_rel,
                entity_cardinalities=fallback_cards,
                rel_text=rel_text,
            )
            return {
                "entity_cardinalities": fallback_cards,
                "entity_participations": fallback_parts,
                "reasoning": (
                    "Fallback (deterministic): LLM returned incomplete cardinality/participation output even after repair. "
                    f"Validation error: {check2.get('error')}"
                ),
            }

        logger.debug(
            f"Relation {', '.join(entities_in_rel)} cardinalities: {cardinalities}, participations: {participations}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        # Never fail Phase 1 due to unstable cardinality inference. Use deterministic fallback.
        logger.warning(
            "Step 1.11: exception during LLM cardinality inference; falling back deterministically. relation=%s error=%s",
            ", ".join(entities_in_rel),
            str(e),
            exc_info=True,
        )
        fallback_cards = _infer_cardinalities_from_text(
            entities=entities_in_rel,
            rel_type=rel_type,
            rel_text=rel_text,
            entity_descriptions=entity_desc_map,
        )
        fallback_parts = _infer_participations_from_cardinalities(
            entities=entities_in_rel,
            entity_cardinalities=fallback_cards,
            rel_text=rel_text,
        )
        return {
            "entity_cardinalities": fallback_cards,
            "entity_participations": fallback_parts,
            "reasoning": (
                "Fallback (deterministic): exception during LLM inference. "
                f"Error: {str(e)}"
            ),
        }


async def step_1_11_relation_cardinality(
    relations: List[Dict[str, Any]],
    entities: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.11: Determine cardinality and participation for all relations (parallel execution).
    
    Args:
        relations: List of relations from Step 1.9
        entities: List of all entities for context
        nl_description: Optional original NL description
        
    Returns:
        dict: Cardinality and participation information for all relations
        
    Example:
        >>> result = await step_1_11_relation_cardinality(
        ...     relations=[{"entities": ["Customer", "Order"]}],
        ...     entities=[{"name": "Customer"}, {"name": "Order"}]
        ... )
        >>> len(result["relation_cardinalities"])
        1
    """
    logger.info(f"Starting Step 1.11: Relation Cardinality & Participation for {len(relations)} relations")
    
    if not relations:
        logger.warning("No relations provided for cardinality analysis")
        return {"relation_cardinalities": []}
    
    # Execute in parallel for all relations
    import asyncio
    
    tasks = []
    for relation in relations:
        task = step_1_11_relation_cardinality_single(
            relation=relation,
            entities=entities,
            nl_description=nl_description,
        )
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    relation_cardinalities = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error processing relation {i}: {result}")
            # Create a default entry for failed relations
            entities_in_rel = relations[i].get("entities", [])
            relation_cardinalities.append({
                "entities": entities_in_rel,
                "entity_cardinalities": {},
                "entity_participations": {},
                "reasoning": f"Error during analysis: {str(result)}"
            })
        else:
            # Add entity list to result for reference
            entities_in_rel = relations[i].get("entities", [])
            result["entities"] = entities_in_rel
            relation_cardinalities.append(result)
    
    logger.info(f"Relation cardinality and participation analysis completed for {len(relation_cardinalities)} relations")
    
    return {"relation_cardinalities": relation_cardinalities}

