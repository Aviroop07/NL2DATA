"""Phase 1, Step 1.11: Relation Cardinality & Participation.

Determines relationship cardinality (1 or N) and participation (total or partial) for each entity in the relation.
"""

from typing import List, Optional, Literal, Tuple, Dict, Any
import re
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.llm.json_schema_fix import OpenAICompatibleJsonSchema
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
)
# NOTE:
# Avoid LangChain @tool functions in internal Python flow. They are StructuredTools and not callable
# like normal Python functions. We do deterministic post-validation instead.

logger = get_logger(__name__)


class EntityCardinalityEntry(BaseModel):
    entity_name: str = Field(description="Name of the entity")
    cardinality: Literal["1", "N"] = Field(description="Cardinality in the relation: '1' for one, 'N' for many")

    model_config = ConfigDict(extra="forbid")


class EntityParticipationEntry(BaseModel):
    entity_name: str = Field(description="Name of the entity")
    participation: Literal["total", "partial"] = Field(description="Participation type: 'total' means every instance must participate, 'partial' means some instances may not participate")

    model_config = ConfigDict(extra="forbid")


class RelationCardinalityOutput(BaseModel):
    """Output structure for relation cardinality and participation."""
    # NOTE: These are intentionally REQUIRED (no defaults).
    # If the LLM omits them, we want schema validation to fail loudly rather than quietly
    # accepting {} and only failing later during deterministic completeness checks.
    entity_cardinalities: List[EntityCardinalityEntry] = Field(
        ...,
        description="List of entity cardinality entries, one per entity in the relation. Each entry maps an entity name to its cardinality ('1' for one, 'N' for many). Entity names MUST match exactly."
    )
    entity_participations: List[EntityParticipationEntry] = Field(
        ...,
        description="List of entity participation entries, one per entity in the relation. Each entry maps an entity name to its participation type ('total' means every instance must participate, 'partial' means some instances may not participate). Entity names MUST match exactly."
    )
    reasoning: str = Field(description="Reasoning for the cardinality and participation decisions")

    model_config = ConfigDict(extra="forbid")


class RelationCardinalityOutputLoose(BaseModel):
    """
    Loose output structure used ONLY for parsing LLM responses.

    Rationale:
    - Small/fast models occasionally omit required fields even at temperature=0.
    - If we parse with the strict schema, parsing fails inside standardized_llm_call and we lose
      the ability to run our own deterministic validator + targeted repair prompts.

    We therefore parse loosely, then enforce completeness/allowed values deterministically.
    """
    entity_cardinalities: Optional[List[Dict[str, Any]]] = None
    entity_participations: Optional[List[Dict[str, Any]]] = None
    reasoning: Optional[str] = None

    model_config = ConfigDict(extra="forbid", json_schema_extra={"schema_generator": OpenAICompatibleJsonSchema})


def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().split())


def _dict_to_cardinality_list(card_dict) -> List[EntityCardinalityEntry]:
    """Convert dict to list of EntityCardinalityEntry objects."""
    if isinstance(card_dict, dict):
        return [EntityCardinalityEntry(entity_name=k, cardinality=v) for k, v in card_dict.items()]
    elif isinstance(card_dict, list):
        # Already a list, validate and convert
        result = []
        for item in card_dict:
            if isinstance(item, dict):
                result.append(EntityCardinalityEntry(**item))
            elif isinstance(item, EntityCardinalityEntry):
                result.append(item)
        return result
    return []


def _dict_to_participation_list(part_dict) -> List[EntityParticipationEntry]:
    """Convert dict to list of EntityParticipationEntry objects."""
    if isinstance(part_dict, dict):
        return [EntityParticipationEntry(entity_name=k, participation=v) for k, v in part_dict.items()]
    elif isinstance(part_dict, list):
        # Already a list, validate and convert
        result = []
        for item in part_dict:
            if isinstance(item, dict):
                result.append(EntityParticipationEntry(**item))
            elif isinstance(item, EntityParticipationEntry):
                result.append(item)
        return result
    return []


def _cardinality_list_to_dict(card_list: List[EntityCardinalityEntry]) -> dict:
    """Convert list of EntityCardinalityEntry to dict for internal processing."""
    return {entry.entity_name: entry.cardinality for entry in card_list}


def _participation_list_to_dict(part_list: List[EntityParticipationEntry]) -> dict:
    """Convert list of EntityParticipationEntry to dict for internal processing."""
    return {entry.entity_name: entry.participation for entry in part_list}


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


def _build_variant_to_canonical_map(entities: List[str]) -> Dict[str, str]:
    """
    Build a lookup of normalized variant -> canonical entity name.

    Purpose:
    - Step 1.11 validation requires keys that exactly match entity names.
    - LLMs frequently return keys in lowercase/snake_case/plural forms.
    - We deterministically remap such keys back to canonical entity names.
    """
    out: Dict[str, str] = {}
    for e in (entities or []):
        canonical = str(e).strip()
        if not canonical:
            continue
        for v in _entity_name_variants(canonical):
            vv = str(v).strip().lower()
            if not vv:
                continue
            # Prefer first mapping in case of collisions (rare but possible)
            out.setdefault(vv, canonical)
        # Also map exact canonical lowercased
        out.setdefault(canonical.strip().lower(), canonical)
    return out


def _canonicalize_llm_constraint_dicts(
    *,
    entities: List[str],
    entity_cardinalities: Dict[str, Any],
    entity_participations: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Canonicalize LLM output dict keys to match exact entity names.

    This addresses the common failure mode where the LLM returns keys like:
    - "session" vs "Session"
    - "sensor_reading" vs "SensorReading"
    - pluralized keys like "orders" vs "Order"

    Strategy:
    - For each incoming key, normalize (strip + lower) and map via variants -> canonical.
    - Keep only keys that map to participating entities (prevents accidental leakage).
    - If multiple keys map to same canonical, keep the first.
    """
    variant_to_canonical = _build_variant_to_canonical_map(entities)

    canon_cards: Dict[str, Any] = {}
    for k, v in (entity_cardinalities or {}).items():
        kk = str(k).strip().lower()
        canonical = variant_to_canonical.get(kk)
        if not canonical:
            continue
        canon_cards.setdefault(canonical, v)

    canon_parts: Dict[str, Any] = {}
    for k, v in (entity_participations or {}).items():
        kk = str(k).strip().lower()
        canonical = variant_to_canonical.get(kk)
        if not canonical:
            continue
        canon_parts.setdefault(canonical, v)

    return canon_cards, canon_parts


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
    if rel_type_norm == "ternary":
        # For n-ary relations, default to N to avoid under-constraining
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

        # Type-based default direction if rel_type suggests it's a 1:N family.
        # We do not assume entity ordering is directional, so when we must choose,
        # default to [many, one] as a conservative FK-like assumption.
        if rel_type_norm in {"one-to-many", "many-to-one"}:
            return {a: "N", b: "1"}

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
    relation,
    entities: List,
    nl_description: Optional[str] = None,
) -> RelationCardinalityOutput:
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
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=RelationCardinalityOutput,
        additional_requirements=[
            "STRICT OUTPUT KEYING: Keys MUST use EXACT entity names from the relation (case-sensitive, CamelCase preserved)",
            "Do NOT use lowercase keys, snake_case keys, pluralized keys, or synonyms",
            "Include ALL entities from the relation in BOTH dictionaries (no omissions, no extras)",
            "Cardinality values MUST be exactly \"1\" or \"N\" (no other values)",
            "Participation values MUST be exactly \"total\" or \"partial\" (no other values)",
            "Reasoning is REQUIRED and cannot be omitted or empty"
        ]
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
  - Result: {{"entity_cardinalities": [{{"entity_name": "Customer", "cardinality": "1"}}, {{"entity_name": "Order", "cardinality": "N"}}], "entity_participations": [{{"entity_name": "Customer", "participation": "partial"}}, {{"entity_name": "Order", "participation": "total"}}], "reasoning": "One-to-many relationship..."}}

- **Many-to-Many (N:M)**: Student (N, partial) - Course (N, partial)
  - Many students can enroll in many courses
  - Each course can have many students
  - Not all students enroll in all courses (Student: partial)
  - Not all courses have students enrolled (Course: partial)
  - Result: {{"entity_cardinalities": [{{"entity_name": "Student", "cardinality": "N"}}, {{"entity_name": "Course", "cardinality": "N"}}], "entity_participations": [{{"entity_name": "Student", "participation": "partial"}}, {{"entity_name": "Course", "participation": "partial"}}], "reasoning": "Many-to-many relationship..."}}

- **Many-to-One (N:1) - CRITICAL**: Customer (N, partial) - Address (1, partial)
  - **MULTIPLE customers can share the SAME address** (e.g., family members, roommates)
  - Each address can be associated with many customers
  - Each customer has one address (for delivery purposes)
  - This is **NOT one-to-one** - it is **many-to-one** (customers are "N", address is "1")
  - Result: {{"entity_cardinalities": [{{"entity_name": "Customer", "cardinality": "N"}}, {{"entity_name": "Address", "cardinality": "1"}}], "entity_participations": [{{"entity_name": "Customer", "participation": "partial"}}, {{"entity_name": "Address", "participation": "partial"}}], "reasoning": "Many-to-one relationship..."}}
  - **Common mistake**: Do NOT mark this as 1:1 just because "each customer has one address" - the key question is "can multiple customers share the same address?" If yes → many-to-one

- **One-to-One (1:1)**: User (1, partial) - Profile (1, total)
  - One user has one profile
  - Each profile belongs to one user
  - **CRITICAL**: In a true 1:1, instances are unique and cannot be shared
  - Not all users have profiles (User: partial)
  - Every profile must belong to a user (Profile: total)
  - Result: {{"entity_cardinalities": [{{"entity_name": "User", "cardinality": "1"}}, {{"entity_name": "Profile", "cardinality": "1"}}], "entity_participations": [{{"entity_name": "User", "participation": "partial"}}, {{"entity_name": "Profile", "participation": "total"}}], "reasoning": "One-to-one relationship..."}}

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

STRICT OUTPUT KEYING (CRITICAL):
- You MUST use the EXACT entity names provided in "Relation: Entities" as the entity_name values in BOTH lists.
- Entity names are case-sensitive and must match exactly (CamelCase included).
- Do NOT use lowercase entity names, snake_case entity names, pluralized entity names, or synonyms.
- Include ALL entities from the relation in BOTH lists (no omissions, no extras).
- Each list must contain one entry per entity with the exact entity name.
- If you omit any required field (entity_cardinalities / entity_participations / reasoning), the response will be rejected.
- Even if you are uncertain, you MUST still output complete lists with your best-guess values.

CRITICAL: You MUST return ONLY valid JSON. Do NOT include any markdown formatting, explanations, or text outside the JSON object.

You have access to validation tools:
1) validate_entity_cardinality: validate that cardinality values are "1" or "N"
2) validate_relation_cardinality_output: validate completeness/allowed values for the full output

""" + output_structure_section + """

Example JSON output (NO markdown, NO text before/after):
{{"entity_cardinalities": [{{"entity_name": "Customer", "cardinality": "1"}}, {{"entity_name": "Order", "cardinality": "N"}}], "entity_participations": [{{"entity_name": "Customer", "participation": "partial"}}, {{"entity_name": "Order", "participation": "total"}}], "reasoning": "The relationship is one-to-many..."}}"""
    
    # Human prompt template
    # Note: Use format() placeholders for template variables, not f-string
    # to avoid conflicts with entity names that might contain special characters
    entities_str = ", ".join(entities_in_rel)
    # Build list template for entity_cardinalities and entity_participations
    cardinality_entries = ",\n    ".join([f'{{"entity_name": "{e}", "cardinality": "1"}}' for e in entities_in_rel])
    participation_entries = ",\n    ".join([f'{{"entity_name": "{e}", "participation": "partial"}}' for e in entities_in_rel])
    
    human_prompt = """Relation:
- Entities: {entities_str}
- Type: {rel_type}
- Description: {rel_description}

Entity details:
{{entity_context}}

Original description (if available):
{{nl_description}}

REQUIRED OUTPUT TEMPLATE (fill this in; use lists of objects with entity_name fields):
{{
  "entity_cardinalities": [
    {cardinality_entries}
  ],
  "entity_participations": [
    {participation_entries}
  ],
  "reasoning": "explain your decisions"
}}

Note: Replace the placeholder values ("1" for cardinality, "partial" for participation) with the correct values based on the relationship analysis.""".format(
        entities_str=entities_str,
        rel_type=rel_type,
        rel_description=rel_description or "No description",
        cardinality_entries=cardinality_entries,
        participation_entries=participation_entries,
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

    async def _call_llm(prompt_human: str) -> RelationCardinalityOutputLoose:
        config = get_trace_config(
            "1.11",
            phase=1,
            tags=["relation_cardinality"],
            additional_metadata={"relation_id": relation_id},
        )
        return await standardized_llm_call(
            llm=llm,
            output_schema=RelationCardinalityOutputLoose,
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
        result: RelationCardinalityOutputLoose = await _call_llm(human_prompt)
        
        # Extract raw (loose) payload - handle both list and dict formats
        cardinalities_raw = result.entity_cardinalities or []
        participations_raw = result.entity_participations or []
        reasoning = result.reasoning or ""

        # Convert lists to dictionaries if needed
        # The LLM returns lists of EntityCardinalityEntry/EntityParticipationEntry objects
        if isinstance(cardinalities_raw, list):
            cardinalities = {}
            for item in cardinalities_raw:
                if isinstance(item, dict):
                    # Handle dict format: {"entity_name": "...", "cardinality": "..."}
                    entity_name = item.get("entity_name", "")
                    cardinality = item.get("cardinality", "")
                    if entity_name and cardinality:
                        cardinalities[entity_name] = cardinality
                elif hasattr(item, "entity_name") and hasattr(item, "cardinality"):
                    # Handle Pydantic model format
                    cardinalities[item.entity_name] = item.cardinality
        else:
            # Already a dict
            cardinalities = cardinalities_raw or {}

        if isinstance(participations_raw, list):
            participations = {}
            for item in participations_raw:
                if isinstance(item, dict):
                    # Handle dict format: {"entity_name": "...", "participation": "..."}
                    entity_name = item.get("entity_name", "")
                    participation = item.get("participation", "")
                    if entity_name and participation:
                        participations[entity_name] = participation
                elif hasattr(item, "entity_name") and hasattr(item, "participation"):
                    # Handle Pydantic model format
                    participations[item.entity_name] = item.participation
        else:
            # Already a dict
            participations = participations_raw or {}

        # Canonicalize keys (LLMs often return lowercase/snake_case variants).
        cardinalities, participations = _canonicalize_llm_constraint_dicts(
            entities=entities_in_rel,
            entity_cardinalities=cardinalities,
            entity_participations=participations,
        )

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
            repaired: RelationCardinalityOutputLoose = await _call_llm(repair_prompt)
            
            # Convert lists to dictionaries if needed (same logic as above)
            cardinalities_raw = repaired.entity_cardinalities or []
            participations_raw = repaired.entity_participations or []
            
            if isinstance(cardinalities_raw, list):
                cardinalities = {}
                for item in cardinalities_raw:
                    if isinstance(item, dict):
                        entity_name = item.get("entity_name", "")
                        cardinality = item.get("cardinality", "")
                        if entity_name and cardinality:
                            cardinalities[entity_name] = cardinality
                    elif hasattr(item, "entity_name") and hasattr(item, "cardinality"):
                        cardinalities[item.entity_name] = item.cardinality
            else:
                cardinalities = cardinalities_raw or {}

            if isinstance(participations_raw, list):
                participations = {}
                for item in participations_raw:
                    if isinstance(item, dict):
                        entity_name = item.get("entity_name", "")
                        participation = item.get("participation", "")
                        if entity_name and participation:
                            participations[entity_name] = participation
                    elif hasattr(item, "entity_name") and hasattr(item, "participation"):
                        participations[item.entity_name] = item.participation
            else:
                participations = participations_raw or {}
            
            reasoning = repaired.reasoning or reasoning
            cardinalities, participations = _canonicalize_llm_constraint_dicts(
                entities=entities_in_rel,
                entity_cardinalities=cardinalities,
                entity_participations=participations,
            )
            check2 = _validate_relation_cardinality_output_impl(
                entities=entities_in_rel,
                entity_cardinalities=cardinalities,
                entity_participations=participations,
            )
            if check2.get("valid", True):
                card_list = _dict_to_cardinality_list(cardinalities)
                part_list = _dict_to_participation_list(participations)
                return RelationCardinalityOutput(
                    entity_cardinalities=card_list,
                    entity_participations=part_list,
                    reasoning=reasoning,
                )

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
            card_list = _dict_to_cardinality_list(fallback_cards)
            part_list = _dict_to_participation_list(fallback_parts)
            return RelationCardinalityOutput(
                entity_cardinalities=card_list,
                entity_participations=part_list,
                reasoning=(
                    "Fallback (deterministic): LLM returned incomplete cardinality/participation output even after repair. "
                    f"Validation error: {check2.get('error')}"
                ),
            )

        logger.debug(
            f"Relation {', '.join(entities_in_rel)} cardinalities: {cardinalities}, participations: {participations}"
        )
        
        # Convert dicts to lists at return boundary
        card_list = _dict_to_cardinality_list(cardinalities)
        part_list = _dict_to_participation_list(participations)
        
        return RelationCardinalityOutput(
            entity_cardinalities=card_list,
            entity_participations=part_list,
            reasoning=reasoning,
        )
        
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
        card_list = _dict_to_cardinality_list(fallback_cards)
        part_list = _dict_to_participation_list(fallback_parts)
        return RelationCardinalityOutput(
            entity_cardinalities=card_list,
            entity_participations=part_list,
            reasoning=(
                "Fallback (deterministic): exception during LLM inference. "
                f"Error: {str(e)}"
            ),
        )


async def step_1_11_relation_cardinality(
    relations: List,
    entities: List,
    nl_description: Optional[str] = None,
):
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

