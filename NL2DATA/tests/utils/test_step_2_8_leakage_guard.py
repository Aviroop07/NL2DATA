from NL2DATA.phases.phase7.step_7_1_multivalued_derived_detection import (
    MultivaluedDerivedOutput,
    _clean_2_8_result,
    _validate_2_8_output,
)


def test_step_2_8_flags_cross_entity_derivation_text_using_backticked_cols():
    nl = "Trip base columns: `duration_minutes`, `distance_km`. PromotionalDiscount columns: `discount_rate`."
    attrs = ["promotional_discount_id", "discount_rate", "start_time", "end_time"]
    raw = MultivaluedDerivedOutput(
        multivalued=[],
        derived=["end_time"],
        derivation_rules={"end_time": "start_time + duration_minutes"},
        multivalued_handling={},
        reasoning={"end_time": "Computed from duration_minutes."},
    )
    cleaned = _clean_2_8_result(entity_name="PromotionalDiscount", attributes=attrs, result=raw)
    issues = _validate_2_8_output(
        entity_name="PromotionalDiscount",
        attributes=attrs,
        nl_description=nl,
        out=cleaned,
    )
    assert issues
    assert "duration_minutes" in issues[0]


def test_step_2_8_allows_entity_local_derivation_text():
    nl = "Promo columns: `discount_rate`, `start_time`, `end_time`."
    attrs = ["discount_rate", "start_time", "end_time"]
    raw = MultivaluedDerivedOutput(
        multivalued=[],
        derived=["end_time"],
        derivation_rules={"end_time": "end_time"},  # trivial but entity-local
        multivalued_handling={},
        reasoning={"end_time": "Uses only end_time."},
    )
    cleaned = _clean_2_8_result(entity_name="PromotionalDiscount", attributes=attrs, result=raw)
    issues = _validate_2_8_output(
        entity_name="PromotionalDiscount",
        attributes=attrs,
        nl_description=nl,
        out=cleaned,
    )
    assert issues == []

