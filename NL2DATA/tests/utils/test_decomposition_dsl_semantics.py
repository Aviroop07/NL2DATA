from NL2DATA.phases.phase2.step_2_4_composite_attribute_handling import _validate_decomposition_dsls


def test_validate_decomposition_dsls_requires_all_subattrs_and_only_composite_identifier():
    issues = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street", "city"],
        decomposition_dsls={"street": "SPLIT_PART(address, ',', 1)"},
    )
    assert issues  # missing 'city' DSL

    issues2 = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street"],
        decomposition_dsls={"street": "SPLIT_PART(other, ',', 1)"},
    )
    assert issues2  # must reference address, not other


def test_validate_decomposition_dsls_accepts_valid_expr():
    issues = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street"],
        decomposition_dsls={"street": "SPLIT_PART(address, ',', 1)"},
    )
    assert issues == []

