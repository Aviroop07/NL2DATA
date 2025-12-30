"""Unit tests ensuring Phase 2 steps tolerate Optional output fields being None.

We marked many fields Optional to satisfy OpenAI response_format schema constraints.
When the model returns null, steps must normalize None -> empty list/dict to avoid
TypeError: 'NoneType' object is not iterable.
"""

import asyncio
import importlib

s28 = importlib.import_module("NL2DATA.phases.phase2.step_2_8_multivalued_derived_detection")
s210 = importlib.import_module("NL2DATA.phases.phase2.step_2_10_unique_constraints")


def test_step_2_8_handles_none_optional_fields(monkeypatch):
    async def fake_call(**kwargs):
        # Return all optional fields as None (simulate model returning nulls)
        return s28.MultivaluedDerivedOutput(
            multivalued=None,
            derived=None,
            derivation_rules=None,
            multivalued_handling=None,
            reasoning=None,
        )

    monkeypatch.setattr(s28, "standardized_llm_call", fake_call)
    monkeypatch.setattr(s28, "get_model_for_step", lambda *args, **kwargs: None)

    out = asyncio.run(
        s28.step_2_8_multivalued_derived_detection(
            entity_name="Customer",
            attributes=["customer_id", "email", "phone_numbers"],
            nl_description="Customers can have multiple phone numbers.",
            domain="e-commerce",
        )
    )

    assert out["multivalued"] == []
    assert out["derived"] == []
    assert out["derivation_rules"] == {}
    assert out["multivalued_handling"] == {}
    assert out["reasoning"] == {}


def test_step_2_10_handles_none_optional_fields(monkeypatch):
    async def fake_call(**kwargs):
        return s210.UniqueConstraintsOutput(
            unique_attributes=None,
            unique_combinations=None,
            reasoning=None,
        )

    monkeypatch.setattr(s210, "standardized_llm_call", fake_call)
    monkeypatch.setattr(s210, "get_model_for_step", lambda *args, **kwargs: None)

    out = asyncio.run(
        s210.step_2_10_unique_constraints(
            entity_name="Customer",
            attributes=["customer_id", "email", "phone"],
            primary_key=["customer_id"],
            nl_description="Customers have unique emails.",
            domain="e-commerce",
        )
    )

    assert out["unique_attributes"] == []
    assert out["unique_combinations"] == []
    assert out["reasoning"] == {}


