"""Fast regression tests for recent pipeline fixes.

These tests are deterministic and do NOT call real LLMs.
They validate that recent bugfixes change behavior in the intended way:
- Step 2.7: primary key fallback when LLM suggests invalid attributes
- Step 4.4: categorical detection tolerates reasoning=None without crashing
- Step 3.5: junction table FK name collision avoidance (duplicate PK names)
- base_router: max_tokens can be configured per task type
"""

import asyncio
import importlib
import sys
from pathlib import Path

import pytest


# Ensure project root on path (match existing tests style)
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


def test_step_2_7_injects_surrogate_pk_when_llm_returns_invalid_pk(monkeypatch) -> None:
    s27 = importlib.import_module("NL2DATA.phases.phase2.step_2_7_primary_key_identification")

    async def fake_call(**kwargs):
        # LLM returns a PK attribute that is NOT in the provided attributes list
        return s27.PrimaryKeyOutput(
            primary_key=["id"],  # invalid
            reasoning="Pick id",
            alternative_keys=[],
        )

    monkeypatch.setattr(s27, "standardized_llm_call", fake_call)
    monkeypatch.setattr(s27, "get_model_for_step", lambda *args, **kwargs: None)

    out = asyncio.run(
        s27.step_2_7_primary_key_identification(
            entity_name="SensorReading",
            attributes=["timestamp", "sensor_id", "value"],
            nl_description="Readings are uniquely identified",
            domain="iot",
        )
    )

    assert out["primary_key"] == ["sensorreading_id"]
    assert "Deterministic fallback" in out["reasoning"]


def test_step_4_4_handles_reasoning_none(monkeypatch) -> None:
    s44 = importlib.import_module("NL2DATA.phases.phase4.step_4_4_categorical_detection")

    async def fake_call(**kwargs):
        return s44.CategoricalDetectionOutput(
            categorical_attributes=["status"],
            reasoning=None,  # simulate model returning null
        )

    monkeypatch.setattr(s44, "standardized_llm_call", fake_call)
    monkeypatch.setattr(s44, "get_model_for_step", lambda *args, **kwargs: None)

    out = asyncio.run(
        s44.step_4_4_categorical_detection(
            entity_name="Order",
            attributes=[{"name": "status", "description": "Order status", "type_hint": "string"}],
            attribute_types={"status": {"type": "VARCHAR", "size": 50}},
            nl_description="Orders have a status (pending/shipped/delivered).",
            domain="e-commerce",
        )
    )

    assert out["categorical_attributes"] == ["status"]
    assert isinstance(out["reasoning"], dict)
    assert "status" in out["reasoning"]


def test_step_3_5_junction_table_fk_collision_is_avoided() -> None:
    # Deterministic: uses the compiler directly (no LLM)
    from NL2DATA.ir.models.er_relational import ERDesign, EREntity, ERRelation, ERAttribute
    from NL2DATA.phases.phase3.step_3_5_relational_schema_compilation import (
        step_3_5_relational_schema_compilation,
    )

    er = ERDesign(
        entities=[
            EREntity(
                name="SENSOR",
                primary_key=["sensor_id"],
                attributes=[ERAttribute(name="sensor_id", type_hint="integer")],
            ),
            EREntity(
                name="INCIDENT",
                primary_key=["sensor_id"],  # intentionally colliding PK name
                attributes=[ERAttribute(name="sensor_id", type_hint="integer")],
            ),
        ],
        relations=[
            ERRelation(
                entities=["SENSOR", "INCIDENT"],
                type="many-to-many",
                description="Sensors can be involved in incidents; incidents can involve sensors",
                arity=2,
                entity_cardinalities={"SENSOR": "N", "INCIDENT": "N"},
                entity_participations={"SENSOR": "total", "INCIDENT": "total"},
                attributes=[],
            )
        ],
    )

    schema = step_3_5_relational_schema_compilation(
        er_design=er.model_dump(),
        foreign_keys=[],
        primary_keys={"SENSOR": ["sensor_id"], "INCIDENT": ["sensor_id"]},
        constraints=[],
    )

    # Junction name is sorted join of entity names
    junction_name = "_".join(sorted(["SENSOR", "INCIDENT"]))
    junction = next(t for t in schema["tables"] if t.get("name") == junction_name)
    col_names = [c.get("name") for c in junction.get("columns", [])]

    # Key assertion: no duplicate column names
    assert len(col_names) == len(set(col_names))
    # And it contains an unprefixed and a prefixed version (collision resolution)
    assert "sensor_id" in col_names
    assert "INCIDENT_sensor_id" in col_names


def test_get_model_for_task_uses_max_tokens_per_task(monkeypatch) -> None:
    base_router = importlib.import_module("NL2DATA.utils.llm.base_router")

    class DummyChatOpenAI:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    # Patch ChatOpenAI + api key + config
    monkeypatch.setattr(base_router, "ChatOpenAI", DummyChatOpenAI)
    monkeypatch.setattr(base_router, "get_api_key", lambda: "test-key")
    monkeypatch.setattr(
        base_router,
        "get_config",
        lambda section: {
            "model": "gpt-4o-mini",
            "temperature": 0,
            "max_tokens": 16000,
            "timeout": 180,
            "model_selection": {"simple": "gpt-4o-mini"},
            "max_tokens_per_task": {"simple": 4000},
        },
    )

    model = base_router.get_model_for_task("simple")
    assert model.kwargs["max_tokens"] == 4000

    # Explicit override should win
    model2 = base_router.get_model_for_task("simple", max_tokens=1234)
    assert model2.kwargs["max_tokens"] == 1234



