"""Unit tests for Step 1.75 (Entity vs Relation Reclassification).

These tests are deterministic and do NOT require LLM calls.
"""

import asyncio

from NL2DATA.phases.phase1.step_1_75_entity_relation_reclassification import (
    step_1_75_entity_relation_reclassification,
)
from NL2DATA.phases.phase1.utils.entity_reclassification import pick_associative_candidates


def test_pick_associative_candidates_flags_order_item_like_names():
    entities = [
        {"name": "Order", "description": "Customer order header"},
        {"name": "Product", "description": "Catalog product"},
        {"name": "OrderItem", "description": "Line items in an order (links Order and Product)"},
        {"name": "Shipment", "description": "Shipment record for an order"},
    ]

    candidates = pick_associative_candidates(entities, threshold=0.6)
    candidate_names = [c.name for c in candidates]

    assert "OrderItem" in candidate_names
    assert "Shipment" not in candidate_names


def test_step_1_75_can_remove_associative_entities_without_llm():
    entities = [
        {"name": "Order", "description": "Customer order header"},
        {"name": "Product", "description": "Catalog product"},
        {"name": "Book", "description": "A book in a library"},
        {"name": "Author", "description": "A book author"},
        {"name": "BookAuthor", "description": "Associative link between book and author with ordering"},
    ]

    out = asyncio.run(
        step_1_75_entity_relation_reclassification(
            entities=entities,
            nl_description="Books have authors. There is a book-author association with author order.",
            domain="library",
            heuristic_threshold=0.55,  # slightly permissive
            use_llm_verification=False,  # deterministic path
        )
    )

    removed = out.get("removed_entity_names", [])
    remaining = [e.get("name") for e in out.get("entities", [])]

    assert "BookAuthor" in removed
    assert "BookAuthor" not in remaining
    assert "Book" in remaining and "Author" in remaining
    # Should also provide at least one relation candidate hint
    assert len(out.get("relation_candidates", []) or []) >= 1


