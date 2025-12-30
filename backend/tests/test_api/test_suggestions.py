"""Tests for suggestions endpoint."""

import pytest
from fastapi.testclient import TestClient
from backend.main import app


client = TestClient(app)


def test_get_suggestions(client, sample_nl_description):
    """Test getting keyword suggestions."""
    response = client.post(
        "/api/suggestions",
        json={"nl_description": sample_nl_description}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "keywords" in data
    assert "extracted_items" in data
    assert isinstance(data["keywords"], list)
    assert isinstance(data["extracted_items"], dict)


def test_get_suggestions_empty_description(client):
    """Test suggestions with empty description."""
    response = client.post(
        "/api/suggestions",
        json={"nl_description": ""}
    )
    assert response.status_code == 422  # Validation error


def test_get_suggestions_structure(client, sample_nl_description):
    """Test suggestions response structure."""
    response = client.post(
        "/api/suggestions",
        json={"nl_description": sample_nl_description}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Check keywords structure
    if len(data["keywords"]) > 0:
        keyword = data["keywords"][0]
        assert "text" in keyword
        assert "type" in keyword
        assert "enhanced_nl_description" in keyword
        assert keyword["type"] in ["domain", "entity", "constraint", "attribute", "relationship", "distribution"]
    
    # Check extracted_items structure
    extracted = data["extracted_items"]
    assert "domain" in extracted
    assert "entities" in extracted
    assert "cardinalities" in extracted
    assert "column_names" in extracted
    assert "constraints" in extracted
    assert "relationships" in extracted



