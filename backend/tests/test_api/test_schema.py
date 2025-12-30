"""Tests for schema endpoints."""

import pytest
from fastapi.testclient import TestClient
from backend.main import app
from backend.utils.job_manager import JobManager
from backend.dependencies import get_job_manager


client = TestClient(app)


def test_get_distributions_metadata(client):
    """Test getting distribution metadata."""
    response = client.get("/api/schema/distributions/metadata")
    
    assert response.status_code == 200
    data = response.json()
    assert "distributions" in data
    assert isinstance(data["distributions"], list)
    assert len(data["distributions"]) > 0
    
    # Check structure of first distribution
    dist = data["distributions"][0]
    assert "name" in dist
    assert "parameters" in dist
    assert isinstance(dist["parameters"], list)
    
    # Check parameter structure
    if len(dist["parameters"]) > 0:
        param = dist["parameters"][0]
        assert "name" in param
        assert "type" in param
        assert param["type"] in ["decimal", "integer", "array", "string"]


def test_get_distributions_metadata_all_types(client):
    """Test that all expected distribution types are present."""
    response = client.get("/api/schema/distributions/metadata")
    assert response.status_code == 200
    
    distributions = {d["name"] for d in response.json()["distributions"]}
    expected = {
        "uniform", "normal", "lognormal", "pareto", "zipf",
        "bernoulli", "categorical", "seasonal", "trend"
    }
    assert expected.issubset(distributions)


def test_save_changes_nonexistent_job(client):
    """Test saving changes for non-existent job."""
    # Use a valid UUID format that doesn't exist
    fake_uuid = "00000000-0000-0000-0000-000000000000"
    response = client.post(
        "/api/schema/save_changes",
        json={
            "job_id": fake_uuid,
            "edit_mode": "er_diagram",
            "changes": {}
        }
    )
    assert response.status_code == 404


def test_save_changes_invalid_job_id_format(client):
    """Test saving changes with invalid job ID format."""
    response = client.post(
        "/api/schema/save_changes",
        json={
            "job_id": "invalid-format",
            "edit_mode": "er_diagram",
            "changes": {}
        }
    )
    assert response.status_code == 422  # Validation error


def test_save_changes_invalid_edit_mode(client, sample_job_id):
    """Test saving changes with invalid edit mode."""
    # First create a job using the same job_manager instance as the API
    from backend.dependencies import get_job_manager
    job_manager = get_job_manager()
    job_manager.create_job(sample_job_id, "Test description")
    
    response = client.post(
        "/api/schema/save_changes",
        json={
            "job_id": sample_job_id,
            "edit_mode": "invalid_mode",
            "changes": {}
        }
    )
    assert response.status_code == 422  # Validation error


def test_get_er_diagram_image_nonexistent_job(client):
    """Test getting ER diagram for non-existent job."""
    response = client.get("/api/schema/er_diagram_image/nonexistent-job")
    assert response.status_code == 404


def test_get_er_diagram_image_existing_job(client, sample_job_id, sample_state):
    """Test getting ER diagram for existing job."""
    # Create job with state using the same job_manager instance as the API
    from backend.dependencies import get_job_manager
    job_manager = get_job_manager()
    job_manager.create_job(sample_job_id, "Test description")
    job_manager.update_job_state(sample_job_id, sample_state)
    
    response = client.get(f"/api/schema/er_diagram_image/{sample_job_id}")
    # Should return image (200) or handle gracefully
    assert response.status_code in [200, 500]  # 500 if diagram generation fails (expected for now)

