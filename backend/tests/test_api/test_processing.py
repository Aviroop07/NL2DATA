"""Tests for processing endpoints."""

import pytest
from fastapi.testclient import TestClient
from backend.main import app
from backend.utils.job_manager import JobManager
from backend.dependencies import get_job_manager


client = TestClient(app)


def test_health_endpoint():
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_start_processing(client, sample_nl_description):
    """Test starting a processing job."""
    # NOTE: This test will fail if NL2DATA pipeline requires checkpointer config.
    # The actual pipeline execution happens in background and may fail,
    # but the endpoint should still return 200 with job_id.
    response = client.post(
        "/api/process/start",
        json={"nl_description": sample_nl_description}
    )
    
    # Endpoint should return 200 even if background task fails
    assert response.status_code == 200
    data = response.json()
    assert "job_id" in data
    assert data["status"] == "started"
    assert "created_at" in data
    assert len(data["job_id"]) > 0


def test_start_processing_empty_description(client):
    """Test starting processing with empty description."""
    response = client.post(
        "/api/process/start",
        json={"nl_description": ""}
    )
    assert response.status_code == 422  # Validation error


def test_get_status_nonexistent_job(client):
    """Test getting status for non-existent job."""
    response = client.get("/api/process/status/nonexistent-job-id")
    assert response.status_code == 404


def test_get_status_existing_job(client, sample_nl_description):
    """Test getting status for existing job."""
    # Create a job
    start_response = client.post(
        "/api/process/start",
        json={"nl_description": sample_nl_description}
    )
    job_id = start_response.json()["job_id"]
    
    # Get status
    status_response = client.get(f"/api/process/status/{job_id}")
    assert status_response.status_code == 200
    data = status_response.json()
    assert data["job_id"] == job_id
    assert "status" in data
    assert data["status"] in ["pending", "in_progress", "completed", "failed"]


def test_start_processing_multiple_jobs(client, sample_nl_description):
    """Test creating multiple jobs."""
    job_ids = []
    for i in range(3):
        response = client.post(
            "/api/process/start",
            json={"nl_description": f"{sample_nl_description} (test {i})"}
        )
        assert response.status_code == 200
        job_ids.append(response.json()["job_id"])
    
    # All job IDs should be unique
    assert len(set(job_ids)) == 3

