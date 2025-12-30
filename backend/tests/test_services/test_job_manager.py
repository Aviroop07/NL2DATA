"""Tests for JobManager."""

import pytest
from backend.utils.job_manager import JobManager


def test_create_job():
    """Test creating a new job."""
    manager = JobManager()
    job_id = "test-job-123"
    
    manager.create_job(job_id, "Test description", status="pending")
    
    job = manager.get_job(job_id)
    assert job is not None
    assert job["job_id"] == job_id
    assert job["nl_description"] == "Test description"
    assert job["status"] == "pending"
    assert "created_at" in job


def test_get_nonexistent_job():
    """Test getting non-existent job."""
    manager = JobManager()
    job = manager.get_job("nonexistent")
    assert job is None


def test_update_job():
    """Test updating job fields."""
    manager = JobManager()
    job_id = "test-job-456"
    
    manager.create_job(job_id, "Test description")
    manager.update_job(job_id, status="in_progress", phase=1, step="1.1", progress=0.5)
    
    job = manager.get_job(job_id)
    assert job["status"] == "in_progress"
    assert job["phase"] == 1
    assert job["step"] == "1.1"
    assert job["progress"] == 0.5


def test_update_job_state():
    """Test updating job state."""
    manager = JobManager()
    job_id = "test-job-789"
    
    manager.create_job(job_id, "Test description")
    new_state = {"entities": [{"name": "Customer"}]}
    manager.update_job_state(job_id, new_state)
    
    job = manager.get_job(job_id)
    assert job["state"] == new_state


def test_update_nonexistent_job():
    """Test updating non-existent job (should not raise error)."""
    manager = JobManager()
    manager.update_job("nonexistent", status="in_progress")
    # Should not raise error, just do nothing


def test_multiple_jobs():
    """Test managing multiple jobs."""
    manager = JobManager()
    
    job1_id = "job-1"
    job2_id = "job-2"
    
    manager.create_job(job1_id, "Description 1")
    manager.create_job(job2_id, "Description 2")
    
    job1 = manager.get_job(job1_id)
    job2 = manager.get_job(job2_id)
    
    assert job1["nl_description"] == "Description 1"
    assert job2["nl_description"] == "Description 2"
    assert job1["job_id"] != job2["job_id"]



