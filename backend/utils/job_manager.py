"""Job state and lifecycle management."""

from typing import Dict, Any, Optional
from datetime import datetime, UTC
import uuid


class JobManager:
    """Manages job state and lifecycle."""
    
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
    
    def create_job(
        self,
        job_id: str,
        nl_description: str,
        status: str = "pending"
    ):
        """Create a new job."""
        self.jobs[job_id] = {
            "job_id": job_id,
            "nl_description": nl_description,
            "status": status,
            "created_at": datetime.now(UTC).isoformat(),
            "state": {},
            "phase": None,
            "step": None,
            "progress": 0.0
        }
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        phase: Optional[int] = None,
        step: Optional[str] = None,
        progress: Optional[float] = None,
        state: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ):
        """Update job fields."""
        if job_id not in self.jobs:
            return
        
        if status:
            self.jobs[job_id]["status"] = status
        if phase is not None:
            self.jobs[job_id]["phase"] = phase
        if step:
            self.jobs[job_id]["step"] = step
        if progress is not None:
            self.jobs[job_id]["progress"] = progress
        if state:
            self.jobs[job_id]["state"] = state
        if error:
            self.jobs[job_id]["error"] = error
    
    def update_job_state(self, job_id: str, state: Dict[str, Any]):
        """Update job state."""
        if job_id in self.jobs:
            self.jobs[job_id]["state"] = state



