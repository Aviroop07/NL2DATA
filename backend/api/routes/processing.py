"""Processing endpoints."""

import logging
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from uuid import uuid4
from datetime import datetime, UTC

from backend.models.requests import ProcessStartRequest
from backend.models.responses import ProcessStartResponse, ProcessStatusResponse
from backend.dependencies import (
    get_job_manager,
    get_nl2data_service
)
from backend.utils.job_manager import JobManager
from backend.services.nl2data_service import NL2DataService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/process", tags=["processing"])


@router.post("/start", response_model=ProcessStartResponse)
async def start_processing(
    request: ProcessStartRequest,
    background_tasks: BackgroundTasks,
    job_manager: JobManager = Depends(get_job_manager),
    nl2data_service: NL2DataService = Depends(get_nl2data_service)
):
    """
    Start NL2DATA processing pipeline with checkpoint-based workflow.
    
    Creates a new job, executes to first checkpoint (domain) in background, returns job_id immediately.
    """
    import time
    start_time = time.time()
    
    job_id = str(uuid4())
    
    logger.info("=" * 80)
    logger.info("API ENDPOINT: POST /api/process/start")
    logger.info("=" * 80)
    logger.info(f"Request received at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Generated Job ID: {job_id}")
    logger.info(f"NL Description Length: {len(request.nl_description)} characters")
    logger.info(f"NL Description Preview: {request.nl_description[:100]}...")
    logger.debug(f"Full NL Description: {request.nl_description}")
    
    # Register job
    logger.info("Registering job in JobManager...")
    job_manager.create_job(
        job_id=job_id,
        nl_description=request.nl_description,
        status="pending"
    )
    logger.info(f"Job {job_id} registered with status: pending")
    
    # Start processing to first checkpoint (domain) in background
    logger.info("Adding background task to execute to domain checkpoint...")
    background_tasks.add_task(
        nl2data_service.execute_to_checkpoint,
        job_id=job_id,
        nl_description=request.nl_description,
        checkpoint_type="domain",
        job_manager=job_manager,
        current_state=None
    )
    logger.info(f"Background task added for job {job_id}")
    
    elapsed_time = time.time() - start_time
    response = ProcessStartResponse(
        job_id=job_id,
        status="started",
        created_at=datetime.now(UTC).isoformat()
    )
    
    logger.info("-" * 80)
    logger.info("API RESPONSE:")
    logger.info(f"  Job ID: {job_id}")
    logger.info(f"  Status: {response.status}")
    logger.info(f"  Created At: {response.created_at}")
    logger.info(f"  Processing Time: {elapsed_time:.3f} seconds")
    logger.info("=" * 80)
    
    return response


@router.get("/status/{job_id}", response_model=ProcessStatusResponse)
async def get_status(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Get current status of a processing job.
    
    Note: Real-time updates are via WebSocket. This endpoint is for
    initial status check or reconnection scenarios.
    """
    import time
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info(f"API ENDPOINT: GET /api/process/status/{job_id}")
    logger.info("=" * 80)
    logger.info(f"Request received at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    job = job_manager.get_job(job_id)
    if not job:
        logger.warning(f"Job {job_id} not found")
        logger.info("=" * 80)
        raise HTTPException(status_code=404, detail="Job not found")
    
    logger.info(f"Job {job_id} found:")
    logger.info(f"  Status: {job.get('status')}")
    logger.info(f"  Phase: {job.get('phase')}")
    logger.info(f"  Step: {job.get('step')}")
    logger.info(f"  Progress: {job.get('progress', 0.0)}%")
    
    elapsed_time = time.time() - start_time
    response = ProcessStatusResponse(
        job_id=job_id,
        status=job["status"],
        phase=job.get("phase"),
        step=job.get("step"),
        progress=job.get("progress", 0.0)
    )
    
    logger.info(f"Processing Time: {elapsed_time:.3f} seconds")
    logger.info("=" * 80)
    
    return response
