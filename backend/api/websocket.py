"""WebSocket handler for real-time updates."""

import logging
from fastapi import WebSocket, WebSocketDisconnect, Depends
from backend.dependencies import get_websocket_manager, get_job_manager
from backend.utils.websocket_manager import WebSocketManager
from backend.utils.job_manager import JobManager

logger = logging.getLogger(__name__)


async def websocket_endpoint(
    websocket: WebSocket,
    job_id: str,
    ws_manager: WebSocketManager = Depends(get_websocket_manager),
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    WebSocket endpoint for real-time updates.
    
    Client connects with job_id, receives:
    - status_tick events (progress updates)
    - api_request_start / api_response_success events (LLM call tracking)
    - phase_complete events (when phases finish)
    - error events (on failures)
    """
    logger.info("=" * 80)
    logger.info(f"WEBSOCKET ENDPOINT: Connection request for job {job_id}")
    logger.info("=" * 80)
    
    await ws_manager.connect(websocket, job_id)
    
    try:
        # Send connection confirmation
        logger.info("Sending connection confirmation message...")
        await websocket.send_json({
            "type": "connected",
            "data": {
                "job_id": job_id,
                "message": "WebSocket connection established"
            }
        })
        logger.info("Connection confirmation sent")
        
        # Keep connection alive and handle incoming messages
        message_count = 0
        while True:
            try:
                logger.debug(f"Waiting for message from client (job {job_id})...")
                data = await websocket.receive_text()
                message_count += 1
                logger.info(f"Received message #{message_count} from client: {data[:100]}...")
                
                # Handle client messages if needed (e.g., ping/pong)
                # For now, just echo or ignore
                if data == "ping":
                    logger.info("Received ping, sending pong...")
                    await websocket.send_text("pong")
                    logger.info("Pong sent")
            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected by client (job {job_id})")
                break
            
            # Check if job exists
            job = job_manager.get_job(job_id)
            if not job:
                logger.warning(f"Job {job_id} not found, sending error and disconnecting")
                await websocket.send_json({
                    "type": "error",
                    "data": {
                        "message": "Job not found",
                        "error_type": "job_not_found"
                    }
                })
                break
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected (job {job_id})")
    except Exception as e:
        logger.error(f"Error in WebSocket endpoint (job {job_id}): {e}")
        logger.exception("Full traceback:")
    finally:
        logger.info(f"Cleaning up WebSocket connection for job {job_id}")
        ws_manager.disconnect(job_id, websocket)
        logger.info("=" * 80)

