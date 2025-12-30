"""WebSocket connection management."""

from typing import Dict, Set, Optional, Any
from fastapi import WebSocket
from datetime import datetime, UTC
import json
import logging

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages WebSocket connections per job_id."""
    
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.sequence_numbers: Dict[str, int] = {}
    
    async def connect(self, websocket: WebSocket, job_id: str):
        """Accept and register a WebSocket connection."""
        logger.info("=" * 80)
        logger.info(f"WEBSOCKET: Connection request for job {job_id}")
        logger.info("=" * 80)
        
        await websocket.accept()
        logger.info(f"WebSocket accepted for job {job_id}")
        
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
            self.sequence_numbers[job_id] = 0
            logger.info(f"Created new connection set for job {job_id}")
        else:
            logger.info(f"Adding to existing connection set for job {job_id} (current count: {len(self.active_connections[job_id])})")
        
        self.active_connections[job_id].add(websocket)
        logger.info(f"WebSocket connected for job {job_id} (total connections: {len(self.active_connections[job_id])})")
        logger.info("=" * 80)
    
    def disconnect(self, job_id: str, websocket: WebSocket = None):
        """Remove connection(s) for job_id."""
        logger.info(f"WEBSOCKET: Disconnect request for job {job_id}")
        
        if job_id in self.active_connections:
            if websocket:
                logger.info(f"Removing specific websocket for job {job_id}")
                self.active_connections[job_id].discard(websocket)
            else:
                logger.info(f"Clearing all connections for job {job_id}")
                self.active_connections[job_id].clear()
            
            if not self.active_connections[job_id]:
                logger.info(f"No more connections for job {job_id}, cleaning up")
                del self.active_connections[job_id]
                del self.sequence_numbers[job_id]
        else:
            logger.warning(f"Attempted to disconnect job {job_id} but no connections found")
    
    def _get_next_seq(self, job_id: str) -> int:
        """Get next sequence number for job_id."""
        if job_id not in self.sequence_numbers:
            self.sequence_numbers[job_id] = 0
        self.sequence_numbers[job_id] += 1
        return self.sequence_numbers[job_id]
    
    async def send_event(self, job_id: str, event: dict):
        """Send event to all connections for job_id."""
        if job_id not in self.active_connections:
            logger.warning(f"WEBSOCKET: No active connections for job {job_id}, cannot send event {event.get('type', 'unknown')}")
            return
        
        event_type = event.get("type", "unknown")
        logger.debug(f"WEBSOCKET: Sending event '{event_type}' to job {job_id} ({len(self.active_connections[job_id])} connections)")
        
        # Add sequence number and timestamp
        if "data" in event and isinstance(event["data"], dict):
            seq = self._get_next_seq(job_id)
            event["data"]["seq"] = seq
            if "ts" not in event["data"]:
                event["data"]["ts"] = datetime.now(UTC).isoformat()
            # Convert datetime objects to ISO strings for JSON serialization
            if isinstance(event["data"].get("ts"), datetime):
                event["data"]["ts"] = event["data"]["ts"].isoformat()
            
            logger.debug(f"  Event seq: {seq}, timestamp: {event['data'].get('ts', 'N/A')}")
        
        message = json.dumps(event)
        message_size = len(message)
        logger.debug(f"  Message size: {message_size} bytes")
        
        disconnected = set()
        sent_count = 0
        
        for websocket in self.active_connections[job_id]:
            try:
                await websocket.send_text(message)
                sent_count += 1
                logger.debug(f"  Sent to connection {sent_count}/{len(self.active_connections[job_id])}")
            except Exception as e:
                logger.error(f"  Error sending WebSocket message to connection: {e}")
                logger.exception("  Full traceback:")
                disconnected.add(websocket)
        
        if disconnected:
            logger.warning(f"  Disconnected {len(disconnected)} websocket(s) due to send errors")
        
        # Remove disconnected websockets
        for ws in disconnected:
            self.active_connections[job_id].discard(ws)
        
        if sent_count > 0:
            logger.info(f"WEBSOCKET: Successfully sent '{event_type}' to {sent_count} connection(s) for job {job_id}")
    
    async def send_api_request_start(
        self,
        job_id: str,
        phase: int,
        step: str,
        step_name: str,
        step_id: str,
        scope: Dict[str, Any],
        api_call: Dict[str, Any],
        message: str
    ):
        """Send API request start event."""
        from backend.models.websocket_events import APIRequestStartEvent, Scope, APICallInfo
        
        event = APIRequestStartEvent(
            type="api_request_start",
            data={
                "job_id": job_id,
                "seq": 0,  # Will be set by send_event
                "ts": datetime.now(UTC),
                "phase": phase,
                "step": step,
                "step_name": step_name,
                "step_id": step_id,
                "scope": Scope(**scope),
                "api_call": APICallInfo(**api_call),
                "message": message
            }
        )
        
        await self.send_event(job_id, event.model_dump())
    
    async def send_api_response_success(
        self,
        job_id: str,
        phase: int,
        step: str,
        step_name: str,
        step_id: str,
        scope: Dict[str, Any],
        api_call: Dict[str, Any],
        message: str
    ):
        """Send API response success event."""
        from backend.models.websocket_events import APIResponseSuccessEvent, Scope, APICallInfo
        
        event = APIResponseSuccessEvent(
            type="api_response_success",
            data={
                "job_id": job_id,
                "seq": 0,  # Will be set by send_event
                "ts": datetime.now(UTC),
                "phase": phase,
                "step": step,
                "step_name": step_name,
                "step_id": step_id,
                "scope": Scope(**scope),
                "api_call": APICallInfo(**api_call),
                "message": message
            }
        )
        
        await self.send_event(job_id, event.model_dump())
    
    async def send_status_tick(
        self,
        job_id: str,
        phase: int,
        step: str,
        step_name: str,
        scope: Dict[str, Any],
        message: str,
        level: str = "info"
    ):
        """Send status tick event."""
        from backend.models.websocket_events import StatusTickEvent, Scope
        
        event = StatusTickEvent(
            type="status_tick",
            data={
                "job_id": job_id,
                "seq": 0,  # Will be set by send_event
                "ts": datetime.now(UTC).isoformat(),
                "phase": phase,
                "step": step,
                "step_name": step_name,
                "scope": Scope(**scope),
                "message": message,
                "level": level
            }
        )
        
        await self.send_event(job_id, event.model_dump())

    async def send_step_start(
        self,
        job_id: str,
        phase: int,
        step: str,
        step_name: str,
        step_id: str,
        scope: Dict[str, Any],
        message: str,
    ):
        """Send step start event."""
        from backend.models.websocket_events import StepStartEvent, Scope
        
        event = StepStartEvent(
            data={
                "job_id": job_id,
                "seq": 0,  # set by send_event
                "ts": datetime.now(UTC),
                "phase": phase,
                "step": step,
                "step_name": step_name,
                "step_id": step_id,
                "scope": Scope(**scope),
                "message": message,
                "summary": None,
            }
        )
        await self.send_event(job_id, event.model_dump())

    async def send_step_complete(
        self,
        job_id: str,
        phase: int,
        step: str,
        step_name: str,
        step_id: str,
        scope: Dict[str, Any],
        message: str,
        summary: Optional[Dict[str, Any]] = None,
    ):
        """Send step complete event with optional summary."""
        from backend.models.websocket_events import StepCompleteEvent, Scope
        
        event = StepCompleteEvent(
            data={
                "job_id": job_id,
                "seq": 0,  # set by send_event
                "ts": datetime.now(UTC),
                "phase": phase,
                "step": step,
                "step_name": step_name,
                "step_id": step_id,
                "scope": Scope(**scope),
                "message": message,
                "summary": summary or {},
            }
        )
        await self.send_event(job_id, event.model_dump())

