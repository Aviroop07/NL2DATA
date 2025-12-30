"""WebSocket event models."""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime


class Scope(BaseModel):
    """Scope information for a step (entity, attribute, relation)."""
    entity: Optional[str] = None
    attribute: Optional[str] = None
    relation: Optional[str] = None


class APICallInfo(BaseModel):
    """Information about an API call."""
    estimated_tokens: Optional[int] = None
    tokens_used: Optional[int] = None
    response_time_ms: Optional[int] = None
    call_type: Optional[str] = None


class APIRequestStartEvent(BaseModel):
    """Event when an API request starts."""
    type: Literal["api_request_start"] = "api_request_start"
    data: "APIRequestStartData"


class APIRequestStartData(BaseModel):
    """Data for API request start event."""
    job_id: str
    seq: int
    ts: datetime
    phase: int
    step: str
    step_name: str
    step_id: str
    scope: Scope
    api_call: APICallInfo
    message: str


class APIResponseSuccessEvent(BaseModel):
    """Event when an API response succeeds."""
    type: Literal["api_response_success"] = "api_response_success"
    data: "APIResponseSuccessData"


class APIResponseSuccessData(BaseModel):
    """Data for API response success event."""
    job_id: str
    seq: int
    ts: datetime
    phase: int
    step: str
    step_name: str
    step_id: str
    scope: Scope
    api_call: APICallInfo
    message: str


class StatusTickEvent(BaseModel):
    """Event for status tick updates."""
    type: Literal["status_tick"] = "status_tick"
    data: "StatusTickData"


class StatusTickData(BaseModel):
    """Data for status tick event."""
    job_id: str
    seq: int
    ts: datetime
    phase: int
    step: str
    step_name: str
    scope: Scope
    message: str
    level: Literal["info", "warning", "error"] = "info"


class StepStartEvent(BaseModel):
    """Event when a step starts."""
    type: Literal["step_start"] = "step_start"
    data: "StepLifecycleData"


class StepCompleteEvent(BaseModel):
    """Event when a step completes."""
    type: Literal["step_complete"] = "step_complete"
    data: "StepLifecycleData"


class StepLifecycleData(BaseModel):
    """Shared payload for step start/complete events."""
    job_id: str
    seq: int
    ts: datetime
    phase: int
    step: str
    step_name: str
    step_id: str
    scope: Scope
    message: str
    summary: Optional[dict] = None


# Update forward references
APIRequestStartEvent.model_rebuild()
APIResponseSuccessEvent.model_rebuild()
StatusTickEvent.model_rebuild()
StepStartEvent.model_rebuild()
StepCompleteEvent.model_rebuild()



