"""Request models for API endpoints."""

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


class ProcessStartRequest(BaseModel):
    """Request to start NL2DATA processing."""
    nl_description: str = Field(..., min_length=1, description="Natural language description of the database")


class SuggestionsRequest(BaseModel):
    """Request for keyword suggestions."""
    nl_description: str = Field(..., min_length=1, description="Current NL description")


class SaveChangesRequest(BaseModel):
    """Request to save schema changes."""
    job_id: str = Field(..., pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    edit_mode: str = Field(..., pattern=r"^(er_diagram|relational_schema)$")
    changes: Dict[str, Any] = Field(..., description="Changes to apply")


class ToyDataGenerationRequest(BaseModel):
    """Request to generate toy dataset."""
    job_id: str
    config: Dict[str, Any]
    state: Dict[str, Any]


class CSVGenerationRequest(BaseModel):
    """Request to generate CSV files."""
    job_id: str
    config: Dict[str, Any]
    state: Dict[str, Any]



