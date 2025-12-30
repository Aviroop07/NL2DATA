"""Response models for API endpoints."""

from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class ProcessStartResponse(BaseModel):
    """Response when starting processing."""
    job_id: str
    status: str
    created_at: str


class ProcessStatusResponse(BaseModel):
    """Response for job status."""
    job_id: str
    status: str
    phase: Optional[int] = None
    step: Optional[str] = None
    progress: float = 0.0


class KeywordSuggestion(BaseModel):
    """A keyword suggestion with enhanced NL description."""
    text: str
    type: str  # "domain" | "entity" | "constraint" | "attribute" | "relationship" | "distribution"
    enhanced_nl_description: str


class ExtractedItems(BaseModel):
    """Items extracted from NL description for quality calculation."""
    domain: Optional[str] = None
    entities: List[str] = []
    cardinalities: List[str] = []
    column_names: List[str] = []
    constraints: List[str] = []
    relationships: List[str] = []


class SuggestionsResponse(BaseModel):
    """Response with keyword suggestions and extracted items."""
    keywords: List[KeywordSuggestion]
    extracted_items: ExtractedItems


class ValidationError(BaseModel):
    """A validation error with fix suggestion."""
    type: str
    entity: Optional[str] = None
    attribute: Optional[str] = None
    message: str
    fix_suggestion: str


class SaveChangesResponse(BaseModel):
    """Response when saving schema changes."""
    status: str  # "success" | "validation_failed"
    updated_state: Optional[Dict[str, Any]] = None
    validation_errors: List[ValidationError] = []
    er_diagram_image_url: Optional[str] = None


class DistributionParameter(BaseModel):
    """A distribution parameter definition."""
    name: str
    type: str  # "decimal" | "integer" | "array" | "string"
    description: Optional[str] = None


class DistributionMetadata(BaseModel):
    """Metadata for a distribution type."""
    name: str
    parameters: List[DistributionParameter]


class DistributionMetadataResponse(BaseModel):
    """Response with all available distributions."""
    distributions: List[DistributionMetadata]



