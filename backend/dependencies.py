"""FastAPI dependencies - process-wide singletons for demo."""

from functools import lru_cache
from backend.utils.job_manager import JobManager
from backend.utils.websocket_manager import WebSocketManager
from backend.services.nl2data_service import NL2DataService
from backend.services.validation_service import ValidationService
from backend.services.conversion_service import ConversionService
from backend.services.diagram_service import DiagramService
from backend.services.suggestion_service import SuggestionService


# Process-wide singletons (for demo - single process, no DB)
@lru_cache(maxsize=1)
def get_job_manager() -> JobManager:
    """Singleton JobManager - shared across all requests."""
    return JobManager()


@lru_cache(maxsize=1)
def get_websocket_manager() -> WebSocketManager:
    """Singleton WebSocketManager - for WebSocket connections (currently not used for pipeline)."""
    return WebSocketManager()


@lru_cache(maxsize=1)
def get_validation_service() -> ValidationService:
    """Singleton ValidationService."""
    return ValidationService()


@lru_cache(maxsize=1)
def get_conversion_service() -> ConversionService:
    """Singleton ConversionService."""
    return ConversionService()


@lru_cache(maxsize=1)
def get_diagram_service() -> DiagramService:
    """Singleton DiagramService."""
    return DiagramService()


@lru_cache(maxsize=1)
def get_suggestion_service() -> SuggestionService:
    """Singleton SuggestionService."""
    return SuggestionService()


@lru_cache(maxsize=1)
def get_nl2data_service() -> NL2DataService:
    """Create NL2DataService with dependencies (singleton)."""
    return NL2DataService(
        job_manager=get_job_manager()
    )

