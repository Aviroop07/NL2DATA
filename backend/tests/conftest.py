"""Pytest fixtures and configuration."""

import pytest
from fastapi.testclient import TestClient
from backend.main import app
from backend.utils.job_manager import JobManager
from backend.utils.websocket_manager import WebSocketManager
from backend.services.status_ticker_service import StatusTickerService
from backend.services.validation_service import ValidationService
from backend.services.conversion_service import ConversionService
from backend.services.diagram_service import DiagramService
from backend.services.suggestion_service import SuggestionService


@pytest.fixture
def client():
    """Test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def job_manager():
    """Fresh JobManager instance for testing."""
    return JobManager()


@pytest.fixture
def ws_manager():
    """Fresh WebSocketManager instance for testing."""
    return WebSocketManager()


@pytest.fixture
def status_ticker():
    """StatusTickerService instance for testing."""
    return StatusTickerService()


@pytest.fixture
def validation_service():
    """ValidationService instance for testing."""
    return ValidationService()


@pytest.fixture
def conversion_service():
    """ConversionService instance for testing."""
    return ConversionService()


@pytest.fixture
def diagram_service():
    """DiagramService instance for testing."""
    return DiagramService()


@pytest.fixture
def suggestion_service():
    """SuggestionService instance for testing."""
    return SuggestionService()


@pytest.fixture
def sample_nl_description():
    """Sample NL description for testing."""
    return "I need a database for an e-commerce system with customers, products, and orders. Customers can place multiple orders, and each order contains multiple products."


@pytest.fixture
def sample_job_id():
    """Sample job ID for testing."""
    return "test-job-12345-67890-abcdef"


@pytest.fixture
def sample_state():
    """Sample GenerationState-like dict for testing."""
    return {
        "description": "Test description",
        "metadata": {},
        "entities": [
            {
                "name": "Customer",
                "description": "Customer entity",
                "cardinality": "large",
                "attributes": [
                    {"name": "customer_id", "type_hint": "integer"},
                    {"name": "name", "type_hint": "string"}
                ]
            }
        ],
        "relations": [],
        "er_design": {
            "entities": [
                {
                    "name": "Customer",
                    "attributes": [
                        {"name": "customer_id"},
                        {"name": "name"}
                    ]
                }
            ],
            "relations": []
        },
        "relational_schema": {
            "tables": [
                {
                    "name": "Customer",
                    "columns": [
                        {"name": "customer_id", "data_type": "INT"},
                        {"name": "name", "data_type": "VARCHAR"}
                    ]
                }
            ]
        }
    }



