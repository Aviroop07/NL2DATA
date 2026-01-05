"""Configuration settings for the backend API."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
from pathlib import Path
import tempfile


class Settings(BaseSettings):
    """Application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"  # Ignore extra environment variables
    )
    
    # API
    api_title: str = "NL2DATA Backend API"
    api_version: str = "1.0.0"
    # Default to common local dev origins (Vite=5173, CRA=3000).
    # Can be overridden via env var: CORS_ORIGINS='["http://localhost:5173"]'
    cors_origins: List[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    # NL2DATA
    nl2data_config_path: str = "NL2DATA/config/config.yaml"
    
    # WebSocket
    websocket_timeout: int = 300
    
    # File storage
    # Use OS temp dir by default (works on Windows/Linux/macOS).
    csv_storage_path: str = str(Path(tempfile.gettempdir()) / "nl2data_csv")
    csv_cleanup_after_hours: int = 24
    
    # ER diagram storage
    # Store ER diagram images in backend/static/er_diagrams/
    # Note: The generate_and_save_er_diagram function will create the er_diagrams subdirectory
    er_diagram_storage_path: str = str(Path(__file__).parent / "static")


settings = Settings()

