"""Setup logging configuration."""

import logging
import sys
from pathlib import Path
from typing import Optional


def clear_log_file(log_file: Optional[str] = None) -> None:
    """
    Clear the log file if it exists.
    
    Args:
        log_file: Path to log file (relative to NL2DATA root). If None, uses default.
    """
    if log_file is None:
        log_file = "logs/nl2data.log"
    
    # Resolve log file path relative to NL2DATA root
    nl2data_root = Path(__file__).parent.parent.parent
    log_path = nl2data_root / log_file
    
    # Create logs directory if it doesn't exist
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Clear the log file if it exists
    if log_path.exists():
        try:
            log_path.unlink()
        except PermissionError:
            # File is locked (e.g., open in IDE) - skip clearing
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Cannot clear log file {log_path} - file is locked. Continuing without clearing.")


def setup_logging(
    level: str = "INFO",
    format_type: str = "detailed",
    log_to_file: bool = True,
    log_file: Optional[str] = None,
    clear_existing: bool = False,
) -> None:
    """
    Setup logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: "simple" or "detailed"
        log_to_file: Whether to log to file
        log_file: Path to log file (relative to NL2DATA root)
        clear_existing: Whether to clear the log file before setting up logging
    """
    # Clear log file if requested
    if clear_existing and log_to_file:
        clear_log_file(log_file)
    
    # Convert string level to logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatters
    if format_type == "detailed":
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    else:
        formatter = logging.Formatter(
            fmt="%(levelname)s | %(name)s | %(message)s"
        )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        if log_file is None:
            log_file = "logs/nl2data.log"
        
        # Resolve log file path relative to NL2DATA root
        nl2data_root = Path(__file__).parent.parent.parent
        log_path = nl2data_root / log_file
        
        # Create logs directory if it doesn't exist
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)

