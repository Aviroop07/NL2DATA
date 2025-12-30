"""Standardized error handling for NL2DATA steps.

Provides consistent error handling, logging, and error response creation.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
import traceback

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ErrorContext:
    """Context information for error handling."""
    step_id: str
    phase: int
    entity_name: Optional[str] = None
    relation_id: Optional[str] = None
    attribute_name: Optional[str] = None
    information_need: Optional[str] = None
    additional_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepError(Exception):
    """Standardized error for step failures."""
    message: str
    context: ErrorContext
    original_exception: Optional[Exception] = None
    error_type: str = "step_error"
    
    def __str__(self) -> str:
        return f"[{self.context.step_id}] {self.message}"


def log_error_with_context(
    error: Exception,
    context: ErrorContext,
    level: str = "error"
) -> None:
    """
    Log error with full context information.
    
    Args:
        error: The exception that occurred
        context: Error context information
        level: Log level ("error", "warning", "critical")
    """
    log_msg_parts = [
        f"Error in {context.step_id} (Phase {context.phase})"
    ]
    
    if context.entity_name:
        log_msg_parts.append(f"Entity: {context.entity_name}")
    if context.relation_id:
        log_msg_parts.append(f"Relation: {context.relation_id}")
    if context.attribute_name:
        log_msg_parts.append(f"Attribute: {context.attribute_name}")
    if context.information_need:
        log_msg_parts.append(f"Information Need: {context.information_need}")
    
    log_msg = " | ".join(log_msg_parts)
    
    if level == "critical":
        logger.critical(f"{log_msg}: {error}", exc_info=True)
    elif level == "warning":
        logger.warning(f"{log_msg}: {error}", exc_info=True)
    else:
        logger.error(f"{log_msg}: {error}", exc_info=True)
    
    # Log additional context if provided
    if context.additional_context:
        logger.debug(f"Additional context: {context.additional_context}")


def create_error_response(
    error: Exception,
    context: ErrorContext,
    return_partial: bool = False
) -> Dict[str, Any]:
    """
    Create standardized error response dictionary.
    
    Args:
        error: The exception that occurred
        context: Error context information
        return_partial: If True, return partial results if available
        
    Returns:
        Dictionary with error information and optional partial results
    """
    error_response = {
        "success": False,
        "error": {
            "type": type(error).__name__,
            "message": str(error),
            "step_id": context.step_id,
            "phase": context.phase,
            "timestamp": datetime.now().isoformat(),
        }
    }
    
    # Add context-specific fields
    if context.entity_name:
        error_response["error"]["entity_name"] = context.entity_name
    if context.relation_id:
        error_response["error"]["relation_id"] = context.relation_id
    if context.attribute_name:
        error_response["error"]["attribute_name"] = context.attribute_name
    if context.information_need:
        error_response["error"]["information_need"] = context.information_need
    
    # Add traceback for debugging (truncated)
    if hasattr(error, "__traceback__"):
        tb_str = "".join(traceback.format_tb(error.__traceback__))
        # Truncate to last 500 chars to avoid huge error responses
        error_response["error"]["traceback"] = tb_str[-500:] if len(tb_str) > 500 else tb_str
    
    # Add additional context
    if context.additional_context:
        error_response["error"]["additional_context"] = context.additional_context
    
    return error_response


def handle_step_error(
    error: Exception,
    context: ErrorContext,
    return_partial: bool = False,
    log_level: str = "error",
    reraise: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Handle step error with standardized logging and response creation.
    
    This is the main error handling function that should be used throughout
    the codebase for consistent error handling.
    
    Args:
        error: The exception that occurred
        context: Error context information
        return_partial: If True, return partial results if available
        log_level: Log level ("error", "warning", "critical")
        reraise: If True, re-raise the exception after handling
        
    Returns:
        Error response dictionary (if return_partial=False) or None
        
    Raises:
        StepError: If reraise=True, wraps original error in StepError
    """
    # Log error with context
    log_error_with_context(error, context, level=log_level)
    
    # Create error response
    error_response = create_error_response(error, context, return_partial=return_partial)
    
    # Re-raise if requested
    if reraise:
        step_error = StepError(
            message=str(error),
            context=context,
            original_exception=error,
            error_type=type(error).__name__
        )
        raise step_error from error
    
    return error_response


def wrap_step_with_error_handling(
    step_func,
    context: ErrorContext,
    return_partial: bool = False
):
    """
    Decorator/wrapper to add standardized error handling to step functions.
    
    Args:
        step_func: The step function to wrap
        context: Error context (can be partial, will be updated with step info)
        return_partial: If True, return partial results on error
        
    Returns:
        Wrapped function with error handling
    """
    async def wrapped(*args, **kwargs):
        try:
            return await step_func(*args, **kwargs)
        except Exception as e:
            # Update context with function name if not set
            if not context.step_id:
                context.step_id = step_func.__name__
            
            return handle_step_error(
                e,
                context,
                return_partial=return_partial,
                reraise=True  # Re-raise to maintain error propagation
            )
    
    return wrapped

