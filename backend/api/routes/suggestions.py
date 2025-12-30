"""Suggestions endpoint."""

import logging
from fastapi import APIRouter, Depends
from backend.models.requests import SuggestionsRequest
from backend.models.responses import SuggestionsResponse
from backend.dependencies import get_suggestion_service
from backend.services.suggestion_service import SuggestionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/suggestions", tags=["suggestions"])


@router.post("", response_model=SuggestionsResponse)
async def get_suggestions(
    request: SuggestionsRequest,
    suggestion_service: SuggestionService = Depends(get_suggestion_service)
):
    """
    Get keyword suggestions and enhanced NL descriptions.
    
    Backend analyzes NL description and returns:
    - 4-5 keywords (fixed types: domain, entity, constraint, attribute, relationship, distribution)
    - Each keyword has its own enhanced NL description
    - Extracted items for client-side quality calculation
    
    NOTE: This is a REST endpoint (not WebSocket). Frontend polls every 5 seconds.
    WebSocket is used only for pipeline progress updates.
    """
    import time
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("API ENDPOINT: POST /api/suggestions")
    logger.info("=" * 80)
    logger.info(f"Request received at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"NL Description Length: {len(request.nl_description)} characters")
    logger.info(f"NL Description Preview: {request.nl_description[:100]}...")
    logger.debug(f"Full Request NL Description: {request.nl_description}")
    
    try:
        result = await suggestion_service.analyze_and_suggest(request.nl_description)
        
        elapsed_time = time.time() - start_time
        logger.info("-" * 80)
        logger.info("API RESPONSE:")
        logger.info(f"  Status: SUCCESS")
        logger.info(f"  Processing Time: {elapsed_time:.2f} seconds")
        logger.info(f"  Keywords Returned: {len(result.keywords)}")
        logger.info(f"  Extracted Items - Entities: {len(result.extracted_items.entities)}")
        logger.info(f"  Extracted Items - Columns: {len(result.extracted_items.column_names)}")
        logger.info(f"  Extracted Items - Relationships: {len(result.extracted_items.relationships)}")
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error("=" * 80)
        logger.error("API ENDPOINT: ERROR in /api/suggestions")
        logger.error("=" * 80)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception message: {str(e)}")
        logger.error(f"Processing Time: {elapsed_time:.2f} seconds")
        logger.exception("Full traceback:")
        logger.error("=" * 80)
        raise
