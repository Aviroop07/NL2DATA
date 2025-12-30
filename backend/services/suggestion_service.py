"""Suggestion service - analyzes NL description and generates keyword suggestions."""

import logging
import time
from pathlib import Path
from backend.models.responses import SuggestionsResponse, KeywordSuggestion, ExtractedItems
from backend.utils.llm_client import load_prompt, call_llm_with_pydantic
from pydantic import BaseModel, Field
from typing import List

logger = logging.getLogger(__name__)


class SuggestionsOutput(BaseModel):
    """LLM output schema for suggestions."""
    keywords: List[KeywordSuggestion] = Field(..., description="List of keyword suggestions (4-5 keywords that are NOT already in the description)")
    extracted_items: ExtractedItems = Field(..., description="Items extracted from NL description (existing keywords/entities for quality calculation)")


class SuggestionService:
    """Analyzes NL description and generates keyword suggestions."""
    
    def __init__(self):
        """Initialize service and load prompt."""
        try:
            self.prompt_template = load_prompt("suggestion_prompt.txt")
            logger.info("Loaded suggestion prompt from backend/prompts/suggestion_prompt.txt")
        except Exception as e:
            logger.error(f"Failed to load prompt: {e}")
            # Fallback prompt
            self.prompt_template = """Analyze this natural language database description and extract existing items, then suggest 4-5 new keywords.

Description: "{nl_description}"

Return JSON with keywords and extracted_items."""
    
    async def analyze_and_suggest(self, nl_description: str) -> SuggestionsResponse:
        """
        Analyze NL description and return suggestions.
        
        Uses LLM to:
        1. Extract explicit items (domain, entities, etc.) for quality calculation
        2. Generate 4-5 keywords that are NOT already in the description
        3. For each keyword, create a minimally modified NL description that adds the keyword
           without removing any existing information
        """
        logger.info("=" * 80)
        logger.info("SUGGESTION SERVICE: Starting analysis")
        logger.info("=" * 80)
        logger.info(f"Input NL Description Length: {len(nl_description)} characters")
        logger.info(f"Input NL Description (first 200 chars): {nl_description[:200]}...")
        logger.debug(f"Full NL Description: {nl_description}")
        
        try:
            start_time = time.time()
            
            # Call LLM with single prompt
            logger.info("Making LLM call with single prompt...")
            result: SuggestionsOutput = await call_llm_with_pydantic(
                prompt_text=self.prompt_template,
                output_schema=SuggestionsOutput,
                input_data={"nl_description": nl_description},
                model_name="gpt-4o-mini",
                temperature=0.1
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"LLM call completed in {elapsed_time:.2f} seconds")
            
            # Log extracted items
            logger.info("-" * 80)
            logger.info("EXTRACTED ITEMS (for quality calculation):")
            logger.info("-" * 80)
            logger.info(f"  Domain: {result.extracted_items.domain}")
            logger.info(f"  Entities ({len(result.extracted_items.entities)}): {result.extracted_items.entities}")
            logger.info(f"  Column Names ({len(result.extracted_items.column_names)}): {result.extracted_items.column_names}")
            logger.info(f"  Relationships ({len(result.extracted_items.relationships)}): {result.extracted_items.relationships}")
            logger.info(f"  Constraints ({len(result.extracted_items.constraints)}): {result.extracted_items.constraints}")
            logger.info(f"  Cardinalities ({len(result.extracted_items.cardinalities)}): {result.extracted_items.cardinalities}")
            
            # Log keyword suggestions
            logger.info("-" * 80)
            logger.info(f"KEYWORD SUGGESTIONS ({len(result.keywords)}):")
            logger.info("-" * 80)
            for idx, keyword in enumerate(result.keywords, 1):
                logger.info(f"  [{idx}] {keyword.text} (type: {keyword.type})")
                logger.info(f"      Enhanced NL (first 150 chars): {keyword.enhanced_nl_description[:150]}...")
                logger.debug(f"      Full Enhanced NL: {keyword.enhanced_nl_description}")
                # Check if keyword is actually in original description
                keyword_lower = keyword.text.lower()
                nl_lower = nl_description.lower()
                if keyword_lower in nl_lower:
                    logger.warning(f"      WARNING: Keyword '{keyword.text}' appears to be in original description!")
                else:
                    logger.info(f"      Keyword '{keyword.text}' is NOT in original description (as expected)")
            
            response = SuggestionsResponse(
                keywords=result.keywords,
                extracted_items=result.extracted_items
            )
            
            logger.info("=" * 80)
            logger.info("SUGGESTION SERVICE: Analysis complete")
            logger.info(f"Returning {len(response.keywords)} keywords and extracted items")
            logger.info("=" * 80)
            
            return response
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("SUGGESTION SERVICE: ERROR occurred")
            logger.error("=" * 80)
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception message: {str(e)}")
            logger.exception("Full traceback:")
            
            # Fallback to empty response on error
            logger.warning("Returning empty response as fallback")
            return SuggestionsResponse(
                keywords=[],
                extracted_items=ExtractedItems(
                    domain=None,
                    entities=[],
                    cardinalities=[],
                    column_names=[],
                    constraints=[],
                    relationships=[]
                )
            )
