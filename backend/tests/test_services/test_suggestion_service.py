"""Tests for SuggestionService."""

import pytest
from backend.services.suggestion_service import SuggestionService


@pytest.mark.asyncio
async def test_analyze_and_suggest():
    """Test analyzing NL description and generating suggestions."""
    service = SuggestionService()
    nl_description = "I need a database with customers and orders."
    
    result = await service.analyze_and_suggest(nl_description)
    
    assert result is not None
    assert hasattr(result, "keywords")
    assert hasattr(result, "extracted_items")
    assert isinstance(result.keywords, list)
    assert result.extracted_items is not None


@pytest.mark.asyncio
async def test_analyze_and_suggest_structure():
    """Test suggestions response structure."""
    service = SuggestionService()
    nl_description = "E-commerce system with products and customers."
    
    result = await service.analyze_and_suggest(nl_description)
    
    # Check keywords structure
    if len(result.keywords) > 0:
        keyword = result.keywords[0]
        assert hasattr(keyword, "text")
        assert hasattr(keyword, "type")
        assert hasattr(keyword, "enhanced_nl_description")
        assert keyword.type in ["domain", "entity", "constraint", "attribute", "relationship", "distribution"]
    
    # Check extracted_items structure
    assert hasattr(result.extracted_items, "domain")
    assert hasattr(result.extracted_items, "entities")
    assert hasattr(result.extracted_items, "cardinalities")
    assert hasattr(result.extracted_items, "column_names")
    assert hasattr(result.extracted_items, "constraints")
    assert hasattr(result.extracted_items, "relationships")



