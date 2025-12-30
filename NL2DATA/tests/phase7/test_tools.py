"""Unit tests for tool catalog and tool-to-strategy mapping."""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.tools.catalog import GENERATION_TOOL_CATALOG, GenerationToolDefinition
from NL2DATA.phases.phase7.tools.mapping import (
    TOOL_TO_STRATEGY_MAP,
    create_strategy_from_tool_call,
    get_tools_for_column,
    get_allowed_strategy_kinds_for_column,
)
from NL2DATA.phases.phase7.tools.langchain_tools import (
    create_generation_tool_from_definition,
    get_langchain_tools_for_column,
)


class TestToolCatalog:
    """Test tool catalog definitions."""
    
    def test_catalog_not_empty(self):
        """Test that catalog has tools."""
        assert len(GENERATION_TOOL_CATALOG) > 0
    
    def test_catalog_structure(self):
        """Test that all catalog entries have required fields."""
        for tool_name, tool_def in GENERATION_TOOL_CATALOG.items():
            assert isinstance(tool_def, GenerationToolDefinition)
            assert tool_def.name == tool_name
            assert tool_def.kind in ["distribution", "string", "location", "datetime"]
            assert len(tool_def.description) > 0
            assert len(tool_def.parameters) > 0
            assert len(tool_def.returns) > 0
    
    def test_catalog_has_distributions(self):
        """Test that catalog includes distribution tools."""
        distribution_tools = [
            "generate_normal",
            "generate_lognormal",
            "generate_uniform",
            "generate_pareto",
            "generate_zipf",
            "generate_exponential",
            "generate_categorical",
            "generate_bernoulli",
        ]
        for tool_name in distribution_tools:
            assert tool_name in GENERATION_TOOL_CATALOG
            assert GENERATION_TOOL_CATALOG[tool_name].kind == "distribution"
    
    def test_catalog_has_faker_tools(self):
        """Test that catalog includes Faker tools."""
        faker_tools = [
            "generate_faker_name",
            "generate_faker_email",
            "generate_faker_address",
            "generate_faker_company",
            "generate_faker_text",
            "generate_faker_url",
            "generate_faker_phone",
        ]
        for tool_name in faker_tools:
            assert tool_name in GENERATION_TOOL_CATALOG
            assert GENERATION_TOOL_CATALOG[tool_name].kind == "string"
    
    def test_catalog_has_mimesis_tools(self):
        """Test that catalog includes Mimesis tools."""
        mimesis_tools = [
            "generate_mimesis_name",
            "generate_mimesis_email",
            "generate_mimesis_text",
            "generate_mimesis_address",
            "generate_mimesis_coordinates",
            "generate_mimesis_country",
        ]
        for tool_name in mimesis_tools:
            assert tool_name in GENERATION_TOOL_CATALOG
            assert GENERATION_TOOL_CATALOG[tool_name].kind in ["string", "location"]
    
    def test_catalog_has_regex_tool(self):
        """Test that catalog includes regex tool."""
        assert "generate_regex" in GENERATION_TOOL_CATALOG
        assert GENERATION_TOOL_CATALOG["generate_regex"].kind == "string"


class TestToolToStrategyMapping:
    """Test tool-to-strategy mapping."""
    
    def test_mapping_completeness(self):
        """Test that all catalog tools have strategy mappings."""
        for tool_name in GENERATION_TOOL_CATALOG.keys():
            assert tool_name in TOOL_TO_STRATEGY_MAP, f"Missing mapping for {tool_name}"
    
    def test_create_strategy_from_tool_call_normal(self):
        """Test creating NormalDistribution from tool call."""
        tool_args = {"mu": 50.0, "sigma": 10.0, "min": 0.0, "max": 100.0}
        strategy = create_strategy_from_tool_call("generate_normal", tool_args)
        assert strategy.name == "normal"
        assert strategy.mu == 50.0
        assert strategy.sigma == 10.0
    
    def test_create_strategy_from_tool_call_uniform(self):
        """Test creating UniformDistribution from tool call."""
        tool_args = {"min": 10.0, "max": 20.0}
        strategy = create_strategy_from_tool_call("generate_uniform", tool_args)
        assert strategy.name == "uniform"
        assert strategy.min == 10.0
        assert strategy.max == 20.0
    
    def test_create_strategy_from_tool_call_faker_name(self):
        """Test creating FakerNameStrategy from tool call."""
        tool_args = {"locale": "en_US", "name_type": "full"}
        strategy = create_strategy_from_tool_call("generate_faker_name", tool_args)
        assert strategy.name == "faker_name"
        assert strategy.locale == "en_US"
        assert strategy.name_type == "full"
    
    def test_create_strategy_from_tool_call_lambda_alias(self):
        """Test that lambda parameter is handled correctly."""
        # Pydantic uses alias="lambda", so "lambda" is the correct input key
        tool_args = {"lambda": 0.5, "min": 0.0}
        strategy = create_strategy_from_tool_call("generate_exponential", tool_args)
        assert strategy.name == "exponential"
        assert strategy.lambda_ == 0.5
        
        # Also test with lambda_ (should be converted to lambda)
        tool_args2 = {"lambda_": 0.5, "min": 0.0}
        strategy2 = create_strategy_from_tool_call("generate_exponential", tool_args2)
        assert strategy2.lambda_ == 0.5
    
    def test_create_strategy_invalid_tool(self):
        """Test that invalid tool name raises error."""
        with pytest.raises(ValueError):
            create_strategy_from_tool_call("invalid_tool", {})
    
    def test_create_strategy_invalid_args(self):
        """Test that invalid args raise error."""
        with pytest.raises(ValueError):
            create_strategy_from_tool_call("generate_normal", {"mu": "invalid"})


class TestColumnTypeFiltering:
    """Test column type-based tool filtering."""
    
    def test_get_allowed_kinds_boolean(self):
        """Test that boolean columns only allow distribution."""
        kinds = get_allowed_strategy_kinds_for_column("BOOLEAN", is_boolean=True)
        assert kinds == ["distribution"]
    
    def test_get_allowed_kinds_integer(self):
        """Test that integer columns allow distribution."""
        kinds = get_allowed_strategy_kinds_for_column("INTEGER")
        assert "distribution" in kinds
    
    def test_get_allowed_kinds_varchar(self):
        """Test that varchar columns allow string."""
        kinds = get_allowed_strategy_kinds_for_column("VARCHAR(255)")
        assert "string" in kinds
    
    def test_get_allowed_kinds_categorical_varchar(self):
        """Test that categorical varchar allows both distribution and string."""
        kinds = get_allowed_strategy_kinds_for_column("VARCHAR(255)", is_categorical=True)
        assert "distribution" in kinds
        assert "string" in kinds
    
    def test_get_tools_for_column_boolean(self):
        """Test that boolean columns get bernoulli tool."""
        tools = get_tools_for_column("BOOLEAN", is_boolean=True)
        assert "generate_bernoulli" in tools
    
    def test_get_tools_for_column_integer(self):
        """Test that integer columns get distribution tools."""
        tools = get_tools_for_column("INTEGER")
        assert "generate_uniform" in tools
        assert "generate_normal" in tools
        assert "generate_faker_name" not in tools  # Should not include string tools
    
    def test_get_tools_for_column_varchar(self):
        """Test that varchar columns get string tools."""
        tools = get_tools_for_column("VARCHAR(255)")
        assert "generate_faker_name" in tools
        assert "generate_faker_email" in tools
        assert "generate_uniform" not in tools  # Should not include distribution tools (unless categorical)


class TestLangChainTools:
    """Test LangChain tool creation."""
    
    def test_create_tool_from_definition(self):
        """Test creating LangChain tool from definition."""
        tool_def = GENERATION_TOOL_CATALOG["generate_uniform"]
        tool = create_generation_tool_from_definition(tool_def)
        assert tool.name == "generate_uniform"
        assert tool.description == tool_def.description
    
    def test_get_langchain_tools_for_column(self):
        """Test getting LangChain tools for a column."""
        tools = get_langchain_tools_for_column("INTEGER")
        assert len(tools) > 0
        assert all(hasattr(t, "name") for t in tools)
        assert all(hasattr(t, "description") for t in tools)
    
    def test_get_langchain_tools_boolean(self):
        """Test getting LangChain tools for boolean column."""
        tools = get_langchain_tools_for_column("BOOLEAN", is_boolean=True)
        assert len(tools) > 0
        tool_names = [t.name for t in tools]
        assert "generate_bernoulli" in tool_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

