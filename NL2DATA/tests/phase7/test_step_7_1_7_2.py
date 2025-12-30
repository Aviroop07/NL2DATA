"""Unit tests for Step 7.1 and 7.2 with tool-based strategy selection.

These tests use mocks to avoid actual LLM calls.
"""

import sys
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.step_7_1_numerical_range_definition import step_7_1_numerical_range_definition
from NL2DATA.phases.phase7.step_7_2_text_generation_strategy import step_7_2_text_generation_strategy
from NL2DATA.phases.phase7.tools.strategy_selection import select_strategy_via_tool


class TestStep71:
    """Test Step 7.1: Numerical Range Definition."""
    
    @pytest.mark.asyncio
    async def test_step_7_1_with_mock_tool_call(self):
        """Test step_7_1 with mocked tool selection."""
        # Mock the select_strategy_via_tool function
        mock_strategy_result = {
            "tool_name": "generate_normal",
            "strategy_name": "normal",
            "kind": "distribution",
            "parameters": {"mu": 50.0, "sigma": 10.0, "min": 0.0, "max": 100.0},
            "reasoning": "Selected normal distribution for age attribute",
        }
        
        with patch("NL2DATA.phases.phase7.step_7_1_numerical_range_definition.select_strategy_via_tool", new_callable=AsyncMock) as mock_select:
            mock_select.return_value = mock_strategy_result
            
            # Mock model
            mock_model = MagicMock()
            with patch("NL2DATA.phases.phase7.step_7_1_numerical_range_definition.get_model_for_step", return_value=mock_model):
                result = await step_7_1_numerical_range_definition(
                    attribute_name="age",
                    attribute_description="Customer age",
                    attribute_type="INTEGER",
                    entity_name="Customer",
                    constraints=None,
                    relations=None,
                    entity_cardinality="medium",
                    nl_description=None,
                )
                
                assert result["distribution_type"] == "normal"
                assert result["min"] == 0.0
                assert result["max"] == 100.0
                assert result["parameters"]["mu"] == 50.0
                assert result["parameters"]["sigma"] == 10.0
                assert "reasoning" in result
    
    @pytest.mark.asyncio
    async def test_step_7_1_with_uniform_distribution(self):
        """Test step_7_1 with uniform distribution."""
        mock_strategy_result = {
            "tool_name": "generate_uniform",
            "strategy_name": "uniform",
            "kind": "distribution",
            "parameters": {"min": 1.0, "max": 100.0},
            "reasoning": "Selected uniform distribution",
        }
        
        with patch("NL2DATA.phases.phase7.step_7_1_numerical_range_definition.select_strategy_via_tool", new_callable=AsyncMock) as mock_select:
            mock_select.return_value = mock_strategy_result
            
            mock_model = MagicMock()
            with patch("NL2DATA.phases.phase7.step_7_1_numerical_range_definition.get_model_for_step", return_value=mock_model):
                result = await step_7_1_numerical_range_definition(
                    attribute_name="quantity",
                    attribute_description="Order quantity",
                    attribute_type="INTEGER",
                    entity_name="Order",
                )
                
                assert result["distribution_type"] == "uniform"
                assert result["min"] == 1.0
                assert result["max"] == 100.0


class TestStep72:
    """Test Step 7.2: Text Generation Strategy."""
    
    @pytest.mark.asyncio
    async def test_step_7_2_with_faker_name(self):
        """Test step_7_2 with faker name tool."""
        mock_strategy_result = {
            "tool_name": "generate_faker_name",
            "strategy_name": "faker_name",
            "kind": "string",
            "parameters": {"locale": "en_US", "name_type": "full"},
            "reasoning": "Selected faker name for customer name",
        }
        
        with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.select_strategy_via_tool", new_callable=AsyncMock) as mock_select:
            mock_select.return_value = mock_strategy_result
            
            mock_model = MagicMock()
            with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.get_model_for_step", return_value=mock_model):
                result = await step_7_2_text_generation_strategy(
                    attribute_name="name",
                    attribute_description="Customer name",
                    attribute_type="VARCHAR(255)",
                    entity_name="Customer",
                    generator_catalog=None,
                    domain="e-commerce",
                )
                
                assert result["generator_type"] == "faker.name"  # Converted from faker_name
                assert result["parameters"]["locale"] == "en_US"
                assert result["parameters"]["name_type"] == "full"
                assert result["not_possible"] == False
    
    @pytest.mark.asyncio
    async def test_step_7_2_with_faker_email(self):
        """Test step_7_2 with faker email tool."""
        mock_strategy_result = {
            "tool_name": "generate_faker_email",
            "strategy_name": "faker_email",
            "kind": "string",
            "parameters": {"locale": "en_US"},
            "reasoning": "Selected faker email for email attribute",
        }
        
        with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.select_strategy_via_tool", new_callable=AsyncMock) as mock_select:
            mock_select.return_value = mock_strategy_result
            
            mock_model = MagicMock()
            with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.get_model_for_step", return_value=mock_model):
                result = await step_7_2_text_generation_strategy(
                    attribute_name="email",
                    attribute_description="Customer email",
                    attribute_type="VARCHAR(255)",
                    entity_name="Customer",
                )
                
                assert result["generator_type"] == "faker.email"
                assert result["parameters"]["locale"] == "en_US"
    
    @pytest.mark.asyncio
    async def test_step_7_2_with_regex(self):
        """Test step_7_2 with regex tool."""
        mock_strategy_result = {
            "tool_name": "generate_regex",
            "strategy_name": "regex",
            "kind": "string",
            "parameters": {"pattern": r"[A-Z]{3}[0-9]{3}", "unique": True},
            "reasoning": "Selected regex for SKU code",
        }
        
        with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.select_strategy_via_tool", new_callable=AsyncMock) as mock_select:
            mock_select.return_value = mock_strategy_result
            
            mock_model = MagicMock()
            with patch("NL2DATA.phases.phase7.step_7_2_text_generation_strategy.get_model_for_step", return_value=mock_model):
                result = await step_7_2_text_generation_strategy(
                    attribute_name="sku",
                    attribute_description="Product SKU code",
                    attribute_type="VARCHAR(10)",
                    entity_name="Product",
                )
                
                assert result["generator_type"] == "regex"
                assert result["parameters"]["pattern"] == r"[A-Z]{3}[0-9]{3}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

