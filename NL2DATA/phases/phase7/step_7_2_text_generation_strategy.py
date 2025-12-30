"""Phase 7, Step 7.2: Text Generation Strategy.

Select appropriate text generator (faker, mimesis, regex) for text attributes using tool-based strategy selection.
"""

from typing import Dict, Any, List, Optional

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.phases.phase7.tools.strategy_selection import select_strategy_via_tool
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


@traceable_step("7.2", phase=7, tags=['phase_7_step_2'])
async def step_7_2_text_generation_strategy(
    attribute_name: str,
    attribute_description: Optional[str],
    attribute_type: str,  # SQL type from Step 4.3
    entity_name: str,
    generator_catalog: Optional[List[Dict[str, Any]]] = None,  # Available generators (external context)
    domain: Optional[str] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
    constraints: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Step 7.2 (per-text attribute, LLM): Select text generation strategy.
    
    Args:
        attribute_name: Name of the text attribute
        attribute_description: Optional description
        attribute_type: SQL type (e.g., "VARCHAR(255)")
        entity_name: Name of the entity/table
        generator_catalog: Optional catalog of available generators
        domain: Optional domain context
        relations: Optional relations
        constraints: Optional constraints
        
    Returns:
        dict: Generation strategy with generator_type, parameters, fallback, reasoning
    """
    logger.debug(f"Selecting text generator for {entity_name}.{attribute_name}")
    
    # Get model
    model = get_model_for_step("7.2")
    
    # Create prompt for tool-based strategy selection
    system_prompt = """You are a data generation expert. Your task is to select an appropriate text generation strategy for text attributes.

You have access to generation tools that can create different types of text:
- **generate_faker_name**: Person names (full, first, last)
- **generate_faker_email**: Email addresses
- **generate_faker_address**: Addresses (full, street, city, state, zipcode, country)
- **generate_faker_company**: Company names
- **generate_faker_text**: Random text (word, sentence, paragraph, text)
- **generate_faker_url**: URLs (full URL, domain, URI path)
- **generate_faker_phone**: Phone numbers
- **generate_mimesis_name**: Person names (international locale support)
- **generate_mimesis_email**: Email addresses (international locale support)
- **generate_mimesis_text**: Text content (international locale support)
- **generate_mimesis_address**: Addresses (international locale support)
- **generate_mimesis_coordinates**: Geographic coordinates (lat/lon)
- **generate_mimesis_country**: Country names or codes
- **generate_regex**: Pattern-based generation (for specific formats like SKU codes, license plates)

SELECTION RULES:
- Match generator to attribute purpose (name → generate_faker_name, email → generate_faker_email)
- Consider domain context (business → generate_faker_company, personal → generate_faker_name)
- Use mimesis tools for international data or non-English locales
- Use generate_regex for attributes with specific format requirements
- Choose appropriate parameters (locale, component type, etc.)

Call the appropriate tool with parameters that match the attribute's requirements."""
    
    catalog_context = ""
    if generator_catalog:
        catalog_context = "\n\nAdditional Context - Available Generators:\n" + "\n".join(
            f"- {g.get('name', '')}: {g.get('description', '')}" for g in generator_catalog[:20]
        )
    else:
        catalog_context = ""
    
    # Format human prompt (not using template since we have all values)
    human_prompt = f"""Attribute: {entity_name}.{attribute_name}
Type: {attribute_type}
Description: {attribute_description or 'No description'}
Domain: {domain or 'Unknown'}
{catalog_context}

Select an appropriate text generation strategy for this attribute by calling one of the available tools."""
    
    # Use tool-based strategy selection
    try:
        strategy_result = await select_strategy_via_tool(
            llm=model,
            attribute_name=attribute_name,
            attribute_type=attribute_type,
            attribute_description=attribute_description,
            entity_name=entity_name,
            is_categorical=False,
            is_boolean=False,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,  # Pass formatted string, not template
        )
        
        # Map strategy name to generator_type format (for backward compatibility)
        strategy_name = strategy_result.get("strategy_name", "faker_text")
        parameters = strategy_result.get("parameters", {})
        
        # Convert strategy name to generator_type format
        generator_type = strategy_name.replace("_", ".")  # e.g., "faker_name" -> "faker.name"
        
        return {
            "generator_type": generator_type,
            "parameters": parameters,
            "fallback": None,  # Tool-based selection doesn't use fallback
            "not_possible": False,  # If tool was called, generation is possible
            "reasoning": strategy_result.get("reasoning", "Strategy selected via tool call")
        }
    except Exception as e:
        logger.error(f"Text generation strategy failed: {e}")
        raise


async def step_7_2_text_generation_strategy_batch(
    text_attributes: List[Dict[str, Any]],  # List of text attributes
    generator_catalog: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Select generators for multiple text attributes in parallel."""
    import asyncio
    
    tasks = [
        step_7_2_text_generation_strategy(
            attribute_name=attr.get("attribute_name", ""),
            attribute_description=attr.get("attribute_description"),
            attribute_type=attr.get("attribute_type", "VARCHAR(255)"),
            entity_name=attr.get("entity_name", ""),
            generator_catalog=generator_catalog,
            domain=attr.get("domain"),
            relations=attr.get("relations"),
            constraints=attr.get("constraints"),
        )
        for attr in text_attributes
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    output = {}
    for attr, result in zip(text_attributes, results):
        key = f"{attr.get('entity_name', '')}.{attr.get('attribute_name', '')}"
        if isinstance(result, Exception):
            logger.error(f"Text generation strategy failed for {key}: {result}")
            output[key] = {
                "generator_type": "faker.text",
                "parameters": {},
                "fallback": None,
                "not_possible": False,
                "reasoning": f"Generation failed: {str(result)}"
            }
        else:
            output[key] = result
    
    return output

