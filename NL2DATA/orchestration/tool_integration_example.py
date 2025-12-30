"""Example of integrating validation tools into a step.

This demonstrates how to use LangChain tools with bind_tools() to enable
LLM self-validation in steps.
"""

from typing import Dict, Any, List
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from NL2DATA.utils.tools import (
    check_entity_name_validity,
    validate_attributes_exist,
    check_entity_exists,
)
from NL2DATA.utils.llm.chain_utils import create_structured_chain
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


async def step_with_tools_example(
    nl_description: str,
    schema_state: Dict[str, Any],
    llm: ChatOpenAI
):
    """Example step that uses tools for self-validation.
    
    This demonstrates the pattern for integrating tools into steps:
    1. Define tools needed for this step
    2. Bind tools to LLM
    3. Create chain with tools + structured output
    4. LLM can call tools during reasoning
    
    Args:
        nl_description: Natural language description
        schema_state: Current schema state (for tool context)
        llm: ChatOpenAI model instance
        
    Returns:
        Step result
    """
    from pydantic import BaseModel, Field
    
    # Define output schema
    class StepOutput(BaseModel):
        entities: List[Dict[str, str]] = Field(description="List of entities")
        validated: bool = Field(description="Whether validation passed")
    
    # Define tools for this step
    # Tools need access to schema_state, so we create bound versions
    def check_entity_exists_bound(entity: str) -> bool:
        """Bound version of check_entity_exists with schema_state."""
        return check_entity_exists(entity, schema_state)
    
    def validate_attributes_exist_bound(entity: str, attributes: List[str]) -> Dict[str, bool]:
        """Bound version of validate_attributes_exist with schema_state."""
        return validate_attributes_exist(entity, attributes, schema_state)
    
    # Create tool list
    tools = [
        check_entity_name_validity,
        check_entity_exists_bound,
        validate_attributes_exist_bound,
    ]
    
    # Bind tools to LLM
    llm_with_tools = llm.bind_tools(tools)
    
    # Create prompt
    system_prompt = """You are a database design assistant. 
    
You have access to validation tools that you can use to check your work:
- check_entity_name_validity: Check if entity names are valid
- check_entity_exists: Check if entities exist in schema
- validate_attributes_exist: Validate attributes exist for entities

Use these tools to validate your responses before finalizing them."""
    
    human_prompt = "Extract entities from: {nl_description}"
    
    # Create chain with tools + structured output
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", human_prompt),
    ])
    
    # Apply structured output to tool-enabled LLM
    structured_llm = llm_with_tools.with_structured_output(StepOutput)
    chain = prompt | structured_llm
    
    # Invoke
    result = await chain.ainvoke({"nl_description": nl_description})
    
    return result


# Alternative: Using create_structured_chain with tools
async def step_with_tools_via_chain_utils(
    nl_description: str,
    schema_state: Dict[str, Any],
    llm: ChatOpenAI
):
    """Example using create_structured_chain with tools.
    
    This shows how to extend create_structured_chain to support tools.
    """
    from NL2DATA.utils.tools import check_entity_name_validity
    
    # For now, create_structured_chain doesn't support tools directly
    # We need to bind tools before calling it
    
    # Bind tools
    tools = [check_entity_name_validity]
    llm_with_tools = llm.bind_tools(tools)
    
    # Then use create_structured_chain (would need modification to accept llm_with_tools)
    # For now, this is a placeholder showing the pattern
    
    logger.info("Tool integration example - tools would be bound here")
    return None

