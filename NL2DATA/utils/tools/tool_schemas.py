"""Pydantic schemas for tool arguments.

These schemas ensure proper documentation and type validation for tool arguments,
helping LLMs understand the exact format required for tool calls.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class VerifyEntitiesExistArgs(BaseModel):
    """Arguments for verify_entities_exist tool.
    
    IMPORTANT: When calling this tool, provide arguments as a JSON object:
    {"entities": ["Customer", "Order"]}
    NOT as a list: ["entities"] (WRONG)
    """
    entities: List[str] = Field(
        ...,
        description="List of entity names to verify. Must be a list of strings, e.g., ['Customer', 'Order']. "
                   "When calling this tool, provide as a JSON object: {'entities': ['Customer', 'Order']}",
        examples=[["Customer", "Order"], ["Book", "Author", "Publisher"]]
    )
    schema_state: Dict[str, Any] = Field(
        ...,
        description="Current schema state dictionary (internal use, may be bound)",
    )


class ValidateQueryAgainstSchemaArgs(BaseModel):
    """Arguments for validate_query_against_schema tool."""
    sql: str = Field(
        ...,
        description="SQL query string to validate against the schema. Must be a valid SQL query string.",
        examples=["SELECT * FROM Customer;", "SELECT order_id, total_amount FROM \"Order\" WHERE customer_id = 1;"]
    )


class ValidateSQLSyntaxArgs(BaseModel):
    """Arguments for validate_sql_syntax tool."""
    sql: str = Field(
        ...,
        description="SQL query string to validate for syntax errors. Must be a valid SQL query string.",
        examples=["SELECT * FROM Customer;", "SELECT COUNT(*) FROM \"Order\" WHERE status = 'completed';"]
    )


class CheckEntityExistsArgs(BaseModel):
    """Arguments for check_entity_exists tool."""
    entity: str = Field(
        ...,
        description="Name of the entity to check. Must be a single string, e.g., 'Customer'",
        examples=["Customer", "Order", "Book"]
    )


class ValidateAttributesExistArgs(BaseModel):
    """Arguments for validate_attributes_exist tool."""
    entity: str = Field(
        ...,
        description="Name of the entity to check. Must be a single string.",
        examples=["Customer", "Order"]
    )
    attributes: List[str] = Field(
        ...,
        description="List of attribute names to verify. Must be a list of strings.",
        examples=[["name", "email"], ["order_id", "order_date", "total_amount"]]
    )


class CheckSchemaComponentExistsArgs(BaseModel):
    """Arguments for check_schema_component_exists tool."""
    component_type: str = Field(
        ...,
        description="Type of component to check (e.g., 'table', 'column', 'entity', 'attribute'). Must be a single string.",
        examples=["table", "column", "entity", "attribute"]
    )
    name: str = Field(
        ...,
        description="Name of the component to check. Must be a single string, e.g., 'Customer' for a table or 'customer_id' for a column.",
        examples=["Customer", "Order", "customer_id", "order_date"]
    )


class CheckSchemaComponentExistsBoundArgs(BaseModel):
    """Arguments for check_schema_component_exists_bound tool (schema_state is already bound)."""
    component_type: str = Field(
        ...,
        description="Type of component to check (e.g., 'table', 'column', 'entity', 'attribute'). Must be a single string. "
                   "When calling this tool, provide as a JSON object: {'component_type': 'table', 'name': 'Customer'}",
        examples=["table", "column", "entity", "attribute"]
    )
    name: str = Field(
        ...,
        description="Name of the component to check. Must be a single string, e.g., 'Customer' for a table or 'customer_id' for a column. "
                   "When calling this tool, provide as a JSON object: {'component_type': 'table', 'name': 'Customer'}",
        examples=["Customer", "Order", "customer_id", "order_date"]
    )


class ValidateDSLExpressionArgs(BaseModel):
    """Arguments for validate_dsl_expression tool."""
    dsl: str = Field(
        ...,
        description="DSL expression to validate. Must be a valid DSL expression string. "
                   "When calling this tool, provide as a JSON object: {'dsl': 'amount > 0'}",
        examples=["amount > 0", "flag ~ Bernoulli(0.05)", "amount ~ LogNormal(3.5, 1.2)"]
    )


class ValidateDSLExpressionBoundArgs(BaseModel):
    """Arguments for validate_dsl_expression_bound tool (grammar is already bound)."""
    dsl: str = Field(
        ...,
        description="DSL expression to validate. Must be a valid DSL expression string. "
                   "When calling this tool, provide as a JSON object: {'dsl': 'amount > 0'}",
        examples=["amount > 0", "flag ~ Bernoulli(0.05)", "amount ~ LogNormal(3.5, 1.2)"]
    )


# Map tool names to their argument schemas
# Note: For bound functions, we create a simplified schema without the bound parameter
class VerifyEntitiesExistBoundArgs(BaseModel):
    """Arguments for verify_entities_exist_bound tool (schema_state is already bound)."""
    entities: List[str] = Field(
        ...,
        description="List of entity names to verify. Must be a list of strings, e.g., ['Customer', 'Order']. "
                   "When calling this tool, provide as a JSON object: {'entities': ['Customer', 'Order']}",
        examples=[["Customer", "Order"], ["Book", "Author", "Publisher"]]
    )


class CheckEntityConnectivityBoundArgs(BaseModel):
    """Arguments for check_entity_connectivity_bound tool (relations/schema_state are already bound)."""
    entity: str = Field(
        ...,
        description="Entity name to check connectivity for. Provide as JSON: {'entity': 'Customer'}",
        examples=["Customer", "Order", "Book"],
    )


class ValidateAttributesExistBoundArgs(BaseModel):
    """Arguments for validate_attributes_exist_bound tool (schema_state is already bound)."""
    entity: str = Field(
        ...,
        description="Entity name to check. Provide as JSON: {'entity': 'Customer', 'attributes': ['name']}",
        examples=["Customer", "Order"],
    )
    attributes: List[str] = Field(
        ...,
        description="List of attribute names to verify. Provide as JSON: {'entity': 'Customer', 'attributes': ['name','email']}",
        examples=[["name", "email"], ["order_id", "order_date"]],
    )


class DetectCircularDependenciesBoundArgs(BaseModel):
    """Arguments for detect_circular_dependencies_bound tool (schema_state is already bound)."""
    relations_list: List[Dict[str, Any]] = Field(
        ...,
        description="List of relation dicts to analyze. Provide as JSON: {'relations_list': [...]}",
    )


class ValidateCardinalityConsistencyBoundArgs(BaseModel):
    """Arguments for validate_cardinality_consistency_bound tool (schema_state is already bound)."""
    relation: Dict[str, Any] = Field(
        ...,
        description="Single relation dict to validate. Provide as JSON: {'relation': {...}}",
    )

TOOL_ARG_SCHEMAS = {
    "verify_entities_exist": VerifyEntitiesExistArgs,
    "verify_entities_exist_bound": VerifyEntitiesExistBoundArgs,
    "validate_query_against_schema": ValidateQueryAgainstSchemaArgs,
    "validate_query_against_schema_bound": ValidateQueryAgainstSchemaArgs,
    "validate_sql_syntax": ValidateSQLSyntaxArgs,
    "check_entity_exists": CheckEntityExistsArgs,
    "validate_attributes_exist": ValidateAttributesExistArgs,
    "check_schema_component_exists": CheckSchemaComponentExistsArgs,
    "check_schema_component_exists_bound": CheckSchemaComponentExistsBoundArgs,
    "validate_dsl_expression": ValidateDSLExpressionArgs,
    "validate_dsl_expression_bound": ValidateDSLExpressionBoundArgs,
    "check_entity_connectivity_bound": CheckEntityConnectivityBoundArgs,
    "validate_attributes_exist_bound": ValidateAttributesExistBoundArgs,
    "detect_circular_dependencies_bound": DetectCircularDependenciesBoundArgs,
    "validate_cardinality_consistency_bound": ValidateCardinalityConsistencyBoundArgs,
}

