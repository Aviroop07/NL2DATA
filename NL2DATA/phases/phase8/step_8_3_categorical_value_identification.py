"""Phase 8, Step 8.3: Categorical Value Identification.

Identify explicit categorical values for each categorical column.
This is a high fanout step - one LLM call per categorical column.
Includes refinement loop with deterministic validation.

Note: Boolean columns are automatically filtered out - they are not processed.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
import json
import asyncio

from NL2DATA.phases.phase8.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.utils.pipeline_config import get_phase4_config

logger = get_logger(__name__)


class CategoricalValue(BaseModel):
    """A single categorical value."""
    value: str = Field(description="The categorical value (must match column datatype)")
    description: Optional[str] = Field(
        default=None,
        description="Optional description of what this value represents"
    )

    model_config = ConfigDict(extra="forbid")


class CategoricalValueIdentificationOutput(BaseModel):
    """Output structure for categorical value identification."""
    categorical_values: List[CategoricalValue] = Field(
        default_factory=list,
        description="List of categorical values for this column"
    )
    reasoning: str = Field(
        description="REQUIRED - Explanation of why these values were chosen"
    )
    no_more_changes: bool = Field(
        default=True,
        description="Whether the LLM is satisfied with these values"
    )

    model_config = ConfigDict(extra="forbid")


def _detect_categorical_value_issues(
    entity_name: str,
    attribute_name: str,
    column_datatype: str,
    categorical_values: List[CategoricalValue],
) -> List[str]:
    """
    Deterministically detect issues with categorical values.
    
    Args:
        entity_name: Entity name
        attribute_name: Attribute name
        column_datatype: SQL datatype of the column (e.g., "VARCHAR(50)", "INT")
        categorical_values: List of categorical values
        
    Returns:
        List of issue descriptions
    """
    issues = []
    
    if not categorical_values:
        issues.append("No categorical values provided. At least 2 values are required.")
        return issues
    
    if len(categorical_values) < 2:
        issues.append(f"Only {len(categorical_values)} value(s) provided. At least 2 distinct values are required for a categorical column.")
    
    # Check for duplicate values
    value_strings = [cv.value for cv in categorical_values]
    seen = set()
    duplicates = []
    for val in value_strings:
        if val in seen:
            duplicates.append(val)
        seen.add(val)
    
    if duplicates:
        issues.append(f"Duplicate values found: {', '.join(duplicates)}. All values must be unique.")
    
    # Check datatype matching
    # Extract base type (before parentheses)
    base_type = column_datatype.upper().strip().split("(")[0].strip()
    
    # For numeric types, check if values are numeric
    if base_type in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE"):
        non_numeric = []
        for cv in categorical_values:
            try:
                if base_type in ("FLOAT", "REAL", "DOUBLE"):
                    float(cv.value)
                else:
                    int(cv.value)
            except (ValueError, TypeError):
                non_numeric.append(cv.value)
        
        if non_numeric:
            issues.append(
                f"Column datatype is {base_type} (numeric), but the following values are not numeric: {', '.join(non_numeric)}. "
                f"All values must be valid {base_type} values."
            )
    
    # For string types, check length constraints if specified
    elif base_type in ("VARCHAR", "CHAR", "TEXT", "STRING"):
        # Extract size from VARCHAR(50) or CHAR(10)
        size = None
        if "(" in column_datatype and ")" in column_datatype:
            try:
                size_str = column_datatype.split("(")[1].split(")")[0].split(",")[0]
                size = int(size_str)
            except (ValueError, IndexError):
                pass
        
        if size is not None:
            too_long = []
            for cv in categorical_values:
                if len(cv.value) > size:
                    too_long.append(f"{cv.value} (length {len(cv.value)} > {size})")
            
            if too_long:
                issues.append(
                    f"Column datatype is {column_datatype} (max length {size}), but the following values exceed this length: {', '.join(too_long)}. "
                    f"All values must fit within the column's size constraint."
                )
    
    # For boolean types
    elif base_type in ("BOOLEAN", "BOOL"):
        invalid_bool = []
        valid_bools = {"true", "false", "1", "0", "yes", "no", "y", "n"}
        for cv in categorical_values:
            if cv.value.lower() not in valid_bools:
                invalid_bool.append(cv.value)
        
        if invalid_bool:
            issues.append(
                f"Column datatype is {base_type}, but the following values are not valid boolean values: {', '.join(invalid_bool)}. "
                f"Valid boolean values include: true, false, 1, 0, yes, no, y, n"
            )
    
    return issues


@traceable_step("8.3", phase=8, tags=['phase_8_step_3'])
async def step_8_3_categorical_value_identification_single(
    entity_name: str,
    attribute_name: str,
    attribute_description: Optional[str] = None,
    column_datatype: str = "VARCHAR(255)",
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
    nl_description: Optional[str] = None,
) -> CategoricalValueIdentificationOutput:
    """
    Step 8.3 (per-categorical-column): Identify explicit categorical values for a single categorical column.
    
    This is a high fanout step - called once per categorical column.
    Includes refinement loop with deterministic validation.
    
    Note: Boolean columns are automatically filtered out and not processed.
    
    Args:
        entity_name: Name of the entity
        attribute_name: Name of the categorical attribute
        attribute_description: Optional description of the attribute
        column_datatype: SQL datatype of the column (e.g., "VARCHAR(50)", "INT")
        entity_description: Optional description of the entity
        domain: Optional domain context
        nl_description: Optional natural language description
        
    Returns:
        CategoricalValueIdentificationOutput with categorical values
    """
    # Filter out boolean columns deterministically
    base_type = column_datatype.upper().strip().split("(")[0].strip()
    if base_type in ("BOOLEAN", "BOOL"):
        logger.debug(f"Skipping {entity_name}.{attribute_name} - boolean columns are not processed for categorical values")
        return CategoricalValueIdentificationOutput(
            categorical_values=[],
            reasoning="Boolean columns are not processed for categorical value identification",
            no_more_changes=True
        )
    
    logger.info(f"Identifying categorical values for {entity_name}.{attribute_name}")
    
    # Build context
    context_parts = []
    if entity_description:
        context_parts.append(f"Entity: {entity_name} - {entity_description}")
    else:
        context_parts.append(f"Entity: {entity_name}")
    
    if attribute_description:
        context_parts.append(f"Attribute: {attribute_name} - {attribute_description}")
    else:
        context_parts.append(f"Attribute: {attribute_name}")
    
    context_parts.append(f"Column datatype: {column_datatype}")
    
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
        # Escape braces to prevent format string errors
        context_msg = context_msg.replace("{", "{{").replace("}", "}}")
    
    # Generate output structure section
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=CategoricalValueIdentificationOutput,
        additional_requirements=[
            "The 'categorical_values' list must contain at least 2 distinct values",
            "Each value in 'categorical_values' must match the column's datatype",
            "All values must be unique (no duplicates)",
            "The 'reasoning' field is REQUIRED and cannot be empty",
            "If 'no_more_changes' is false, the LLM wants to modify the values in the next iteration"
        ]
    )
    
    # System prompt
    system_prompt = f"""You are a database schema analyst. Your task is to identify explicit categorical values for a categorical column.

A categorical column has a limited, discrete set of possible values. Your job is to identify what those specific values are.

**CRITICAL REQUIREMENTS**:
1. Provide at least 2 distinct categorical values
2. All values MUST match the column's datatype ({column_datatype})
3. All values must be unique (no duplicates)
4. Values should be realistic and appropriate for the domain
5. Consider common values that would appear in real-world data

**DATATYPE MATCHING**:
- If the column is VARCHAR/CHAR/TEXT: Provide string values
- If the column is INT/INTEGER: Provide integer values (as strings, e.g., "1", "2", "3")
- If the column is DECIMAL/NUMERIC/FLOAT: Provide numeric values (as strings, e.g., "1.5", "2.0")
- If the column is BOOLEAN: Provide boolean values (e.g., "true", "false", "1", "0")
- Respect size constraints (e.g., VARCHAR(50) means max 50 characters)

**EXAMPLES**:
- Status column (VARCHAR): ["pending", "completed", "cancelled", "failed"]
- Priority column (INT): ["1", "2", "3", "4", "5"]
- Severity column (VARCHAR): ["low", "medium", "high", "critical"]
- Category column (VARCHAR): ["electronics", "clothing", "food", "books"]

For each value, provide:
- value: The actual categorical value (must match datatype)
- description: Optional description of what this value represents

{output_structure_section}"""
    
    # Human prompt
    human_prompt = f"""Identify categorical values for the following column:

Entity: {entity_name}
Attribute: {attribute_name}
Column Datatype: {column_datatype}
{context_msg}

Natural language description:
{{nl_description}}

Provide a comprehensive list of categorical values that this column can take."""
    
    # Get model
    llm = get_model_for_step("8.3")  # High fanout step
    
    # Use phase4 config (same pattern as step_8_1)
    cfg = get_phase4_config()
    max_revision_rounds = getattr(cfg, 'step_8_3_max_revision_rounds', 3)
    
    try:
        config = get_trace_config("8.3", phase=8, tags=["phase_8_step_3"])
        
        # Initial extraction
        result: CategoricalValueIdentificationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CategoricalValueIdentificationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description or ""},
            config=config,
        )
        
        # Refinement loop: detect issues deterministically, ask LLM to revise
        for round_idx in range(max_revision_rounds):
            issues = _detect_categorical_value_issues(
                entity_name=entity_name,
                attribute_name=attribute_name,
                column_datatype=column_datatype,
                categorical_values=result.categorical_values,
            )
            
            if not issues:
                break
            
            logger.warning(
                f"{entity_name}.{attribute_name}: Step 8.3 detected {len(issues)} issue(s); "
                f"requesting LLM revision round {round_idx + 1}/{max_revision_rounds}."
            )
            
            revision_system_prompt = """You are a database schema analyst.

You previously produced a list of categorical values for a column.
You will now receive:
- The column information and context
- Your previous categorical values list (JSON)
- A deterministic list of validation issues

Task:
Return a revised categorical values list that fixes the issues while maintaining:
- All values must match the column's datatype
- All values must be unique
- At least 2 distinct values are required
- Values should be realistic for the domain

Return ONLY valid JSON matching the required schema."""
            
            revision_human_prompt = f"""Entity: {entity_name}
Attribute: {attribute_name}
Column Datatype: {column_datatype}
{context_msg}

Natural language description:
{{nl_description}}

Previous categorical values (JSON):
{{previous_values_json}}

Detected issues (JSON):
{{issues_json}}

Return a revised JSON object with corrected categorical values."""
            
            result = await standardized_llm_call(
                llm=llm,
                output_schema=CategoricalValueIdentificationOutput,
                system_prompt=revision_system_prompt,
                human_prompt_template=revision_human_prompt,
                input_data={
                    "nl_description": nl_description or "",
                    "previous_values_json": json.dumps(result.model_dump(), ensure_ascii=True, indent=2),
                    "issues_json": json.dumps(issues, ensure_ascii=True, indent=2),
                },
                config=config,
            )
        
        # Final validation
        final_issues = _detect_categorical_value_issues(
            entity_name=entity_name,
            attribute_name=attribute_name,
            column_datatype=column_datatype,
            categorical_values=result.categorical_values,
        )
        
        if final_issues:
            logger.warning(
                f"{entity_name}.{attribute_name}: Still has {len(final_issues)} issue(s) after {max_revision_rounds} revision rounds. "
                f"Issues: {final_issues}"
            )
        
        logger.info(
            f"{entity_name}.{attribute_name}: Identified {len(result.categorical_values)} categorical values"
        )
        
        return result
        
    except Exception as e:
        logger.error(
            f"Error identifying categorical values for {entity_name}.{attribute_name}: {e}",
            exc_info=True
        )
        # Return empty result on error
        return CategoricalValueIdentificationOutput(
            categorical_values=[],
            reasoning=f"Error during identification: {str(e)}",
            no_more_changes=True
        )


@traceable_step("8.3", phase=8, tags=['phase_8_step_3'])
async def step_8_3_categorical_value_identification_batch(
    categorical_attributes: Dict[str, List[str]],  # entity_name -> list of categorical attribute names
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> list of attributes
    data_types: Dict[str, Dict[str, Dict[str, Any]]],  # entity_name -> attribute_types -> attribute_name -> type_info
    entity_descriptions: Optional[Dict[str, str]] = None,
    domain: Optional[str] = None,
    nl_description: Optional[str] = None,
) -> Dict[str, Dict[str, CategoricalValueIdentificationOutput]]:
    """
    Step 8.3 (batch): Identify categorical values for all categorical columns.
    
    This is a high fanout step - makes one LLM call per categorical column in parallel.
    
    Note: Boolean columns are automatically filtered out and not processed.
    
    Args:
        categorical_attributes: Dictionary mapping entity names to lists of categorical attribute names
        entity_attributes: Dictionary mapping entity names to their attribute lists
        data_types: Dictionary mapping entity names to attribute type information
        entity_descriptions: Optional dictionary mapping entity names to descriptions
        domain: Optional domain context
        nl_description: Optional natural language description
        
    Returns:
        Dictionary mapping entity_name -> attribute_name -> CategoricalValueIdentificationOutput
    """
    logger.info("Starting Step 8.3: Categorical Value Identification (batch)")
    
    if not categorical_attributes:
        logger.warning("No categorical attributes provided for value identification")
        return {}
    
    # Build list of tasks for parallel execution
    tasks = []
    task_keys = []  # (entity_name, attribute_name) tuples
    
    for entity_name, cat_attrs in categorical_attributes.items():
        if not cat_attrs:
            continue
        
        # Get entity description
        entity_desc = None
        if entity_descriptions:
            entity_desc = entity_descriptions.get(entity_name)
        
        # Get attributes for this entity
        attrs = entity_attributes.get(entity_name, [])
        attr_dict = {attr.get("name", ""): attr for attr in attrs if isinstance(attr, dict)}
        
        # Get data types for this entity
        entity_data_types = data_types.get(entity_name, {})
        attr_types = entity_data_types.get("attribute_types", {}) if isinstance(entity_data_types, dict) else {}
        
        for attr_name in cat_attrs:
            # Get attribute info
            attr_info = attr_dict.get(attr_name, {})
            attr_desc = attr_info.get("description", "") if isinstance(attr_info, dict) else ""
            
            # Get column datatype
            type_info = attr_types.get(attr_name, {}) if isinstance(attr_types, dict) else {}
            sql_type = type_info.get("type", "VARCHAR(255)") if isinstance(type_info, dict) else "VARCHAR(255)"
            
            # Filter out boolean columns deterministically (don't even create a task)
            base_type = sql_type.upper().strip().split("(")[0].strip()
            if base_type in ("BOOLEAN", "BOOL"):
                logger.debug(f"Skipping {entity_name}.{attr_name} - boolean columns are not processed for categorical values")
                continue
            
            # Create task
            tasks.append(
                step_8_3_categorical_value_identification_single(
                    entity_name=entity_name,
                    attribute_name=attr_name,
                    attribute_description=attr_desc,
                    column_datatype=sql_type,
                    entity_description=entity_desc,
                    domain=domain,
                    nl_description=nl_description,
                )
            )
            task_keys.append((entity_name, attr_name))
    
    if not tasks:
        logger.warning("No categorical columns to process")
        return {}
    
    logger.info(f"Processing {len(tasks)} categorical columns in parallel")
    
    # Execute all tasks in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Organize results by entity and attribute
    categorical_values = {}
    for (entity_name, attr_name), result in zip(task_keys, results):
        if isinstance(result, Exception):
            logger.error(
                f"Error processing {entity_name}.{attr_name}: {result}",
                exc_info=result
            )
            # Create error result
            result = CategoricalValueIdentificationOutput(
                categorical_values=[],
                reasoning=f"Error during identification: {str(result)}",
                no_more_changes=True
            )
        
        if entity_name not in categorical_values:
            categorical_values[entity_name] = {}
        categorical_values[entity_name][attr_name] = result
    
    total_values = sum(
        len(result.categorical_values)
        for entity_results in categorical_values.values()
        for result in entity_results.values()
    )
    
    logger.info(
        f"Step 8.3 completed: Identified categorical values for {len(tasks)} columns "
        f"({total_values} total values across all columns)"
    )
    
    return categorical_values
