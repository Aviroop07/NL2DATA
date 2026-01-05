"""Phase 5, Step 5.3: Deterministic Foreign Key Data Type Assignment.

Derives foreign key data types from the referenced primary key types.
This is deterministic - FKs must match the type of the PK they reference.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.observability import traceable_step
from NL2DATA.utils.data_types.type_assignment import AttributeTypeInfo

logger = get_logger(__name__)


class FkTypeAssignment(BaseModel):
    """Foreign key type assignment."""
    attribute_key: str = Field(description="Attribute key in format 'Entity.attribute'")
    type_info: AttributeTypeInfo = Field(description="Type information for this FK attribute")

    model_config = ConfigDict(extra="forbid")


class FkDataTypesOutput(BaseModel):
    """Output structure for foreign key data type assignment."""
    fk_data_types: List[FkTypeAssignment] = Field(
        description="List of foreign key type assignments"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("5.3", phase=5, tags=["phase_5_step_3"])
def step_5_3_deterministic_fk_data_types(
    foreign_keys: List[Dict[str, Any]],
    independent_attribute_data_types,  # Can be dict or IndependentAttributeDataTypesBatchOutput
) -> FkDataTypesOutput:
    """
    Step 5.3: Derive foreign key data types from referenced primary key types.
    
    This is deterministic - a foreign key must have the same type as the primary key
    it references. The FK type is looked up from the independent_attribute_data_types
    (which should contain the PK types from Step 5.2).
    
    Args:
        foreign_keys: List of FK definitions with:
            - from_entity: Entity containing the FK
            - from_attributes: List of FK attribute names
            - to_entity: Referenced entity
            - to_attributes: List of referenced PK attribute names
        independent_attribute_data_types: Dictionary mapping "entity.attribute" -> type_info
            from Step 5.2 (should contain PK types)
        
    Returns:
        dict: fk_data_types dict mapping "entity.attribute" -> type_info
    """
    logger.info(f"Deriving FK types for {len(foreign_keys)} foreign keys")
    
    # Convert independent_attribute_data_types to dict if it's a Pydantic model
    if hasattr(independent_attribute_data_types, 'data_types'):
        # It's IndependentAttributeDataTypesBatchOutput
        independent_types_dict = {
            assignment.attribute_key: assignment.type_info.model_dump()
            for assignment in independent_attribute_data_types.data_types
        }
    elif hasattr(independent_attribute_data_types, 'model_dump'):
        # It's a Pydantic model with data_types field
        independent_types_dict = independent_attribute_data_types.model_dump().get("data_types", {})
        # If it's a list, convert to dict
        if isinstance(independent_types_dict, list):
            independent_types_dict = {
                assignment.get("attribute_key"): assignment.get("type_info", {})
                if isinstance(assignment, dict) else assignment.type_info.model_dump()
                for assignment in independent_types_dict
            }
    else:
        # It's already a dict
        independent_types_dict = independent_attribute_data_types
    
    fk_data_types_list = []
    
    for fk in foreign_keys:
        from_entity = fk.get("from_entity", "")
        from_attributes = fk.get("from_attributes", [])
        to_entity = fk.get("to_entity", "")
        to_attributes = fk.get("to_attributes", [])
        
        if not from_entity or not to_entity:
            logger.warning(f"Skipping FK with missing entity info: {fk}")
            continue
        
        if not from_attributes or not to_attributes:
            logger.warning(f"Skipping FK with missing attributes: {fk}")
            continue
        
        # Match FK attributes to referenced PK attributes
        # For composite keys, match positionally
        for i, (fk_attr, pk_attr) in enumerate(zip(from_attributes, to_attributes)):
            # Look up PK type
            pk_key = f"{to_entity}.{pk_attr}"
            pk_type_info = independent_types_dict.get(pk_key)
            
            fk_key = f"{from_entity}.{fk_attr}"
            
            if not pk_type_info:
                logger.warning(
                    f"PK type not found for {pk_key}, using default BIGINT for FK {fk_key}"
                )
                # Default to BIGINT for FKs if PK type not found
                fk_data_types_list.append(FkTypeAssignment(
                    attribute_key=fk_key,
                    type_info=AttributeTypeInfo(
                        type="BIGINT",
                        size=None,
                        precision=None,
                        scale=None,
                        reasoning=f"Default BIGINT for FK (PK type for {pk_key} not found in independent types)",
                    )
                ))
                continue
            
            # Copy PK type info for FK (FK must match PK type)
            # Handle both dict and AttributeTypeInfo
            if isinstance(pk_type_info, dict):
                pk_type = pk_type_info.get("type", "BIGINT")
                reasoning = (
                    f"FK type matches referenced PK type: {pk_key} has type {pk_type}. "
                    f"Foreign keys must have the same type as the primary key they reference."
                )
                fk_data_types_list.append(FkTypeAssignment(
                    attribute_key=fk_key,
                    type_info=AttributeTypeInfo(
                        type=pk_type,
                        size=pk_type_info.get("size"),
                        precision=pk_type_info.get("precision"),
                        scale=pk_type_info.get("scale"),
                        reasoning=reasoning,
                    )
                ))
            else:
                # It's already an AttributeTypeInfo
                reasoning = (
                    f"FK type matches referenced PK type: {pk_key} has type {pk_type_info.type}. "
                    f"Foreign keys must have the same type as the primary key they reference."
                )
                fk_data_types_list.append(FkTypeAssignment(
                    attribute_key=fk_key,
                    type_info=AttributeTypeInfo(
                        type=pk_type_info.type,
                        size=pk_type_info.size,
                        precision=pk_type_info.precision,
                        scale=pk_type_info.scale,
                        reasoning=reasoning,
                    )
                ))
            
            logger.debug(
                f"Assigned FK type {pk_type_info.get('type', 'BIGINT') if isinstance(pk_type_info, dict) else pk_type_info.type} to {fk_key} "
                f"(matches PK {pk_key})"
            )
    
    logger.info(f"Derived types for {len(fk_data_types_list)} foreign key attributes")
    
    return FkDataTypesOutput(fk_data_types=fk_data_types_list)
