"""Checkpoint endpoints for step-by-step pipeline execution."""

import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any

from backend.models.requests import (
    CheckpointProceedRequest,
    DomainEditRequest,
    EntitiesEditRequest,
    RelationsEditRequest,
    AttributesEditRequest,
    PrimaryKeysEditRequest,
    MultivaluedDerivedEditRequest,
    NullabilityEditRequest,
    ERDiagramEditRequest,
    DatatypesEditRequest,
    RelationalSchemaEditRequest,
    InformationMiningEditRequest,
    FunctionalDependenciesEditRequest,
    ConstraintsEditRequest,
    GenerationStrategiesEditRequest
)
from backend.models.responses import CheckpointResponse, CheckpointProceedResponse
from backend.dependencies import (
    get_job_manager,
    get_nl2data_service
)
from backend.utils.job_manager import JobManager
from backend.services.nl2data_service import NL2DataService
from backend.services.er_diagram_compiler import generate_and_save_er_diagram
from backend.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/checkpoint", tags=["checkpoints"])


def _extract_checkpoint_data(state: Dict[str, Any], checkpoint_type: str) -> Dict[str, Any]:
    """Extract checkpoint data from state based on checkpoint type."""
    prev_answers = state.get("previous_answers", {})
    
    if checkpoint_type == "domain":
        return {
            "domain": state.get("domain"),
            "has_explicit_domain": state.get("has_explicit_domain", False),
            "justification": {
                "step_1_1": prev_answers.get("1.1", {}),
                "step_1_2": prev_answers.get("1.2", {})
            }
        }
    elif checkpoint_type == "entities":
        return {
            "entities": state.get("entities", []),
            "justification": {
                "step_1_4": prev_answers.get("1.4", {})
            }
        }
    elif checkpoint_type == "relations":
        return {
            "relations": state.get("relations", []),
            "relation_cardinalities": state.get("relation_cardinalities", {}),
            "justification": {
                "step_1_9": prev_answers.get("1.9", {}),
                "step_1_11": prev_answers.get("1.11", {})
            }
        }
    elif checkpoint_type == "attributes":
        return {
            "attributes": state.get("attributes", {}),
            "justification": {
                "step_2_2": prev_answers.get("2.2", {})
            }
        }
    elif checkpoint_type == "primary_keys":
        return {
            "primary_keys": state.get("primary_keys", {}),
            "justification": {
                "step_2_7": prev_answers.get("2.7", {})
            }
        }
    elif checkpoint_type == "multivalued_derived":
        return {
            "multivalued_derived": state.get("multivalued_derived", {}),
            "derived_formulas": state.get("derived_formulas", {}),
            "attributes": state.get("attributes", {}),  # Include attributes so frontend can show all attributes
            "justification": {
                "step_2_8": prev_answers.get("2.8", {}),
                "step_2_9": prev_answers.get("2.9", {})
            }
        }
    elif checkpoint_type == "nullability":
        return {
            "nullability": state.get("nullability", {}),
            "justification": {
                "step_5_5": prev_answers.get("5.5", {})
            }
        }
    elif checkpoint_type == "er_diagram":
        er_design = state.get("er_design", {})
        # Generate ER diagram image if er_design exists
        # Note: imageUrl will be added to er_design in get_checkpoint endpoint
        return {
            "er_design": er_design,
            "justification": {
                "step_3_1": prev_answers.get("3.1", {})
            }
        }
    elif checkpoint_type == "datatypes":
        return {
            "data_types": state.get("data_types", {}),
            "relational_schema": state.get("relational_schema", {}),  # Include enriched schema
            "justification": {
                "step_5_1": prev_answers.get("5.1", {}),
                "step_5_2": prev_answers.get("5.2", {}),
                "step_5_3": prev_answers.get("5.3", {}),
                "step_5_4": prev_answers.get("5.4", {}),
            }
        }
    elif checkpoint_type == "relational_schema":
        return {
            "relational_schema": state.get("relational_schema", {}),
            "justification": {
                "step_4_1": prev_answers.get("4.1", {})
            }
        }
    elif checkpoint_type == "information_mining":
        return {
            "information_needs": state.get("information_needs", []),
            "sql_validation_result": prev_answers.get("7.2", {}),
            "justification": {
                "step_7_1": prev_answers.get("7.1", {}),
                "step_7_2": prev_answers.get("7.2", {})
            }
        }
    elif checkpoint_type == "functional_dependencies":
        return {
            "functional_dependencies": state.get("functional_dependencies", []),
            "relational_schema": state.get("relational_schema", {}),  # For attribute dropdowns
            "primary_keys": state.get("primary_keys", {}),  # For validation
            "justification": {
                "step_8_1": prev_answers.get("8.1", {})
            }
        }
    elif checkpoint_type == "constraints":
        return {
            "constraints": state.get("constraints", []),
            "relational_schema": state.get("relational_schema", {}),  # For context
            "justification": {
                "step_9_1": prev_answers.get("9.1", {}),
                "step_9_2": prev_answers.get("9.2", {}),
                "step_9_3": prev_answers.get("9.3", {})
            }
        }
    elif checkpoint_type == "generation_strategies":
        return {
            "generation_strategies": state.get("generation_strategies", {}),
            "relational_schema": state.get("relational_schema", {}),  # For context
            "justification": {
                "step_9_6": prev_answers.get("9.6", {}),
                "step_9_7": prev_answers.get("9.7", {}),
                "step_9_8": prev_answers.get("9.8", {}),
                "step_9_9": prev_answers.get("9.9", {}),
                "step_9_10": prev_answers.get("9.10", {}),
                "step_9_11": prev_answers.get("9.11", {})
            }
        }
    else:
        return {}


@router.get("/{job_id}", response_model=CheckpointResponse)
async def get_checkpoint(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Get current checkpoint data for a job.
    
    Returns the data at the current checkpoint (domain, entities, relations, or attributes).
    """
    logger.info(f"GET /api/checkpoint/{job_id}")
    
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    status = job.get("status", "")
    state = job.get("state", {})
    
    # Map status to checkpoint type
    status_to_checkpoint = {
        "checkpoint_domain": "domain",
        "checkpoint_entities": "entities",
        "checkpoint_relations": "relations",
        "checkpoint_attributes": "attributes",
        "checkpoint_primary_keys": "primary_keys",
        "checkpoint_multivalued_derived": "multivalued_derived",
        "checkpoint_er_diagram": "er_diagram",
        "checkpoint_relational_schema": "relational_schema",
        "checkpoint_datatypes": "datatypes",
        "checkpoint_nullability": "nullability",
        "checkpoint_information_mining": "information_mining",
        "checkpoint_functional_dependencies": "functional_dependencies",
        "checkpoint_constraints": "constraints",
        "checkpoint_generation_strategies": "generation_strategies",
        "completed": "complete"
    }
    
    checkpoint_type = status_to_checkpoint.get(status)
    if not checkpoint_type:
        # If job is still processing (pending or in_progress), return a "not ready" response
        # Frontend should poll until checkpoint is ready
        if status in ["pending", "in_progress"]:
            raise HTTPException(
                status_code=202,  # Accepted - processing in progress
                detail=f"Checkpoint not ready yet. Job status: {status}. Please retry in a moment."
            )
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at a checkpoint. Current status: {status}"
        )
    
    if checkpoint_type == "complete":
        data = {"message": "Pipeline completed"}
    else:
        data = _extract_checkpoint_data(state, checkpoint_type)
        
        # Generate ER diagram image if this is the er_diagram checkpoint
        if checkpoint_type == "er_diagram" and data.get("er_design"):
            er_design_dict = data.get("er_design", {})
            # Only generate if er_design has entities (valid ER design)
            # Also regenerate if imageUrl is missing (in case it wasn't generated before)
            if er_design_dict.get("entities") and not er_design_dict.get("imageUrl"):
                try:
                    logger.info(f"Generating ER diagram image for job {job_id} (imageUrl missing)")
                    image_path = generate_and_save_er_diagram(
                        er_design_dict=er_design_dict,
                        job_id=job_id,
                        storage_path=settings.er_diagram_storage_path,
                        format="svg"
                    )
                    # Add imageUrl to er_design object (frontend expects it there)
                    if "er_design" in data:
                        data["er_design"]["imageUrl"] = f"/static/{image_path}"
                    logger.info(f"ER diagram image generated successfully: /static/{image_path}")
                except Exception as e:
                    logger.error(f"Error generating ER diagram image: {e}", exc_info=True)
                    # Continue without image URL if generation fails
            elif er_design_dict.get("entities") and er_design_dict.get("imageUrl"):
                logger.debug(f"ER diagram image already exists for job {job_id}: {er_design_dict.get('imageUrl')}")
            else:
                logger.warning(f"ER design has no entities, skipping image generation for job {job_id}")
    
    return CheckpointResponse(
        checkpoint_type=checkpoint_type,
        data=data,
        justification=data.get("justification")
    )


@router.post("/domain/save", response_model=CheckpointProceedResponse)
async def save_domain_edit(
    request: DomainEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save domain edits and update job state."""
    logger.info(f"POST /api/checkpoint/domain/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_domain":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at domain checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited domain
    state = job.get("state", {})
    state["domain"] = request.domain
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Domain updated to: {request.domain}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Domain saved successfully",
        next_checkpoint="entities"
    )


@router.post("/entities/save", response_model=CheckpointProceedResponse)
async def save_entities_edit(
    request: EntitiesEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save entity edits and update job state."""
    logger.info(f"POST /api/checkpoint/entities/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_entities":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at entities checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited entities
    state = job.get("state", {})
    state["entities"] = request.entities
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Entities updated. Count: {len(request.entities)}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Entities saved successfully",
        next_checkpoint="relations"
    )


@router.post("/relations/save", response_model=CheckpointProceedResponse)
async def save_relations_edit(
    request: RelationsEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save relation edits and update job state."""
    logger.info(f"POST /api/checkpoint/relations/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_relations":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at relations checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited relations
    state = job.get("state", {})
    state["relations"] = request.relations
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Relations updated. Count: {len(request.relations)}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Relations saved successfully",
        next_checkpoint="attributes"
    )


@router.post("/attributes/save", response_model=CheckpointProceedResponse)
async def save_attributes_edit(
    request: AttributesEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save attribute edits and update job state."""
    logger.info(f"POST /api/checkpoint/attributes/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_attributes":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at attributes checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited attributes
    state = job.get("state", {})
    state["attributes"] = request.attributes
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Attributes updated for {len(request.attributes)} entities")
    
    return CheckpointProceedResponse(
        status="success",
        message="Attributes saved successfully",
        next_checkpoint="primary_keys"
    )


@router.post("/primary_keys/save", response_model=CheckpointProceedResponse)
async def save_primary_keys_edit(
    request: PrimaryKeysEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save primary key edits and update job state."""
    logger.info(f"POST /api/checkpoint/primary_keys/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_primary_keys":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at primary keys checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited primary keys
    state = job.get("state", {})
    state["primary_keys"] = request.primary_keys
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Primary keys updated for {len(request.primary_keys)} entities")
    
    return CheckpointProceedResponse(
        status="success",
        message="Primary keys saved successfully",
        next_checkpoint="multivalued_derived"
    )


@router.post("/er_diagram/save", response_model=CheckpointProceedResponse)
async def save_er_diagram_edit(
    request: ERDiagramEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save ER diagram edits and update job state."""
    logger.info(f"POST /api/checkpoint/er_diagram/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_er_diagram":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at ER diagram checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited ER design
    state = job.get("state", {})
    state["er_design"] = request.er_design
    job_manager.update_job_state(request.job_id, state)
    
    logger.info("ER diagram updated")
    
    return CheckpointProceedResponse(
        status="success",
        message="ER diagram saved successfully",
        next_checkpoint="relational_schema"
    )


@router.post("/multivalued_derived/save", response_model=CheckpointProceedResponse)
async def save_multivalued_derived_edit(
    request: MultivaluedDerivedEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save multivalued/derived attributes edits and update job state."""
    logger.info(f"POST /api/checkpoint/multivalued_derived/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_multivalued_derived":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at multivalued/derived checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited multivalued/derived data
    state = job.get("state", {})
    state["multivalued_derived"] = request.multivalued_derived
    state["derived_formulas"] = request.derived_formulas
    job_manager.update_job_state(request.job_id, state)
    
    logger.info("Multivalued/derived attributes updated")
    
    return CheckpointProceedResponse(
        status="success",
        message="Multivalued/derived attributes saved successfully",
        next_checkpoint="er_diagram"
    )


@router.post("/nullability/save", response_model=CheckpointProceedResponse)
async def save_nullability_edit(
    request: NullabilityEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save nullability edits and update job state."""
    logger.info(f"POST /api/checkpoint/nullability/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_nullability":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at nullability checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited nullability
    state = job.get("state", {})
    state["nullability"] = request.nullability
    job_manager.update_job_state(request.job_id, state)
    
    logger.info("Nullability constraints updated")
    
    return CheckpointProceedResponse(
        status="success",
        message="Nullability constraints saved successfully",
        next_checkpoint="information_mining"
    )


@router.post("/datatypes/save", response_model=CheckpointProceedResponse)
async def save_datatypes_edit(
    request: DatatypesEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save datatype edits and update job state."""
    logger.info(f"POST /api/checkpoint/datatypes/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_datatypes":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at datatypes checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited data types
    state = job.get("state", {})
    state["data_types"] = request.data_types
    job_manager.update_job_state(request.job_id, state)
    
    logger.info("Data types updated")
    
    return CheckpointProceedResponse(
        status="success",
        message="Data types saved successfully",
        next_checkpoint="nullability"
    )


@router.post("/relational_schema/save", response_model=CheckpointProceedResponse)
async def save_relational_schema_edit(
    request: RelationalSchemaEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save relational schema edits and update job state."""
    logger.info(f"POST /api/checkpoint/relational_schema/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_relational_schema":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at relational schema checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited relational schema
    state = job.get("state", {})
    state["relational_schema"] = request.relational_schema
    job_manager.update_job_state(request.job_id, state)
    
    logger.info("Relational schema updated")
    
    return CheckpointProceedResponse(
        status="success",
        message="Relational schema saved successfully",
        next_checkpoint="datatypes"
    )


@router.post("/information_mining/save", response_model=CheckpointProceedResponse)
async def save_information_mining_edit(
    request: InformationMiningEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save information mining edits and update job state."""
    logger.info(f"POST /api/checkpoint/information_mining/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_information_mining":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at information mining checkpoint. Current status: {job.get('status')}"
        )
    
    # Validate SQL queries for each information need
    from NL2DATA.phases.phase7.step_7_2_sql_generation_and_validation import _validate_sql_on_schema
    relational_schema = job.get("state", {}).get("relational_schema", {})
    
    if not relational_schema:
        raise HTTPException(
            status_code=400,
            detail="Relational schema not found in state. Cannot validate SQL queries."
        )
    
    validated_needs = []
    for need in request.information_needs:
        sql_query = need.get("sql_query", "")
        if sql_query:
            is_valid, error_msg = _validate_sql_on_schema(sql_query, relational_schema)
            if not is_valid:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid SQL query for information need '{need.get('description', 'unknown')}': {error_msg}"
                )
        validated_needs.append(need)
    
    # Update state with edited information needs
    state = job.get("state", {})
    state["information_needs"] = validated_needs
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Information mining updated. Count: {len(validated_needs)}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Information mining saved successfully",
        next_checkpoint="functional_dependencies"
    )


@router.post("/functional_dependencies/save", response_model=CheckpointProceedResponse)
async def save_functional_dependencies_edit(
    request: FunctionalDependenciesEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save functional dependencies edits and update job state with validation."""
    logger.info(f"POST /api/checkpoint/functional_dependencies/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_functional_dependencies":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at functional dependencies checkpoint. Current status: {job.get('status')}"
        )
    
    state = job.get("state", {})
    primary_keys = state.get("primary_keys", {})
    relational_schema = state.get("relational_schema", {})
    
    if not relational_schema or not relational_schema.get("tables"):
        raise HTTPException(
            status_code=400,
            detail="Relational schema not found. Cannot validate functional dependencies."
        )
    
    # Extract all attributes from relational schema for validation
    all_attributes = set()
    table_to_entity_map: Dict[str, str] = {}  # Map table names to entity names (may differ)
    for table in relational_schema.get("tables", []):
        table_name = table.get("name", "")
        for col in table.get("columns", []):
            col_name = col.get("name", "")
            if table_name and col_name:
                all_attributes.add(f"{table_name}.{col_name}")
        # Try to map table to entity (table name might be different from entity name)
        # For now, assume table name matches entity name, but we'll also check primary_keys
        table_to_entity_map[table_name] = table_name
    
    # Extract all PK attributes - check both by entity name and table name
    pk_attributes = set()
    for entity_name, pk_list in primary_keys.items():
        for pk_attr in pk_list:
            # Add both entity.attr and table.attr formats
            pk_attributes.add(f"{entity_name}.{pk_attr}")
            # Also check if any table matches this entity
            for table_name, mapped_entity in table_to_entity_map.items():
                if mapped_entity == entity_name or table_name == entity_name:
                    pk_attributes.add(f"{table_name}.{pk_attr}")
    
    # Validate each functional dependency
    validated_fds = []
    seen_fds = set()  # Track seen FDs to detect duplicates
    
    for fd in request.functional_dependencies:
        lhs = fd.get("lhs", [])
        rhs = fd.get("rhs", [])
        
        # Validation checks
        if not lhs or not rhs:
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency must have both lhs and rhs (non-empty lists)"
            )
        
        # Convert to sets for easier comparison
        lhs_set = set(lhs)
        rhs_set = set(rhs)
        
        # Check 1: No common attributes in lhs and rhs
        common = lhs_set & rhs_set
        if common:
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency has common attributes in lhs and rhs: {common}"
            )
        
        # Check 2: No PK attributes in lhs (trivial FDs - PK determines all attributes)
        # Check if any attribute in lhs is a primary key
        lhs_pk_intersection = set()
        for attr in lhs:
            # Check if this attribute is a PK in any entity
            for entity_name, pk_list in primary_keys.items():
                if attr.startswith(f"{entity_name}."):
                    attr_name = attr.split(".", 1)[1] if "." in attr else attr
                    if attr_name in pk_list:
                        lhs_pk_intersection.add(attr)
                elif attr == entity_name:  # Handle case where attr might just be entity name
                    # This shouldn't happen, but check anyway
                    pass
        
        if lhs_pk_intersection:
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency has primary key attributes in lhs (trivial - PK determines all attributes): {lhs_pk_intersection}"
            )
        
        # Check 2b: LHS should not be a superset of any PK (also trivial)
        for entity_name, pk_list in primary_keys.items():
            pk_set = {f"{entity_name}.{pk}" for pk in pk_list}
            if pk_set.issubset(lhs_set):
                raise HTTPException(
                    status_code=400,
                    detail=f"Functional dependency lhs contains a complete primary key (trivial): {entity_name} PK {pk_list}"
                )
        
        # Check 3: All attributes must exist in schema
        all_fd_attrs = lhs_set | rhs_set
        invalid_attrs = all_fd_attrs - all_attributes
        if invalid_attrs:
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency contains invalid attributes not in schema: {invalid_attrs}"
            )
        
        # Check 3b: All attributes in lhs and rhs should be from the same entity (cross-entity FDs not allowed)
        # Extract entity names from attributes
        lhs_entities = set()
        rhs_entities = set()
        for attr in lhs:
            if "." in attr:
                entity = attr.split(".", 1)[0]
                lhs_entities.add(entity)
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Attribute '{attr}' in lhs must be in format 'Entity.attribute'"
                )
        for attr in rhs:
            if "." in attr:
                entity = attr.split(".", 1)[0]
                rhs_entities.add(entity)
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Attribute '{attr}' in rhs must be in format 'Entity.attribute'"
                )
        
        all_entities = lhs_entities | rhs_entities
        if len(all_entities) > 1:
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency spans multiple entities (cross-entity FDs not allowed): {all_entities}. All attributes must be from the same entity."
            )
        
        # Check 3c: No duplicate attributes in lhs
        if len(lhs) != len(lhs_set):
            duplicates = [attr for attr in lhs if lhs.count(attr) > 1]
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency lhs has duplicate attributes: {set(duplicates)}"
            )
        
        # Check 3d: No duplicate attributes in rhs
        if len(rhs) != len(rhs_set):
            duplicates = [attr for attr in rhs if rhs.count(attr) > 1]
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency rhs has duplicate attributes: {set(duplicates)}"
            )
        
        # Check 4: No duplicate FDs (normalize by sorting lhs and rhs)
        fd_key = (tuple(sorted(lhs)), tuple(sorted(rhs)))
        if fd_key in seen_fds:
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate functional dependency: {lhs} -> {rhs}"
            )
        seen_fds.add(fd_key)
        
        # Check 5: LHS should not be empty (already checked above, but ensure)
        if not lhs:
            raise HTTPException(
                status_code=400,
                detail="Functional dependency lhs cannot be empty"
            )
        
        # Check 6: RHS should not be empty (already checked above, but ensure)
        if not rhs:
            raise HTTPException(
                status_code=400,
                detail="Functional dependency rhs cannot be empty"
            )
        
        # Check 7: LHS should not be a subset of RHS (trivial - if RHS contains LHS, then LHS -> RHS is trivial)
        if lhs_set.issubset(rhs_set):
            raise HTTPException(
                status_code=400,
                detail=f"Functional dependency is trivial: lhs {lhs} is a subset of rhs {rhs}"
            )
        
        validated_fds.append(fd)
    
    # Update state with edited functional dependencies
    state["functional_dependencies"] = validated_fds
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Functional dependencies updated. Count: {len(validated_fds)}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Functional dependencies saved successfully",
        next_checkpoint="constraints"
    )


@router.post("/constraints/save", response_model=CheckpointProceedResponse)
async def save_constraints_edit(
    request: ConstraintsEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save constraints edits and update job state."""
    logger.info(f"POST /api/checkpoint/constraints/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_constraints":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at constraints checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited constraints
    state = job.get("state", {})
    state["constraints"] = request.constraints
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Constraints updated. Count: {len(request.constraints)}")
    
    return CheckpointProceedResponse(
        status="success",
        message="Constraints saved successfully",
        next_checkpoint="generation_strategies"
    )


@router.post("/generation_strategies/save", response_model=CheckpointProceedResponse)
async def save_generation_strategies_edit(
    request: GenerationStrategiesEditRequest,
    job_manager: JobManager = Depends(get_job_manager)
):
    """Save generation strategies edits and update job state."""
    logger.info(f"POST /api/checkpoint/generation_strategies/save for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "checkpoint_generation_strategies":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not at generation strategies checkpoint. Current status: {job.get('status')}"
        )
    
    # Update state with edited generation strategies
    state = job.get("state", {})
    state["generation_strategies"] = request.generation_strategies
    job_manager.update_job_state(request.job_id, state)
    
    logger.info(f"Generation strategies updated for {len(request.generation_strategies)} entities")
    
    return CheckpointProceedResponse(
        status="success",
        message="Generation strategies saved successfully",
        next_checkpoint=None  # This is the last checkpoint
    )


@router.post("/proceed", response_model=CheckpointProceedResponse)
async def proceed_to_next_checkpoint(
    request: CheckpointProceedRequest,
    job_manager: JobManager = Depends(get_job_manager),
    nl2data_service: NL2DataService = Depends(get_nl2data_service)
):
    """
    Proceed to the next checkpoint in the pipeline.
    
    Executes the next phase of the pipeline and stops at the next checkpoint.
    """
    logger.info(f"POST /api/checkpoint/proceed for job {request.job_id}")
    
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    status = job.get("status", "")
    state = job.get("state", {})
    nl_description = job.get("nl_description", "")
    
    # Determine next checkpoint based on current status
    if status == "checkpoint_domain" or status == "pending":
        # Execute to entities checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="entities",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "entities")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to entities checkpoint",
                next_checkpoint="entities",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to entities checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_entities":
        # Execute to relations checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="relations",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "relations")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to relations checkpoint",
                next_checkpoint="relations",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to relations checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_relations":
        # Execute to attributes checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="attributes",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "attributes")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to attributes checkpoint",
                next_checkpoint="attributes",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to attributes checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_attributes":
        # Execute to primary keys checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="primary_keys",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "primary_keys")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to primary keys checkpoint",
                next_checkpoint="primary_keys",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to primary keys checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_primary_keys":
        # Execute to multivalued/derived checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="multivalued_derived",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "multivalued_derived")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to multivalued/derived attributes checkpoint",
                next_checkpoint="multivalued_derived",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to ER diagram checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_multivalued_derived":
        # Execute to ER diagram checkpoint
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="er_diagram",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "er_diagram")
            
            # Generate ER diagram image
            er_design = new_state.get("er_design", {})
            if er_design and "er_design" in checkpoint_data:
                # Only generate if er_design has entities (valid ER design)
                if er_design.get("entities"):
                    try:
                        logger.info(f"Generating ER diagram image for job {request.job_id}")
                        image_path = generate_and_save_er_diagram(
                            er_design_dict=er_design,
                            job_id=request.job_id,
                            storage_path=settings.er_diagram_storage_path,
                            format="svg"
                        )
                        # Add imageUrl to er_design object (frontend expects it there)
                        checkpoint_data["er_design"]["imageUrl"] = f"/static/{image_path}"
                        logger.info(f"ER diagram image generated successfully: /static/{image_path}")
                    except Exception as e:
                        logger.error(f"Error generating ER diagram image: {e}", exc_info=True)
                        # Continue without image URL if generation fails
                else:
                    logger.warning(f"ER design has no entities, skipping image generation for job {request.job_id}")
            
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to ER diagram checkpoint",
                next_checkpoint="er_diagram",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to ER diagram checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_er_diagram":
        # Execute to datatypes checkpoint (Phase 5)
        # Note: Relational schema will be compiled internally during datatypes checkpoint if needed
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="datatypes",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "datatypes")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to datatypes checkpoint",
                next_checkpoint="datatypes",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to datatypes checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_datatypes":
        # Execute to nullability checkpoint (Phase 5.5)
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="nullability",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "nullability")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to nullability checkpoint",
                next_checkpoint="nullability",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to nullability checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_nullability":
        # Execute to relational schema checkpoint (Phase 4)
        # Note: Relational schema should already be compiled (done during datatypes), but we show it here
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="relational_schema",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "relational_schema")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to relational schema checkpoint",
                next_checkpoint="relational_schema",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to relational schema checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_relational_schema":
        # Execute to information mining checkpoint (Phase 7)
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="information_mining",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "information_mining")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to information mining checkpoint",
                next_checkpoint="information_mining",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to information mining checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_information_mining":
        # Execute to functional dependencies checkpoint (Phase 8)
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="functional_dependencies",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "functional_dependencies")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to functional dependencies checkpoint",
                next_checkpoint="functional_dependencies",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to functional dependencies checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_functional_dependencies":
        # Execute to constraints checkpoint (Phase 9)
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="constraints",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "constraints")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to constraints checkpoint",
                next_checkpoint="constraints",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to constraints checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_constraints":
        # Execute to generation strategies checkpoint (Phase 9)
        try:
            new_state = await nl2data_service.execute_to_checkpoint(
                job_id=request.job_id,
                nl_description=nl_description,
                checkpoint_type="generation_strategies",
                job_manager=job_manager,
                current_state=state
            )
            checkpoint_data = _extract_checkpoint_data(new_state, "generation_strategies")
            return CheckpointProceedResponse(
                status="success",
                message="Proceeded to generation strategies checkpoint",
                next_checkpoint="generation_strategies",
                checkpoint_data=checkpoint_data,
                checkpoint_justification=checkpoint_data.get("justification")
            )
        except Exception as e:
            logger.error(f"Error proceeding to generation strategies checkpoint: {e}", exc_info=True)
            return CheckpointProceedResponse(
                status="error",
                message=f"Failed to proceed: {str(e)}",
                next_checkpoint=None,
                checkpoint_data=None,
                checkpoint_justification=None
            )
    elif status == "checkpoint_generation_strategies":
        # Pipeline is complete - no more checkpoints
        return CheckpointProceedResponse(
            status="success",
            message="Pipeline completed successfully",
            next_checkpoint="complete",
            checkpoint_data={"message": "Pipeline completed successfully"},
            checkpoint_justification=None
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot proceed from status: {status}"
        )
