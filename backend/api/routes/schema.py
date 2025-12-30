"""Schema editing endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from backend.models.requests import SaveChangesRequest
from backend.models.responses import SaveChangesResponse, DistributionMetadataResponse, DistributionMetadata, DistributionParameter
from backend.dependencies import (
    get_validation_service,
    get_conversion_service,
    get_diagram_service,
    get_job_manager
)
from backend.services.validation_service import ValidationService
from backend.services.conversion_service import ConversionService
from backend.services.diagram_service import DiagramService
from backend.utils.job_manager import JobManager
from NL2DATA.utils.distributions.catalog import get_distribution_catalog

router = APIRouter(prefix="/api/schema", tags=["schema"])


@router.get("/distributions/metadata", response_model=DistributionMetadataResponse)
async def get_distributions_metadata():
    """
    Get all available distributions with parameter definitions.
    
    Used by frontend to dynamically generate parameter input forms
    based on selected distribution.
    
    Reads from NL2DATA distribution catalog.
    """
    catalog = get_distribution_catalog()
    
    distributions = []
    for dist_name, dist_info in catalog.items():
        # Convert ParameterInfo to DistributionParameter
        parameters = [
            DistributionParameter(
                name=param_name,
                type=param_info.type,
                description=param_info.description
            )
            for param_name, param_info in dist_info.parameters.items()
        ]
        
        distributions.append(
            DistributionMetadata(
                name=dist_name,
                parameters=parameters
            )
        )
    
    # Add additional distributions not in catalog (for backward compatibility)
    # These are special distributions used by the frontend
    additional_distributions = [
        DistributionMetadata(
            name="pareto",
            parameters=[
                DistributionParameter(name="alpha", type="decimal", description="Shape parameter"),
                DistributionParameter(name="xmin", type="decimal", description="Scale parameter (minimum value)")
            ]
        ),
        DistributionMetadata(
            name="bernoulli",
            parameters=[
                DistributionParameter(name="p", type="decimal", description="Probability of success (0-1)")
            ]
        ),
        DistributionMetadata(
            name="categorical",
            parameters=[
                DistributionParameter(name="categories", type="array", description="List of category values"),
                DistributionParameter(name="probabilities", type="array", description="List of probabilities (optional, defaults to uniform)")
            ]
        ),
        DistributionMetadata(
            name="seasonal",
            parameters=[
                DistributionParameter(name="period", type="integer", description="Period length"),
                DistributionParameter(name="amplitude", type="decimal", description="Amplitude of seasonal variation"),
                DistributionParameter(name="phase", type="decimal", description="Phase offset (optional)")
            ]
        ),
        DistributionMetadata(
            name="trend",
            parameters=[
                DistributionParameter(name="slope", type="decimal", description="Trend slope"),
                DistributionParameter(name="intercept", type="decimal", description="Starting value (optional)")
            ]
        )
    ]
    
    distributions.extend(additional_distributions)
    
    return DistributionMetadataResponse(distributions=distributions)


@router.post("/save_changes", response_model=SaveChangesResponse)
async def save_changes(
    request: SaveChangesRequest,
    validation_service: ValidationService = Depends(get_validation_service),
    conversion_service: ConversionService = Depends(get_conversion_service),
    diagram_service: DiagramService = Depends(get_diagram_service),
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Save user modifications to schema.
    
    Workflow:
    - ER changes must be saved before Schema editing is unlocked
    - Validates changes deterministically
    - If valid: commits changes, converts ER ↔ Schema, updates state
    - If invalid: returns errors, keeps draft state
    """
    # Check job exists first (before validation)
    job = job_manager.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    current_state = job.get("state", {})
    
    # Validate changes
    validation_errors = await validation_service.validate_changes(
        current_state=current_state,
        changes=request.changes,
        edit_mode=request.edit_mode
    )
    
    if validation_errors:
        return SaveChangesResponse(
            status="validation_failed",
            validation_errors=validation_errors
        )
    
    # Apply changes and convert
    updated_state = await conversion_service.apply_changes(
        current_state=current_state,
        changes=request.changes,
        edit_mode=request.edit_mode
    )
    
    # Generate ER diagram image if ER was edited
    # Note: Image is generated and stored, URL is constructed from job_id
    er_image_url = None
    if request.edit_mode == "er_diagram":
        # Generate and store image (returns bytes, stored internally)
        await diagram_service.generate_er_diagram(
            job_id=request.job_id,
            er_state=updated_state,
            format="png"
        )
        # Construct URL for frontend to fetch
        er_image_url = f"/api/schema/er_diagram_image/{request.job_id}?format=png"
    
    # Update job state
    job_manager.update_job_state(request.job_id, updated_state)
    
    return SaveChangesResponse(
        status="success",
        updated_state=updated_state,
        validation_errors=[],
        er_diagram_image_url=er_image_url
    )


@router.get("/er_diagram_image/{job_id}")
async def get_er_diagram_image(
    job_id: str,
    format: str = "png",
    diagram_service: DiagramService = Depends(get_diagram_service),
    job_manager: JobManager = Depends(get_job_manager)
):
    """
    Get ER diagram as static image.
    
    Returns PNG or SVG image generated from current ER model state.
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    state = job.get("state", {})
    image_data = await diagram_service.generate_er_diagram(
        job_id=job_id,
        er_state=state,
        format=format
    )
    
    if format == "png":
        return Response(content=image_data, media_type="image/png")
    else:
        return Response(content=image_data, media_type="image/svg+xml")

