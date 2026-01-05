"""Unit tests for Step 1.2: Entity Mention Detection.

These tests provide proper input structure, mock LLM responses,
perform deterministic validations, and display outputs.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_2_entity_mention_detection import (
    step_1_2_entity_mention_detection,
    EntityMentionOutput,
    EntityWithEvidence
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_2_explicit_entities():
    """Test Step 1.2 with explicitly mentioned entities."""
    TestResultDisplay.print_test_header("Entity Mention Detection", "1.2")
    TestResultDisplay.print_test_case(1, "Explicit entity detection")
    
    # Input - Complex financial transactions dataset
    nl_description = "Generate a financial transactions dataset with a large transaction fact table (at least 50 million rows) and dimension tables for customers, merchants, cards, and geography. Transactions are linked to cards which belong to customers. Merchants are located in geographic regions, and transactions occur at merchant locations with fraud detection patterns."
    input_data = {"nl_description": nl_description}
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = EntityMentionOutput(
        has_explicit_entities=True,
        mentioned_entities=[
            EntityWithEvidence(name="Transaction", evidence="transaction fact table"),
            EntityWithEvidence(name="Customer", evidence="customers"),
            EntityWithEvidence(name="Merchant", evidence="merchants"),
            EntityWithEvidence(name="Card", evidence="cards"),
            EntityWithEvidence(name="Geography", evidence="geographic regions")
        ],
        reasoning="Multiple entities are explicitly mentioned: transaction, customers, merchants, cards, and geography"
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.2"):
            result = await step_1_2_entity_mention_detection(nl_description)
    
    TestResultDisplay.print_output_summary(result)
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_entities", "mentioned_entities", "reasoning"]
    )
    validations.append(struct_validation)
    
    # Validate types
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "has_explicit_entities": bool,
            "mentioned_entities": list,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    # Validate entity evidence
    evidence_validation = {"valid": True, "errors": []}
    mentioned_entities = result_dict.get("mentioned_entities", [])
    if result_dict.get("has_explicit_entities"):
        if not isinstance(mentioned_entities, list) or len(mentioned_entities) == 0:
            evidence_validation["valid"] = False
            evidence_validation["errors"].append(
                "When has_explicit_entities is True, mentioned_entities should be non-empty"
            )
        else:
            for entity in mentioned_entities:
                # Handle Pydantic models in the list
                if hasattr(entity, 'model_dump'):
                    entity = entity.model_dump()
                if not isinstance(entity, dict):
                    evidence_validation["valid"] = False
                    evidence_validation["errors"].append(
                        "Each mentioned entity should be a dictionary"
                    )
                    continue
                evidence = entity.get("evidence", "")
                if not evidence or evidence not in nl_description:
                    evidence_validation["valid"] = False
                    evidence_validation["errors"].append(
                        f"Entity evidence '{evidence}' must be a substring of input"
                    )
    validations.append(evidence_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.2")
    return all_valid


async def test_step_1_2_no_explicit_entities():
    """Test Step 1.2 with no explicitly mentioned entities."""
    TestResultDisplay.print_test_case(2, "No explicit entities")
    
    # Input - Implied entities from IoT scenario
    nl_description = "Create an IoT telemetry dataset for industrial sensors deployed across multiple plants. Sensor readings should capture temperature, vibration, and current measurements with anomaly detection capabilities for cascading failure incidents."
    input_data = {"nl_description": nl_description}
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = EntityMentionOutput(
        has_explicit_entities=False,
        mentioned_entities=[],
        reasoning="Entities like Sensor, Plant, SensorReading are implied but not explicitly named in the description"
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.2"):
            result = await step_1_2_entity_mention_detection(nl_description)
    
    TestResultDisplay.print_output_summary(result)
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_entities", "mentioned_entities", "reasoning"]
    )
    validations.append(struct_validation)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    # Validate no entities logic
    no_entities_validation = {"valid": True, "errors": []}
    if not result_dict.get("has_explicit_entities"):
        mentioned_entities = result_dict.get("mentioned_entities", [])
        if mentioned_entities and len(mentioned_entities) > 0:
            no_entities_validation["valid"] = False
            no_entities_validation["errors"].append(
                "When has_explicit_entities is False, mentioned_entities should be empty"
            )
    validations.append(no_entities_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.2")
    return all_valid


async def main():
    """Run all tests for Step 1.2."""
    results = []
    results.append(await test_step_1_2_explicit_entities())
    results.append(await test_step_1_2_no_explicit_entities())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.2 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.2 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
