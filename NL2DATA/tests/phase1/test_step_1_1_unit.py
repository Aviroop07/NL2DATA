"""Unit tests for Step 1.1: Domain Detection & Inference.

These tests provide proper input structure, mock LLM responses,
perform deterministic validations, and display outputs.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase1.step_1_1_domain_detection import (
    step_1_1_domain_detection,
    DomainDetectionAndInferenceOutput,
    DomainAlternative
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_1_1_explicit_domain():
    """Test Step 1.1 with explicit domain in input."""
    TestResultDisplay.print_test_header("Domain Detection & Inference", "1.1")
    TestResultDisplay.print_test_case(1, "Explicit domain detection")
    
    # Input - Complex financial transactions dataset
    nl_description = "Generate a financial transactions dataset with a large transaction fact table (at least 50 million rows) and dimension tables for customers, merchants, cards, and geography. Legitimate transactions should form the majority, with card-level spending showing strong weekly seasonality and pay-day spikes around the 1st and 15th of each month. Inject multiple fraud patterns: low-value test transactions followed by one or more high-value purchases; coordinated fraud rings where many cards transact with the same merchant in a short time window; and location anomalies where the same card has transactions in geographically distant locations within an impossible travel time."
    input_data = {"nl_description": nl_description}
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = DomainDetectionAndInferenceOutput(
        has_explicit_domain=True,
        domain="financial",
        explicit_domain="financial",
        explicit_evidence="financial transactions dataset",
        confidence=1.0,
        inference_evidence=[],
        alternatives=[],
        reasoning="The domain 'financial' is explicitly mentioned in the description"
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.1"):
            result = await step_1_1_domain_detection(nl_description)
    
    TestResultDisplay.print_output_summary(result)
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_domain", "domain", "confidence", "reasoning"]
    )
    validations.append(struct_validation)
    
    # Validate types
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "has_explicit_domain": bool,
            "domain": str,
            "confidence": float,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    # Validate value ranges
    range_validation = ValidationHelper.validate_value_ranges(
        result,
        {"confidence": (0.0, 1.0)}
    )
    validations.append(range_validation)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    # Validate explicit domain logic
    explicit_validation = {"valid": True, "errors": []}
    if result_dict.get("has_explicit_domain"):
        if result_dict.get("domain") != result_dict.get("explicit_domain"):
            explicit_validation["valid"] = False
            explicit_validation["errors"].append(
                "When has_explicit_domain is True, domain must equal explicit_domain"
            )
        if result_dict.get("confidence") != 1.0:
            explicit_validation["valid"] = False
            explicit_validation["errors"].append(
                "When has_explicit_domain is True, confidence must be 1.0"
            )
        if result_dict.get("explicit_evidence") not in nl_description:
            explicit_validation["valid"] = False
            explicit_validation["errors"].append(
                "explicit_evidence must be a substring of the input"
            )
    validations.append(explicit_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.1")
    return all_valid


async def test_step_1_1_inferred_domain():
    """Test Step 1.1 with inferred domain."""
    TestResultDisplay.print_test_case(2, "Domain inference from context")
    
    # Input - IoT telemetry dataset (inferred domain)
    nl_description = "Create an IoT telemetry dataset for 10,000 industrial sensors deployed across 100 plants. There should be one high-frequency fact table called sensor_reading with at least 200 million rows over a 30-day period, plus dimension tables for sensors, plants, and sensor types. Sensor readings (temperature, vibration, current) should mostly remain within normal operating bands that differ by sensor type, with rare anomalies (0.1–0.5% of readings) modeled as spikes, drifts, or sudden step changes."
    input_data = {"nl_description": nl_description}
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM response
    mock_response = DomainDetectionAndInferenceOutput(
        has_explicit_domain=False,
        domain="iot",
        explicit_domain="",
        explicit_evidence="",
        confidence=0.92,
        inference_evidence=["IoT telemetry", "industrial sensors", "sensor_reading", "sensor types"],
        alternatives=[
            DomainAlternative(
                domain="manufacturing",
                confidence=0.75,
                evidence=["industrial sensors", "plants"]
            )
        ],
        reasoning="Inferred IoT domain from sensor telemetry and industrial monitoring context"
    )
    
    # Run test with mock
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("1.1"):
            result = await step_1_1_domain_detection(nl_description)
    
    TestResultDisplay.print_output_summary(result)
    
    # Deterministic validations
    validations = []
    
    # Validate structure
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["has_explicit_domain", "domain", "confidence", "reasoning"]
    )
    validations.append(struct_validation)
    
    # Convert to dict for validation
    result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
    
    # Validate inference logic
    inference_validation = {"valid": True, "errors": []}
    if not result_dict.get("has_explicit_domain"):
        if result_dict.get("confidence") >= 1.0:
            inference_validation["valid"] = False
            inference_validation["errors"].append(
                "When has_explicit_domain is False, confidence should be < 1.0"
            )
        if result_dict.get("explicit_domain") or result_dict.get("explicit_evidence"):
            inference_validation["valid"] = False
            inference_validation["errors"].append(
                "When has_explicit_domain is False, explicit_domain and explicit_evidence should be empty"
            )
        inference_evidence = result_dict.get("inference_evidence", [])
        # Note: inference_evidence can be empty if domain is inferred but no specific evidence phrases are extracted
        if isinstance(inference_evidence, list):
            # Validate evidence phrases are substrings if provided
            for evidence in inference_evidence:
                if evidence and evidence not in nl_description:
                    inference_validation["valid"] = False
                    inference_validation["errors"].append(
                        f"Evidence phrase '{evidence}' is not a substring of input"
                    )
    validations.append(inference_validation)
    
    # Print validation results
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "1.1")
    return all_valid


async def main():
    """Run all tests for Step 1.1."""
    results = []
    results.append(await test_step_1_1_explicit_domain())
    results.append(await test_step_1_1_inferred_domain())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 1.1 unit tests passed!")
    else:
        print("[FAIL] Some Step 1.1 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
