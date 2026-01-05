"""Test helper utilities for unit testing step functions.

This module provides utilities for:
- Mocking LLM calls
- Creating test fixtures with proper input structures
- Performing deterministic validations
- Displaying test results
"""

import sys
from pathlib import Path
from typing import Dict, Any, Type, Optional, List
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import BaseModel

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


class LLMMockHelper:
    """Helper class for mocking LLM calls in unit tests."""
    
    @staticmethod
    def create_mock_llm_response(output_model: Type[BaseModel], response_data: Dict[str, Any]) -> BaseModel:
        """Create a mock Pydantic model instance from response data.
        
        Args:
            output_model: The Pydantic model class expected as output
            response_data: Dictionary with data to populate the model
            
        Returns:
            Instance of output_model with response_data
        """
        return output_model.model_validate(response_data)
    
    @staticmethod
    def patch_standardized_llm_call(mock_response: BaseModel):
        """Create a patch context for standardized_llm_call that returns mock_response.
        
        Args:
            mock_response: The Pydantic model instance to return
            
        Returns:
            Context manager for patching
        """
        async def mock_llm_call(*args, **kwargs):
            return mock_response
        
        return patch('NL2DATA.utils.llm.standardized_llm_call', new=AsyncMock(side_effect=mock_llm_call))
    
    @staticmethod
    def patch_model_router(step_id: str):
        """Create a patch context for get_model_for_step.
        
        Args:
            step_id: The step ID (e.g., "1.1", "2.2")
            
        Returns:
            Context manager for patching
        """
        mock_model = MagicMock()
        return patch(f'NL2DATA.phases.phase{step_id.split(".")[0]}.model_router.get_model_for_step', return_value=mock_model)


class ValidationHelper:
    """Helper class for performing deterministic validations on step outputs."""
    
    @staticmethod
    def _to_dict(output):
        """Convert output to dict if it's a Pydantic model, otherwise return as-is."""
        if hasattr(output, 'model_dump'):
            return output.model_dump()
        return output
    
    @staticmethod
    def validate_output_structure(output, required_fields: List[str]) -> Dict[str, Any]:
        """Validate that output contains all required fields.
        
        Args:
            output: The output dictionary or Pydantic model to validate
            required_fields: List of required field names
            
        Returns:
            Dictionary with validation results:
                - valid: bool
                - missing_fields: List[str]
                - errors: List[str]
        """
        output_dict = ValidationHelper._to_dict(output)
        result = {
            "valid": True,
            "missing_fields": [],
            "errors": []
        }
        
        for field in required_fields:
            if field not in output_dict:
                result["valid"] = False
                result["missing_fields"].append(field)
                result["errors"].append(f"Missing required field: {field}")
        
        return result
    
    @staticmethod
    def validate_non_empty_lists(output, list_fields: List[str]) -> Dict[str, Any]:
        """Validate that list fields are non-empty.
        
        Args:
            output: The output dictionary or Pydantic model to validate
            list_fields: List of field names that should be non-empty lists
            
        Returns:
            Dictionary with validation results
        """
        output_dict = ValidationHelper._to_dict(output)
        result = {
            "valid": True,
            "errors": []
        }
        
        for field in list_fields:
            if field in output_dict:
                value = output_dict[field]
                if not isinstance(value, list):
                    result["valid"] = False
                    result["errors"].append(f"Field '{field}' should be a list, got {type(value).__name__}")
                elif len(value) == 0:
                    result["valid"] = False
                    result["errors"].append(f"Field '{field}' should be a non-empty list")
        
        return result
    
    @staticmethod
    def validate_types(output, type_specs: Dict[str, Type]) -> Dict[str, Any]:
        """Validate that fields have correct types.
        
        Args:
            output: The output dictionary or Pydantic model to validate
            type_specs: Dictionary mapping field names to expected types
            
        Returns:
            Dictionary with validation results
        """
        output_dict = ValidationHelper._to_dict(output)
        result = {
            "valid": True,
            "errors": []
        }
        
        for field, expected_type in type_specs.items():
            if field in output_dict:
                value = output_dict[field]
                if not isinstance(value, expected_type):
                    result["valid"] = False
                    result["errors"].append(
                        f"Field '{field}' should be {expected_type.__name__}, got {type(value).__name__}"
                    )
        
        return result
    
    @staticmethod
    def validate_value_ranges(output, range_specs: Dict[str, tuple]) -> Dict[str, Any]:
        """Validate that numeric fields are within specified ranges.
        
        Args:
            output: The output dictionary or Pydantic model to validate
            range_specs: Dictionary mapping field names to (min, max) tuples
            
        Returns:
            Dictionary with validation results
        """
        output_dict = ValidationHelper._to_dict(output)
        result = {
            "valid": True,
            "errors": []
        }
        
        for field, (min_val, max_val) in range_specs.items():
            if field in output_dict:
                value = output_dict[field]
                if isinstance(value, (int, float)):
                    if value < min_val or value > max_val:
                        result["valid"] = False
                        result["errors"].append(
                            f"Field '{field}' value {value} is outside range [{min_val}, {max_val}]"
                        )
        
        return result


class TestResultDisplay:
    """Helper class for displaying test results in a formatted way."""
    
    @staticmethod
    def print_test_header(step_name: str, step_number: str):
        """Print a formatted test header.
        
        Args:
            step_name: Name of the step being tested
            step_number: Step number (e.g., "1.1", "2.2")
        """
        print("=" * 80)
        print(f"Testing Step {step_number}: {step_name}")
        print("=" * 80)
    
    @staticmethod
    def print_test_case(test_case_num: int, description: str = ""):
        """Print a test case header.
        
        Args:
            test_case_num: Test case number
            description: Optional description of the test case
        """
        print(f"\nTest Case {test_case_num}:")
        if description:
            print(f"  Description: {description}")
        print("-" * 80)
    
    @staticmethod
    def print_input_summary(input_data: Dict[str, Any], max_depth: int = 2):
        """Print a summary of input data.
        
        Args:
            input_data: Input dictionary
            max_depth: Maximum depth to print nested structures
        """
        print("Input:")
        TestResultDisplay._print_dict(input_data, indent=2, max_depth=max_depth)
    
    @staticmethod
    def print_output_summary(output, max_depth: int = 2):
        """Print a summary of output data.
        
        Args:
            output: Output dictionary or Pydantic model
            max_depth: Maximum depth to print nested structures
        """
        print("\nOutput:")
        # Convert Pydantic model to dict if needed
        if hasattr(output, 'model_dump'):
            output = output.model_dump()
        TestResultDisplay._print_dict(output, indent=2, max_depth=max_depth)
    
    @staticmethod
    def print_validation_results(validation_results: Dict[str, Any]):
        """Print validation results.
        
        Args:
            validation_results: Dictionary with validation results
        """
        if validation_results.get("valid", False):
            print("\n[PASS] All validations passed")
        else:
            print("\n[FAIL] Validation errors:")
            for error in validation_results.get("errors", []):
                print(f"  - {error}")
            for field in validation_results.get("missing_fields", []):
                print(f"  - Missing field: {field}")
    
    @staticmethod
    def print_test_summary(passed: bool, step_number: str):
        """Print test summary.
        
        Args:
            passed: Whether all tests passed
            step_number: Step number
        """
        print("\n" + "=" * 80)
        if passed:
            print(f"[PASS] All tests for Step {step_number} passed!")
        else:
            print(f"[FAIL] Some tests for Step {step_number} failed")
        print("=" * 80)
    
    @staticmethod
    def _print_dict(data: Dict[str, Any], indent: int = 0, max_depth: int = 2, current_depth: int = 0):
        """Recursively print dictionary with indentation.
        
        Args:
            data: Dictionary to print
            indent: Current indentation level
            max_depth: Maximum depth to print
            current_depth: Current depth in recursion
        """
        if current_depth >= max_depth:
            print(" " * indent + "...")
            return
        
        for key, value in data.items():
            if isinstance(value, dict):
                print(" " * indent + f"{key}:")
                TestResultDisplay._print_dict(value, indent + 2, max_depth, current_depth + 1)
            elif isinstance(value, list):
                print(" " * indent + f"{key}: [list with {len(value)} items]")
                if len(value) > 0 and current_depth < max_depth - 1:
                    if isinstance(value[0], dict):
                        print(" " * (indent + 2) + "First item:")
                        TestResultDisplay._print_dict(value[0], indent + 4, max_depth, current_depth + 1)
                    else:
                        print(" " * (indent + 2) + f"  {value[0]}")
            else:
                str_value = str(value)
                if len(str_value) > 100:
                    str_value = str_value[:100] + "..."
                print(" " * indent + f"{key}: {str_value}")
